/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.core.persistence.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.BaseUnits;
import io.micrometer.core.instrument.binder.cache.CaffeineStatsCounter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.BehaviorChangeConfiguration;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.dao.entity.ChangeTrackingResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An in-memory entity cache. Entities are held in a single cache keyed by entity id, bounded by a
 * weight budget ({@code ENTITY_CACHE_WEIGHER_TARGET}) and expiring after 1h of no access.
 *
 * <p>Lookups by name go through a secondary index mapping name keys to entity ids; every read
 * through this index is validated against the primary id cache.
 */
public class InMemoryEntityCache implements EntityCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryEntityCache.class);
  public static final int MAX_CACHE_REFRESH_ATTEMPTS = 100;

  // These meters are registered per cache instance and hence carry the per-instance tags, for
  // example the realm. Their names must stay specific to the entity cache: registering a name that
  // another cache already registered with a different set of tag keys does not fail loudly, the
  // Prometheus registry simply omits the conflicting series from the scrape.
  public static final String METER_CACHE_CAPACITY = "polaris.entity-cache.capacity";
  public static final String METER_CACHE_WEIGHT = "polaris.entity-cache.weight-reported";
  public static final String METER_CACHE_ENTRIES = "polaris.entity-cache.entries";
  public static final String METER_CACHE_GETS_BY_NAME = "polaris.entity-cache.gets.by-name";
  public static final String METER_CACHE_GETS_BY_ID = "polaris.entity-cache.gets.by-id";

  public static final String CACHE_NAME = "polaris-entities";

  private final PolarisDiagnostics diagnostics;
  private final PolarisMetaStoreManager polarisMetaStoreManager;
  private final Cache<Long, ResolvedPolarisEntity> byId;
  private final Cache<EntityCacheByNameKey, Long> nameIndex;
  private final Optional<MeterRegistry> meterRegistry;
  private final List<Meter> meters = new ArrayList<>();
  private final Optional<Counter> byNameHits;
  private final Optional<Counter> byNameMisses;

  // Micrometer holds a gauge's target weakly, so the target must be something this cache keeps
  // alive itself. Caffeine's Policy.Eviction is not that: it is reachable only through internals
  // Caffeine happens to memoize. Using this field as the target also keeps 'this' out of the gauge,
  // so a cache dropped without close() can still be collected.
  private final LongSupplier weightSupplier;

  /**
   * Constructor for an uninstrumented cache. Cache can be private or shared
   *
   * @param polarisMetaStoreManager the meta store manager implementation
   */
  public InMemoryEntityCache(
      @NonNull PolarisDiagnostics diagnostics,
      @NonNull RealmConfig realmConfig,
      @NonNull PolarisMetaStoreManager polarisMetaStoreManager) {
    this(diagnostics, realmConfig, polarisMetaStoreManager, Optional.empty(), Tags.empty());
  }

  /**
   * Constructor. Cache can be private or shared. Meters are only registered if a {@link
   * MeterRegistry} is present.
   *
   * @param polarisMetaStoreManager the meta store manager implementation
   * @param meterRegistry the registry to report cache metrics to, if any
   * @param extraTags tags identifying this particular cache instance, for example the realm. Only
   *     applied to the meters that are specific to the entity cache, see {@link
   *     #registerMeters(MeterRegistry, Iterable, long)}
   */
  public InMemoryEntityCache(
      @NonNull PolarisDiagnostics diagnostics,
      @NonNull RealmConfig realmConfig,
      @NonNull PolarisMetaStoreManager polarisMetaStoreManager,
      Optional<MeterRegistry> meterRegistry,
      Iterable<Tag> extraTags) {
    this(
        diagnostics,
        realmConfig,
        polarisMetaStoreManager,
        meterRegistry,
        extraTags,
        ForkJoinPool.commonPool());
  }

  /**
   * Constructor taking the executor that Caffeine runs its maintenance on. Caffeine's bookkeeping -
   * draining the read and write buffers, applying evictions, updating {@code
   * Policy.Eviction#weightedSize()} - happens asynchronously on this executor, so a caller that
   * needs the cache's reported state to be up to date as soon as a write returns can pass {@link
   * Runnable#run} to make that bookkeeping synchronous. Production code uses the default {@link
   * ForkJoinPool#commonPool()}.
   */
  @VisibleForTesting
  InMemoryEntityCache(
      @NonNull PolarisDiagnostics diagnostics,
      @NonNull RealmConfig realmConfig,
      @NonNull PolarisMetaStoreManager polarisMetaStoreManager,
      Optional<MeterRegistry> meterRegistry,
      Iterable<Tag> extraTags,
      @NonNull Executor maintenanceExecutor) {
    this.diagnostics = diagnostics;
    this.meterRegistry = meterRegistry;

    long weigherTarget = realmConfig.getConfig(FeatureConfiguration.ENTITY_CACHE_WEIGHER_TARGET);

    // The name index only holds ids. Its entries are normally removed together with the byId
    // entries they point to, but an entry can be left dangling when the byId value was garbage
    // collected (soft values), so the index expires on the same schedule as byId. A dangling entry
    // can only cause a cache miss. The index itself must never use soft values.
    this.nameIndex =
        Caffeine.newBuilder()
            .expireAfterAccess(1, TimeUnit.HOURS)
            .executor(maintenanceExecutor)
            .build();

    // When an entry is evicted from byId, drop its name index entry. Explicit removals and
    // replacements are handled by the code performing them. An eviction listener runs atomically
    // with the eviction for that id, so it cannot race with cacheNewEntry re-inserting the same id.
    RemovalListener<Long, ResolvedPolarisEntity> evictionListener =
        (key, value, cause) -> {
          if (value != null) {
            // Only remove the entry if it maps to the same id
            this.nameIndex.asMap().remove(new EntityCacheByNameKey(value.getEntity()), key);
          }
        };

    Caffeine<Long, ResolvedPolarisEntity> byIdBuilder =
        Caffeine.newBuilder()
            .maximumWeight(weigherTarget)
            .weigher(EntityWeigher.INSTANCE)
            .expireAfterAccess(1, TimeUnit.HOURS) // Expire entries after 1 hour of no access
            .executor(maintenanceExecutor)
            .evictionListener(evictionListener);

    if (realmConfig.getConfig(BehaviorChangeConfiguration.ENTITY_CACHE_SOFT_VALUES)) {
      byIdBuilder.softValues();
    }

    // Caffeine's own statistics are reported without the extra tags: 'cache.gets' and friends are
    // shared with the other Caffeine caches of this service, which report them tagged with 'cache'
    // only, and a registry requires the same tag keys for a given meter name. Those meters are
    // therefore aggregated over all realms, and the hit ratio of a single realm is recovered from
    // the by-id counters registered alongside them.
    Optional<StatsCounter> statsCounter = meterRegistry.map(reg -> statsCounter(reg, extraTags));
    statsCounter.ifPresent(counter -> byIdBuilder.recordStats(() -> counter));

    // use a Caffeine cache to purge entries when those have not been used for a long time.
    this.byId = byIdBuilder.build();

    var eviction = byId.policy().eviction().orElseThrow();
    this.weightSupplier = () -> eviction.weightedSize().orElse(0L);

    this.byNameHits = byNameLookupCounter(extraTags, "hit");
    this.byNameMisses = byNameLookupCounter(extraTags, "miss");
    meterRegistry.ifPresent(reg -> registerMeters(reg, extraTags, weigherTarget));

    // remember the meta store manager
    this.polarisMetaStoreManager = polarisMetaStoreManager;
  }

  /**
   * Registers the meters that Caffeine's own statistics do not cover: the configured capacity, the
   * currently reported weight and the number of entries in either index. These meter names are
   * specific to the entity cache, hence they can carry the per-instance {@code extraTags}.
   */
  private void registerMeters(MeterRegistry reg, Iterable<Tag> extraTags, long weigherTarget) {
    meters.add(
        Gauge.builder(METER_CACHE_CAPACITY, "", x -> weigherTarget)
            .description("Total capacity of the entity cache, in approximate bytes.")
            .tag("cache", CACHE_NAME)
            .tags(extraTags)
            .baseUnit(BaseUnits.BYTES)
            .register(reg));
    meters.add(
        Gauge.builder(METER_CACHE_WEIGHT, weightSupplier, LongSupplier::getAsLong)
            .description("Current reported weight of the entity cache, in approximate bytes.")
            .tag("cache", CACHE_NAME)
            .tags(extraTags)
            .baseUnit(BaseUnits.BYTES)
            .register(reg));
    meters.add(
        Gauge.builder(METER_CACHE_ENTRIES, byId, Cache::estimatedSize)
            .description("Number of entries in the entity cache.")
            .tag("cache", CACHE_NAME)
            .tag("index", "by-id")
            .tags(extraTags)
            .register(reg));
    meters.add(
        Gauge.builder(METER_CACHE_ENTRIES, nameIndex, Cache::estimatedSize)
            .description("Number of entries in the entity cache.")
            .tag("cache", CACHE_NAME)
            .tag("index", "by-name")
            .tags(extraTags)
            .register(reg));
  }

  /**
   * Counts lookups by name, which resolve through the {@code nameIndex} and are not covered by the
   * {@link CaffeineStatsCounter} of the {@code byId} cache. Empty if there is no registry.
   */
  private Optional<Counter> byNameLookupCounter(Iterable<Tag> extraTags, String result) {
    return meterRegistry.map(
        reg ->
            lookupCounter(
                reg,
                METER_CACHE_GETS_BY_NAME,
                "Entity cache lookups by name. Caffeine's own 'cache.gets' only covers lookups by"
                    + " id.",
                extraTags,
                result));
  }

  /**
   * Reports Caffeine's statistics for the {@code byId} index and additionally counts its lookups
   * under an entity cache specific name. Caffeine's own 'cache.gets' cannot carry the per-instance
   * tags, so it alone does not tell the hit ratio of a single realm apart from the others'.
   */
  private StatsCounter statsCounter(MeterRegistry reg, Iterable<Tag> extraTags) {
    String description =
        "Entity cache lookups by id. Unlike Caffeine's own 'cache.gets', which is shared with the"
            + " other caches of this service, this counter is reported per cache instance.";
    return new TaggedStatsCounter(
        new CaffeineStatsCounter(reg, CACHE_NAME),
        lookupCounter(reg, METER_CACHE_GETS_BY_ID, description, extraTags, "hit"),
        lookupCounter(reg, METER_CACHE_GETS_BY_ID, description, extraTags, "miss"));
  }

  /** Registers a lookup counter of this cache instance, so that {@link #close()} removes it. */
  private Counter lookupCounter(
      MeterRegistry reg, String name, String description, Iterable<Tag> extraTags, String result) {
    Counter counter =
        Counter.builder(name)
            .description(description)
            .tag("cache", CACHE_NAME)
            .tag("result", result)
            .tags(extraTags)
            .register(reg);
    meters.add(counter);
    return counter;
  }

  /**
   * Records everything Caffeine reports through {@code delegate} and, in addition, the hits and
   * misses on the per-instance counters. Only the lookup counts are duplicated: loads and evictions
   * stay with the delegate.
   */
  private record TaggedStatsCounter(CaffeineStatsCounter delegate, Counter hits, Counter misses)
      implements StatsCounter {
    @Override
    public void recordHits(int count) {
      delegate.recordHits(count);
      hits.increment(count);
    }

    @Override
    public void recordMisses(int count) {
      delegate.recordMisses(count);
      misses.increment(count);
    }

    @Override
    public void recordLoadSuccess(long loadTime) {
      delegate.recordLoadSuccess(loadTime);
    }

    @Override
    public void recordLoadFailure(long loadTime) {
      delegate.recordLoadFailure(loadTime);
    }

    @Override
    public void recordEviction(int weight, RemovalCause cause) {
      delegate.recordEviction(weight, cause);
    }

    @Override
    public CacheStats snapshot() {
      return delegate.snapshot();
    }
  }

  /**
   * Removes the meters registered for this cache instance. The Caffeine statistics are left in
   * place: they are registered without the per-instance tags and are hence shared with the caches
   * of other realms.
   */
  @Override
  public void close() {
    meterRegistry.ifPresent(reg -> meters.forEach(reg::remove));
  }

  /**
   * Remove the specified cache entry from the cache
   *
   * @param cacheEntry cache entry to remove
   */
  @Override
  public void removeCacheEntry(@NonNull ResolvedPolarisEntity cacheEntry) {
    long id = cacheEntry.getEntity().getId();

    // Only drop the entry, and its name index entry, if this exact entry is the one cached. Both
    // removals happen atomically for a given id so they cannot race with cacheNewEntry.
    this.byId
        .asMap()
        .computeIfPresent(
            id,
            (key, oldCacheEntry) -> {
              if (oldCacheEntry != cacheEntry) {
                return oldCacheEntry;
              }
              this.nameIndex.asMap().remove(new EntityCacheByNameKey(cacheEntry.getEntity()), id);
              return null;
            });
  }

  /**
   * Cache new entry
   *
   * @param cacheEntry new cache entry
   */
  private void cacheNewEntry(@NonNull ResolvedPolarisEntity cacheEntry) {
    long id = cacheEntry.getEntity().getId();

    // The version check and the name index update happen atomically for a given id
    this.byId
        .asMap()
        .compute(
            id,
            (key, oldCacheEntry) -> {
              ResolvedPolarisEntity winner =
                  oldCacheEntry == null || this.isNewer(cacheEntry, oldCacheEntry)
                      ? cacheEntry
                      : oldCacheEntry;
              EntityCacheByNameKey winnerKey = new EntityCacheByNameKey(winner.getEntity());

              // remove old name if it has changed
              if (oldCacheEntry != null) {
                EntityCacheByNameKey oldNameKey =
                    new EntityCacheByNameKey(oldCacheEntry.getEntity());
                if (!oldNameKey.equals(winnerKey)) {
                  this.nameIndex.asMap().remove(oldNameKey, id);
                }
              }

              if (winner.getEntity().isDropped()) {
                this.nameIndex.asMap().remove(winnerKey, id);
              } else {
                this.nameIndex.put(winnerKey, id);
              }
              return winner;
            });
  }

  /**
   * Determine if the newer value is really newer
   *
   * @param newValue new cache entry
   * @param oldValue old cache entry
   * @return true if the newer cache entry
   */
  private boolean isNewer(ResolvedPolarisEntity newValue, ResolvedPolarisEntity oldValue) {
    return (newValue.getEntity().getEntityVersion() > oldValue.getEntity().getEntityVersion()
        || newValue.getEntity().getGrantRecordsVersion()
            > oldValue.getEntity().getGrantRecordsVersion());
  }

  /**
   * Get a cache entity entry given the id of the entity.
   *
   * <p>Records cache metrics; use {@code byId.asMap().get(entityId)} directly to elide metric
   * collection.
   *
   * @param entityId entity id
   * @return the cache entry or null if not found
   */
  public @Nullable ResolvedPolarisEntity getEntityById(long entityId) {
    return byId.getIfPresent(entityId);
  }

  /**
   * Get a cache entity entry given the name key of the entity.
   *
   * <p>Records the lookup on the by-name counters only: resolving the id found in the name index is
   * part of this lookup, not a lookup by id, so it does not record Caffeine's statistics.
   *
   * @param entityNameKey entity name key
   * @return the cache entry or null if not found
   */
  public @Nullable ResolvedPolarisEntity getEntityByName(
      @NonNull EntityCacheByNameKey entityNameKey) {
    Long id = nameIndex.getIfPresent(entityNameKey);
    if (id == null) {
      byNameMisses.ifPresent(Counter::increment);
      return null;
    }
    // asMap().get() still counts as an access for expiry, but does not record Caffeine's stats
    ResolvedPolarisEntity entry = byId.asMap().get(id);
    // The index may be stale, e.g. the byId value was garbage collected. Drop the stale mapping so
    // repeated misses do not keep it alive, but only if it still points to the same id.
    if (entry == null || !new EntityCacheByNameKey(entry.getEntity()).equals(entityNameKey)) {
      nameIndex.asMap().remove(entityNameKey, id);
      byNameMisses.ifPresent(Counter::increment);
      return null;
    }
    byNameHits.ifPresent(Counter::increment);
    return entry;
  }

  /**
   * Refresh the cache if needs be with a version of the entity/grant records matching the minimum
   * specified version.
   *
   * @param callContext the Polaris call context
   * @param entityToValidate copy of the entity held by the caller to validate
   * @param entityMinVersion minimum expected version. Should be reloaded if found in a cache with a
   *     version less than this one
   * @param entityGrantRecordsMinVersion minimum grant records version which is expected, grants
   *     records should be reloaded if needed
   * @return the cache entry for the entity or null if the specified entity does not exist
   */
  @Override
  public @Nullable ResolvedPolarisEntity getAndRefreshIfNeeded(
      @NonNull PolarisCallContext callContext,
      @NonNull PolarisBaseEntity entityToValidate,
      int entityMinVersion,
      int entityGrantRecordsMinVersion) {
    long entityCatalogId = entityToValidate.getCatalogId();
    long entityId = entityToValidate.getId();
    PolarisEntityType entityType = entityToValidate.getType();

    // first lookup the cache to find the existing cache entry
    ResolvedPolarisEntity existingCacheEntry = this.getEntityById(entityId);

    // the new one to be returned
    final ResolvedPolarisEntity newCacheEntry;

    // see if we need to load or refresh that entity
    if (existingCacheEntry == null
        || existingCacheEntry.getEntity().getEntityVersion() < entityMinVersion
        || existingCacheEntry.getEntity().getGrantRecordsVersion() < entityGrantRecordsMinVersion) {

      // the refreshed entity
      final ResolvedEntityResult refreshedCacheEntry;

      // was not found in the cache?
      final PolarisBaseEntity entity;
      final List<PolarisGrantRecord> grantRecords;
      final int grantRecordsVersion;
      if (existingCacheEntry == null) {
        // try to load it
        refreshedCacheEntry =
            this.polarisMetaStoreManager.loadResolvedEntityById(
                callContext, entityCatalogId, entityId, entityType);
        if (refreshedCacheEntry.isSuccess()) {
          entity = refreshedCacheEntry.getEntity();
          grantRecords = refreshedCacheEntry.getEntityGrantRecords();
          grantRecordsVersion = refreshedCacheEntry.getGrantRecordsVersion();
        } else {
          return null;
        }
      } else {
        // refresh it
        refreshedCacheEntry =
            this.polarisMetaStoreManager.refreshResolvedEntity(
                callContext,
                existingCacheEntry.getEntity().getEntityVersion(),
                existingCacheEntry.getEntity().getGrantRecordsVersion(),
                entityType,
                entityCatalogId,
                entityId);
        if (refreshedCacheEntry.isSuccess()) {
          entity =
              (refreshedCacheEntry.getEntity() != null)
                  ? refreshedCacheEntry.getEntity()
                  : existingCacheEntry.getEntity();
          if (refreshedCacheEntry.getEntityGrantRecords() != null) {
            grantRecords = refreshedCacheEntry.getEntityGrantRecords();
            grantRecordsVersion = refreshedCacheEntry.getGrantRecordsVersion();
          } else {
            grantRecords = existingCacheEntry.getAllGrantRecords();
            grantRecordsVersion = existingCacheEntry.getEntity().getGrantRecordsVersion();
          }
        } else {
          // entity has been purged, remove it
          this.removeCacheEntry(existingCacheEntry);
          return null;
        }
      }

      // assert that entity, grant records and version are all set
      diagnostics.checkNotNull(entity, "unexpected_null_entity");
      diagnostics.checkNotNull(grantRecords, "unexpected_null_grant_records");
      diagnostics.check(grantRecordsVersion > 0, "unexpected_null_grant_records_version");

      // create new cache entry
      newCacheEntry =
          new ResolvedPolarisEntity(diagnostics, entity, grantRecords, grantRecordsVersion);

      // insert cache entry
      this.cacheNewEntry(newCacheEntry);
    } else {
      // found it in the cache and it is up-to-date, simply return it
      newCacheEntry = existingCacheEntry;
    }

    return newCacheEntry;
  }

  /**
   * Get the specified entity by name and load it if it is not found.
   *
   * @param callContext the Polaris call context
   * @param entityCatalogId id of the catalog where this entity resides or NULL_ID if top-level
   * @param entityId id of the entity to lookup
   * @return null if the entity does not exist or was dropped. Else return the entry for that
   *     entity, either as found in the cache or loaded from the backend
   */
  @Override
  public @Nullable EntityCacheLookupResult getOrLoadEntityById(
      @NonNull PolarisCallContext callContext,
      long entityCatalogId,
      long entityId,
      PolarisEntityType entityType) {

    // if it exists, we are set
    ResolvedPolarisEntity entry = this.getEntityById(entityId);
    final boolean cacheHit;

    // we need to load it if it does not exist
    if (entry == null) {
      // this is a miss
      cacheHit = false;

      // load it
      ResolvedEntityResult result =
          polarisMetaStoreManager.loadResolvedEntityById(
              callContext, entityCatalogId, entityId, entityType);

      // not found, exit
      if (!result.isSuccess()) {
        return null;
      }

      // if found, setup entry
      diagnostics.checkNotNull(result.getEntity(), "entity_should_loaded");
      diagnostics.checkNotNull(
          result.getEntityGrantRecords(), "entity_grant_records_should_loaded");
      entry =
          new ResolvedPolarisEntity(
              diagnostics,
              result.getEntity(),
              result.getEntityGrantRecords(),
              result.getGrantRecordsVersion());

      // the above loading could take a long time so check again if the entry exists and only
      this.cacheNewEntry(entry);
    } else {
      cacheHit = true;
    }

    // return what we found
    return new EntityCacheLookupResult(entry, cacheHit);
  }

  /**
   * Get the specified entity by name and load it if it is not found.
   *
   * @param callContext the Polaris call context
   * @param entityNameKey name of the entity to load
   * @return null if the entity does not exist or was dropped. Else return the entry for that
   *     entity, either as found in the cache or loaded from the backend
   */
  @Override
  public @Nullable EntityCacheLookupResult getOrLoadEntityByName(
      @NonNull PolarisCallContext callContext, @NonNull EntityCacheByNameKey entityNameKey) {

    // if it exists, we are set
    ResolvedPolarisEntity entry = this.getEntityByName(entityNameKey);
    final boolean cacheHit;

    // we need to load it if it does not exist
    if (entry == null) {
      // this is a miss
      cacheHit = false;

      // load it
      ResolvedEntityResult result =
          polarisMetaStoreManager.loadResolvedEntityByName(
              callContext,
              entityNameKey.getCatalogId(),
              entityNameKey.getParentId(),
              entityNameKey.getType(),
              entityNameKey.getName());

      // not found, exit
      if (!result.isSuccess()) {
        return null;
      }

      // validate return
      diagnostics.checkNotNull(result.getEntity(), "entity_should_loaded");
      diagnostics.checkNotNull(
          result.getEntityGrantRecords(), "entity_grant_records_should_loaded");

      // if found, setup entry
      entry =
          new ResolvedPolarisEntity(
              diagnostics,
              result.getEntity(),
              result.getEntityGrantRecords(),
              result.getGrantRecordsVersion());

      // the above loading could take a long time so check again if the entry exists and only
      this.cacheNewEntry(entry);
    } else {
      cacheHit = true;
    }

    // return what we found
    return new EntityCacheLookupResult(entry, cacheHit);
  }

  @Override
  public List<EntityCacheLookupResult> getOrLoadResolvedEntities(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityType entityType,
      @NonNull List<PolarisEntityId> entityIds) {
    // use a map to collect cached entries to avoid concurrency problems in case a second thread is
    // trying to populate
    // the cache from a different snapshot
    Map<PolarisEntityId, ResolvedPolarisEntity> resolvedEntities = new HashMap<>();
    boolean stateResolved = false;
    for (int i = 0; i < MAX_CACHE_REFRESH_ATTEMPTS; i++) {
      Function<List<PolarisEntityId>, ResolvedEntitiesResult> loaderFunc =
          idsToLoad -> polarisMetaStoreManager.loadResolvedEntities(callCtx, entityType, idsToLoad);
      if (isCacheStateValid(callCtx, resolvedEntities, entityIds, loaderFunc)) {
        stateResolved = true;
        break;
      }
    }
    if (!stateResolved) {
      LOGGER.warn(
          "Unable to resolve entities in cache after multiple attempts {} - resolved: {}",
          entityIds,
          resolvedEntities);
      diagnostics.fail("cannot_resolve_all_entities", "Unable to resolve entities in cache");
    }

    return entityIds.stream()
        .map(
            id -> {
              ResolvedPolarisEntity entity = resolvedEntities.get(id);
              return entity == null ? null : new EntityCacheLookupResult(entity, true);
            })
        .collect(Collectors.toList());
  }

  private boolean isCacheStateValid(
      @NonNull PolarisCallContext callCtx,
      @NonNull Map<PolarisEntityId, ResolvedPolarisEntity> resolvedEntities,
      @NonNull List<PolarisEntityId> entityIds,
      @NonNull Function<List<PolarisEntityId>, ResolvedEntitiesResult> loaderFunc) {
    ChangeTrackingResult changeTrackingResult =
        polarisMetaStoreManager.loadEntitiesChangeTracking(callCtx, entityIds);
    List<PolarisEntityId> idsToLoad = new ArrayList<>();
    if (changeTrackingResult.isSuccess()) {
      idsToLoad.addAll(validateCacheEntries(entityIds, resolvedEntities, changeTrackingResult));
    } else {
      idsToLoad.addAll(entityIds);
    }
    if (!idsToLoad.isEmpty()) {
      ResolvedEntitiesResult resolvedEntitiesResult = loaderFunc.apply(idsToLoad);
      if (resolvedEntitiesResult.isSuccess()) {
        LOGGER.debug("Resolved entities - validating cache");
        resolvedEntitiesResult.getResolvedEntities().stream()
            .filter(Objects::nonNull)
            .forEach(
                e -> {
                  this.cacheNewEntry(e);
                  resolvedEntities.put(
                      new PolarisEntityId(e.getEntity().getCatalogId(), e.getEntity().getId()), e);
                });
      }
    }

    // the loader function should always return a batch of results from the same "snapshot" of the
    // persistence, so
    // if the changeTracking call above failed, we should have loaded the entire batch in one shot.
    // There should be no
    // need to revalidate the entities.
    List<PolarisEntityId> idsToReload =
        changeTrackingResult.isSuccess()
            ? validateCacheEntries(entityIds, resolvedEntities, changeTrackingResult)
            : List.of();
    return idsToReload.isEmpty();
  }

  private List<PolarisEntityId> validateCacheEntries(
      List<PolarisEntityId> entityIds,
      Map<PolarisEntityId, ResolvedPolarisEntity> resolvedEntities,
      ChangeTrackingResult changeTrackingResult) {
    List<PolarisEntityId> idsToReload = new ArrayList<>();
    Iterator<PolarisEntityId> idIterator = entityIds.iterator();
    Iterator<PolarisChangeTrackingVersions> changeTrackingIterator =
        changeTrackingResult.getChangeTrackingVersions().iterator();
    while (idIterator.hasNext() && changeTrackingIterator.hasNext()) {
      PolarisEntityId entityId = idIterator.next();
      PolarisChangeTrackingVersions changeTrackingVersions = changeTrackingIterator.next();
      if (changeTrackingVersions == null) {
        // entity has been purged
        ResolvedPolarisEntity cachedEntity = getEntityById(entityId.id());
        if (cachedEntity != null || resolvedEntities.containsKey(entityId)) {
          LOGGER.debug("Entity {} has been purged, removing from cache", entityId);
          Optional.ofNullable(cachedEntity).ifPresent(this::removeCacheEntry);
          resolvedEntities.remove(entityId);
        }
        continue;
      }
      // compare versions using equals rather than less than so we can use the same function to
      // validate that the cache
      // entries are consistent with a single call to the change tracking table, rather than some
      // grants ahead and some
      // grants behind
      ResolvedPolarisEntity cachedEntity =
          resolvedEntities.computeIfAbsent(entityId, id -> this.getEntityById(id.id()));
      if (cachedEntity == null
          || cachedEntity.getEntity().getEntityVersion() != changeTrackingVersions.entityVersion()
          || cachedEntity.getEntity().getGrantRecordsVersion()
              != changeTrackingVersions.grantRecordsVersion()) {
        idsToReload.add(entityId);
      } else {
        resolvedEntities.put(entityId, cachedEntity);
      }
    }
    LOGGER.debug("Cache entries {} need to be reloaded", idsToReload);
    return idsToReload;
  }
}
