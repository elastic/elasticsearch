/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.common.cache.RemovalListener;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.MappingLookup;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * The indices request cache allows to cache a shard level request stage responses, helping with improving
 * similar requests that are potentially expensive (because of aggs for example). The cache is fully coherent
 * with the semantics of NRT (the index reader cache key is part of the cache key), and relies on size based
 * eviction to evict old reader associated cache entries as well as scheduler reaper to clean readers that
 * are no longer used or closed shards.
 * <p>
 * Currently, the cache is only enabled for count requests, and can only be opted in on an index
 * level setting that can be dynamically changed and defaults to false.
 * <p>
 * There are still several TODOs left in this class, some easily addressable, some more complex, but the support
 * is functional.
 */
public final class IndicesRequestCache implements RemovalListener<IndicesRequestCache.Key, BytesReference>, Closeable {

    /**
     * A setting to enable or disable request caching on an index level. Its dynamic by default
     * since we are checking on the cluster state IndexMetadata always.
     */
    public static final Setting<Boolean> INDEX_CACHE_REQUEST_ENABLED_SETTING = Setting.boolSetting(
        "index.requests.cache.enable",
        true,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<ByteSizeValue> INDICES_CACHE_QUERY_SIZE = Setting.memorySizeSetting(
        "indices.requests.cache.size",
        "1%",
        Property.NodeScope
    );
    public static final Setting<TimeValue> INDICES_CACHE_QUERY_EXPIRE = Setting.positiveTimeSetting(
        "indices.requests.cache.expire",
        new TimeValue(0),
        Property.NodeScope
    );

    private final ConcurrentMap<CleanupKey, Boolean> registeredClosedListeners = ConcurrentCollections.newConcurrentMap();
    private final Set<CleanupKey> keysToClean = ConcurrentCollections.newConcurrentSet();
    private final ByteSizeValue size;
    private final TimeValue expire;
    private final Cache<Key, BytesReference> cache;

    IndicesRequestCache(Settings settings) {
        this.size = INDICES_CACHE_QUERY_SIZE.get(settings);
        this.expire = INDICES_CACHE_QUERY_EXPIRE.exists(settings) ? INDICES_CACHE_QUERY_EXPIRE.get(settings) : null;
        long sizeInBytes = size.getBytes();
        CacheBuilder<Key, BytesReference> cacheBuilder = CacheBuilder.<Key, BytesReference>builder()
            .setMaximumWeight(sizeInBytes)
            .weigher((k, v) -> k.ramBytesUsed() + v.ramBytesUsed())
            .removalListener(this);
        if (expire != null) {
            cacheBuilder.setExpireAfterAccess(expire);
        }
        cache = cacheBuilder.build();
    }

    @Override
    public void close() {
        cache.invalidateAll();
    }

    void clear(CacheEntity entity) {
        keysToClean.add(new CleanupKey(entity, null));
        cleanCache();
    }

    @Override
    public void onRemoval(RemovalNotification<Key, BytesReference> notification) {
        notification.getKey().entity.onRemoval(notification);
    }

    BytesReference getOrCompute(
        CacheEntity cacheEntity,
        CheckedSupplier<BytesReference, IOException> loader,
        MappingLookup.CacheKey mappingCacheKey,
        DirectoryReader reader,
        BytesReference cacheKey
    ) throws Exception {
        final ESCacheHelper cacheHelper = ElasticsearchDirectoryReader.getESReaderCacheHelper(reader);
        assert cacheHelper != null;
        final Key key = new Key(cacheEntity, mappingCacheKey, cacheHelper.getKey(), cacheKey);
        Loader cacheLoader = new Loader(cacheEntity, loader);
        BytesReference value = cache.computeIfAbsent(key, cacheLoader);
        if (cacheLoader.isLoaded()) {
            key.entity.onMiss();
            // see if its the first time we see this reader, and make sure to register a cleanup key
            CleanupKey cleanupKey = new CleanupKey(cacheEntity, cacheHelper.getKey());
            if (registeredClosedListeners.containsKey(cleanupKey) == false) {
                Boolean previous = registeredClosedListeners.putIfAbsent(cleanupKey, Boolean.TRUE);
                if (previous == null) {
                    cacheHelper.addClosedListener(cleanupKey);
                }
            }
            /*
             * Note that we don't use a closed listener for the mapping. Instead
             * we let cache entries for out of date mappings age out. We do this
             * because we don't reference count the MappingLookup so we can't tell
             * when one is no longer used. Mapping updates should be a lot less
             * frequent than reader closes so this is probably ok. On the other
             * hand, for read only indices mapping changes are, well, possible,
             * and readers are never changed. Oh well.
             */
        } else {
            key.entity.onHit();
        }
        return value;
    }

    /**
     * Invalidates the given the cache entry for the given key and it's context
     * @param cacheEntity the cache entity to invalidate for
     * @param reader the reader to invalidate the cache entry for
     * @param cacheKey the cache key to invalidate
     */
    void invalidate(CacheEntity cacheEntity, MappingLookup.CacheKey mappingCacheKey, DirectoryReader reader, BytesReference cacheKey) {
        assert reader.getReaderCacheHelper() != null;
        cache.invalidate(new Key(cacheEntity, mappingCacheKey, reader.getReaderCacheHelper().getKey(), cacheKey));
    }

    private static class Loader implements CacheLoader<Key, BytesReference> {

        private final CacheEntity entity;
        private final CheckedSupplier<BytesReference, IOException> loader;
        private boolean loaded;

        Loader(CacheEntity entity, CheckedSupplier<BytesReference, IOException> loader) {
            this.entity = entity;
            this.loader = loader;
        }

        public boolean isLoaded() {
            return this.loaded;
        }

        @Override
        public BytesReference load(Key key) throws Exception {
            BytesReference value = loader.get();
            entity.onCached(key, value);
            loaded = true;
            return value;
        }
    }

    /**
     * Basic interface to make this cache testable.
     */
    interface CacheEntity extends Accountable {

        /**
         * Called after the value was loaded.
         */
        void onCached(Key key, BytesReference value);

        /**
         * Returns <code>true</code> iff the resource behind this entity is still open ie.
         * entities associated with it can remain in the cache. ie. IndexShard is still open.
         */
        boolean isOpen();

        /**
         * Returns the cache identity. this is, similar to {@link #isOpen()} the resource identity behind this cache entity.
         * For instance IndexShard is the identity while a CacheEntity is per DirectoryReader. Yet, we group by IndexShard instance.
         */
        Object getCacheIdentity();

        /**
         * Called each time this entity has a cache hit.
         */
        void onHit();

        /**
         * Called each time this entity has a cache miss.
         */
        void onMiss();

        /**
         * Called when this entity instance is removed
         */
        void onRemoval(RemovalNotification<Key, BytesReference> notification);
    }

    static class Key implements Accountable {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Key.class);

        public final CacheEntity entity; // use as identity equality
        public final MappingLookup.CacheKey mappingCacheKey;
        public final Object readerCacheKey;
        public final BytesReference value;

        Key(CacheEntity entity, MappingLookup.CacheKey mappingCacheKey, Object readerCacheKey, BytesReference value) {
            this.entity = entity;
            this.mappingCacheKey = Objects.requireNonNull(mappingCacheKey);
            this.readerCacheKey = Objects.requireNonNull(readerCacheKey);
            this.value = value;
        }

        @Override
        public long ramBytesUsed() {
            return BASE_RAM_BYTES_USED + entity.ramBytesUsed() + value.length();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            if (mappingCacheKey.equals(key.mappingCacheKey) == false) return false;
            if (readerCacheKey.equals(key.readerCacheKey) == false) return false;
            if (entity.getCacheIdentity().equals(key.entity.getCacheIdentity()) == false) return false;
            if (value.equals(key.value) == false) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = entity.getCacheIdentity().hashCode();
            result = 31 * result + mappingCacheKey.hashCode();
            result = 31 * result + readerCacheKey.hashCode();
            result = 31 * result + value.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Key(mappingKey=["
                + mappingCacheKey
                + "],readerKey=["
                + readerCacheKey
                + "],entityKey=["
                + entity.getCacheIdentity()
                + ",value=" // BytesRef's toString already has [] so we don't add it here
                + value.toBytesRef() // BytesRef has a readable toString
                + ")";
        }
    }

    private class CleanupKey implements ESCacheHelper.ClosedListener {
        final CacheEntity entity;
        final Object readerCacheKey;

        private CleanupKey(CacheEntity entity, Object readerCacheKey) {
            this.entity = entity;
            this.readerCacheKey = readerCacheKey;
        }

        @Override
        public void onClose(Object cacheKey) {
            Boolean remove = registeredClosedListeners.remove(this);
            if (remove != null) {
                keysToClean.add(this);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CleanupKey that = (CleanupKey) o;
            if (Objects.equals(readerCacheKey, that.readerCacheKey) == false) return false;
            if (entity.getCacheIdentity().equals(that.entity.getCacheIdentity()) == false) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = entity.getCacheIdentity().hashCode();
            result = 31 * result + Objects.hashCode(readerCacheKey);
            return result;
        }
    }

    synchronized void cleanCache() {
        final Set<CleanupKey> currentKeysToClean = new HashSet<>();
        final Set<Object> currentFullClean = new HashSet<>();
        for (Iterator<CleanupKey> iterator = keysToClean.iterator(); iterator.hasNext();) {
            CleanupKey cleanupKey = iterator.next();
            iterator.remove();
            if (cleanupKey.readerCacheKey == null || cleanupKey.entity.isOpen() == false) {
                // null indicates full cleanup, as does a closed shard
                currentFullClean.add(cleanupKey.entity.getCacheIdentity());
            } else {
                currentKeysToClean.add(cleanupKey);
            }
        }
        if (currentKeysToClean.isEmpty() == false || currentFullClean.isEmpty() == false) {
            for (Iterator<Key> iterator = cache.keys().iterator(); iterator.hasNext();) {
                Key key = iterator.next();
                if (currentFullClean.contains(key.entity.getCacheIdentity())) {
                    iterator.remove();
                } else {
                    if (currentKeysToClean.contains(new CleanupKey(key.entity, key.readerCacheKey))) {
                        iterator.remove();
                    }
                }
            }
        }

        cache.refresh();
    }

    /**
     * Returns the current size of the cache
     */
    int count() {
        return cache.count();
    }

    Iterable<Key> cachedKeys() {
        return cache.keys();
    }

    int numRegisteredCloseListeners() { // for testing
        return registeredClosedListeners.size();
    }
}
