/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.Assertions;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A map between segment core cache keys and the shard that these segments
 * belong to. This allows to get the shard that a segment belongs to or to get
 * the entire set of live core cache keys for a given index. In order to work
 * this class needs to be notified about new segments. It modifies the current
 * mappings as segments that were not known before are added and prevents the
 * structure from growing indefinitely by registering close listeners on these
 * segments so that at any time it only tracks live segments.
 *
 * NOTE: This is heavy. Avoid using this class unless absolutely required.
 */
public final class ShardCoreKeyMap {

    private final Map<IndexReader.CacheKey, ShardId> coreKeyToShard;
    private final Map<String, Set<IndexReader.CacheKey>> indexToCoreKey;

    public ShardCoreKeyMap() {
        coreKeyToShard = new ConcurrentHashMap<>();
        indexToCoreKey = new HashMap<>();
    }

    /**
     * Register a {@link LeafReader}. This is necessary so that the core cache
     * key of this reader can be found later using {@link #getCoreKeysForIndex(String)}.
     */
    public void add(LeafReader reader) {
        final IndexReader.CacheHelper cacheHelper = reader.getCoreCacheHelper();
        if (cacheHelper == null) {
            throwReaderNotSupportCaching(reader);
        }
        final IndexReader.CacheKey coreKey = cacheHelper.getKey();

        if (coreKeyToShard.containsKey(coreKey)) {
            // Do this check before entering the synchronized block in #doAdd in order to
            // avoid taking the mutex if possible (which should happen most of
            // the time).
            return;
        }

        doAdd(reader, cacheHelper, coreKey);
    }

    private static void throwReaderNotSupportCaching(LeafReader reader) {
        throw new IllegalArgumentException("Reader " + reader + " does not support caching");
    }

    private void doAdd(LeafReader reader, IndexReader.CacheHelper cacheHelper, IndexReader.CacheKey coreKey) {
        final ShardId shardId = ShardUtils.extractShardId(reader);
        if (shardId == null) {
            throw new IllegalArgumentException("Could not extract shard id from " + reader);
        }
        final String index = shardId.getIndexName();
        synchronized (this) {
            if (coreKeyToShard.containsKey(coreKey)) {
                return;
            }
            final boolean added = indexToCoreKey.computeIfAbsent(index, k -> new HashSet<>()).add(coreKey);
            assert added;
            IndexReader.ClosedListener listener = ownerCoreCacheKey -> {
                assert coreKey == ownerCoreCacheKey;
                synchronized (ShardCoreKeyMap.this) {
                    coreKeyToShard.remove(ownerCoreCacheKey);
                    final Set<IndexReader.CacheKey> coreKeys = indexToCoreKey.get(index);
                    final boolean removed = coreKeys.remove(coreKey);
                    assert removed;
                    if (coreKeys.isEmpty()) {
                        indexToCoreKey.remove(index);
                    }
                }
            };
            boolean addedListener = false;
            try {
                cacheHelper.addClosedListener(listener);
                addedListener = true;

                // Only add the core key to the map as a last operation so that
                // if another thread sees that the core key is already in the
                // map (like the check just before this synchronized block),
                // then it means that the closed listener has already been
                // registered.
                ShardId previous = coreKeyToShard.put(coreKey, shardId);
                assert previous == null;
            } finally {
                if (false == addedListener) {
                    try {
                        listener.onClose(coreKey);
                    } catch (IOException e) {
                        throw new RuntimeException("Blow up trying to recover from failure to add listener", e);
                    }
                }
            }
        }
    }

    /**
     * Return the {@link ShardId} that holds the given segment, or {@code null}
     * if this segment is not tracked.
     */
    public synchronized ShardId getShardId(Object coreKey) {
        return coreKeyToShard.get(coreKey);
    }

    /**
     * Get the set of core cache keys associated with the given index.
     */
    public synchronized Set<Object> getCoreKeysForIndex(String index) {
        final Set<IndexReader.CacheKey> objects = indexToCoreKey.get(index);
        if (objects == null) {
            return Collections.emptySet();
        }
        // we have to copy otherwise we risk ConcurrentModificationException
        return Set.copyOf(objects);
    }

    /**
     * Return the number of tracked segments.
     */
    public synchronized int size() {
        assert assertSize();
        return coreKeyToShard.size();
    }

    private synchronized boolean assertSize() {
        if (Assertions.ENABLED == false) {
            throw new AssertionError("only run this if assertions are enabled");
        }
        Collection<Set<IndexReader.CacheKey>> values = indexToCoreKey.values();
        int size = 0;
        for (Set<IndexReader.CacheKey> value : values) {
            size += value.size();
        }
        return size == coreKeyToShard.size();
    }

}
