/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.lucene;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReader.CoreClosedListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

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

    private final Map<Object, ShardId> coreKeyToShard;
    private final Multimap<String, Object> indexToCoreKey;

    public ShardCoreKeyMap() {
        coreKeyToShard = new IdentityHashMap<>();
        indexToCoreKey = HashMultimap.create();
    }

    /**
     * Register a {@link LeafReader}. This is necessary so that the core cache
     * key of this reader can be found later using {@link #getShardId(Object)}.
     */
    public void add(LeafReader reader) {
        final ShardId shardId = ShardUtils.extractShardId(reader);
        if (shardId == null) {
            throw new IllegalArgumentException("Could not extract shard id from " + reader);
        }
        final Object coreKey = reader.getCoreCacheKey();
        final String index = shardId.getIndex();
        synchronized (this) {
            if (coreKeyToShard.put(coreKey, shardId) == null) {
                final boolean added = indexToCoreKey.put(index, coreKey);
                assert added;
                CoreClosedListener listener = new CoreClosedListener() {
                    @Override
                    public void onClose(Object ownerCoreCacheKey) throws IOException {
                        assert coreKey == ownerCoreCacheKey;
                        synchronized (ShardCoreKeyMap.this) {
                            coreKeyToShard.remove(ownerCoreCacheKey);
                            indexToCoreKey.remove(index, coreKey);
                        }
                    }
                };
                boolean addedListener = false;
                try {
                    reader.addCoreClosedListener(listener);
                    addedListener = true;
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
        return ImmutableSet.copyOf(indexToCoreKey.get(index));
    }

    /**
     * Return the number of tracked segments.
     */
    public synchronized int size() {
        assert indexToCoreKey.size() == coreKeyToShard.size();
        return coreKeyToShard.size();
    }

}
