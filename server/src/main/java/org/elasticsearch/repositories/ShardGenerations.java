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

package org.elasticsearch.repositories;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public final class ShardGenerations implements ToXContent {

    public static final ShardGenerations EMPTY = ShardGenerations.builder().build();

    private final Map<IndexId, List<String>> shardGenerations;

    private ShardGenerations(Map<IndexId, List<String>> shardGenerations) {
        this.shardGenerations = shardGenerations;
    }

    /**
     * Returns all indices for which shard generations are tracked.
     *
     * @return indices for which shard generations are tracked
     */
    public Collection<IndexId> indices() {
        return Collections.unmodifiableSet(shardGenerations.keySet());
    }

    /**
     * Computes the obsolete shard index generations that can be deleted if this instance was written to the repository.
     * NOTE: Indices that are only found in {@code previous} but not in this instance are not included in the result.
     *
     * @param previous Previous {@code ShardGenerations}
     * @return Map of obsolete shard index generations in indices that are still tracked by this instance
     */
    public Map<IndexId, Map<Integer, String>> obsoleteShardGenerations(ShardGenerations previous) {
        final Map<IndexId, Map<Integer, String>> result = new HashMap<>();
        previous.shardGenerations.forEach(((indexId, oldGens) -> {
            final List<String> updatedGenerations = shardGenerations.get(indexId);
            final Map<Integer, String> obsoleteShardIndices = new HashMap<>();
            assert updatedGenerations != null
                : "Index [" + indexId + "] present in previous shard generations, but missing from updated generations";
            for (int i = 0; i < oldGens.size(); i++) {
                final String oldGeneration = oldGens.get(i);
                final String updatedGeneration = updatedGenerations.get(i);
                assert updatedGeneration != null : "Can't update from a non-null generation to a null generation";
                if (oldGeneration != null && oldGeneration.equals(updatedGeneration) == false) {
                    obsoleteShardIndices.put(i, oldGeneration);
                }
            }
            result.put(indexId, obsoleteShardIndices);
        }));
        return result;
    }

    /**
     * Get the generation of the {@link org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots} blob for a given index
     * and shard.
     *
     * @param indexId IndexId
     * @param shardId Shard Id
     * @return generation of the {@link org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots} blob
     */
    @Nullable
    public String getShardGen(IndexId indexId, int shardId) {
        final List<String> generations = shardGenerations.get(indexId);
        if (generations == null || generations.size() < shardId + 1) {
            return null;
        }
        return generations.get(shardId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(RepositoryData.SHARDS);
        for (Map.Entry<IndexId, List<String>> entry : shardGenerations.entrySet()) {
            builder.array(entry.getKey().getId(), entry.getValue().toArray(Strings.EMPTY_ARRAY));
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ShardGenerations that = (ShardGenerations) o;
        return shardGenerations.equals(that.shardGenerations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardGenerations);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private final Map<IndexId, Map<Integer, String>> raw = new HashMap<>();

        public Builder filterByIndices(Set<IndexId> indices) {
            raw.keySet().retainAll(indices);
            return this;
        }

        public Builder add(ShardGenerations shardGenerations) {
            shardGenerations.shardGenerations.forEach((indexId, gens) -> {
                for (int i = 0; i < gens.size(); i++) {
                    final String gen = gens.get(i);
                    if (gen != null) {
                        add(indexId, i, gens.get(i));
                    }
                }
            });
            return this;
        }

        public Builder add(IndexId indexId, int shardId, String generation) {
            raw.computeIfAbsent(indexId, i -> new HashMap<>()).put(shardId, generation);
            return this;
        }

        public ShardGenerations build() {
            return new ShardGenerations(raw.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                    final Set<Integer> shardIds = entry.getValue().keySet();
                    assert shardIds.isEmpty() == false;
                    final int size = shardIds.stream().mapToInt(i -> i).max().getAsInt() + 1;
                    final String[] gens = new String[size];
                    entry.getValue().forEach((shardId, generation) -> gens[shardId] = generation);
                    return Arrays.asList(gens);
                }
            )));
        }
    }
}
