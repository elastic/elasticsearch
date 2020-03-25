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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public final class ShardGenerations {

    public static final ShardGenerations EMPTY = new ShardGenerations(Collections.emptyMap());

    /**
     * Special generation that signifies that a shard is new and the repository does not yet contain a valid
     * {@link org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots} blob for it.
     */
    public static final String NEW_SHARD_GEN = "_new";

    /**
     * Special generation that signifies that the shard has been deleted from the repository.
     * This generation is only used during computations. It should never be written to disk.
     */
    public static final String DELETED_SHARD_GEN = "_deleted";

    private final Map<IndexId, List<String>> shardGenerations;

    private ShardGenerations(Map<IndexId, List<String>> shardGenerations) {
        this.shardGenerations = shardGenerations;
    }

    /**
     * Returns the total number of shards tracked by this instance.
     */
    public int totalShards() {
        return shardGenerations.values().stream().mapToInt(List::size).sum();
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
     * Computes the obsolete shard index generations that can be deleted once this instance was written to the repository.
     * Note: This method should only be used when finalizing a snapshot and we can safely assume that data has only been added but not
     *       removed from shard paths.
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
            for (int i = 0; i < Math.min(oldGens.size(), updatedGenerations.size()); i++) {
                final String oldGeneration = oldGens.get(i);
                final String updatedGeneration = updatedGenerations.get(i);
                // If we had a previous generation that is different from an updated generation it's obsolete
                // Since this method assumes only additions and no removals of shards, a null updated generation means no update
                if (updatedGeneration != null && oldGeneration != null && oldGeneration.equals(updatedGeneration) == false) {
                    obsoleteShardIndices.put(i, oldGeneration);
                }
            }
            result.put(indexId, Collections.unmodifiableMap(obsoleteShardIndices));
        }));
        return Collections.unmodifiableMap(result);
    }

    /**
     * Get the generation of the {@link org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots} blob for a given index
     * and shard.
     * There are three special kinds of generations that can be returned here.
     * <ul>
     *     <li>{@link #DELETED_SHARD_GEN} a deleted shard that isn't referenced by any snapshot in the repository any longer</li>
     *     <li>{@link #NEW_SHARD_GEN} a new shard that we know doesn't hold any valid data yet in the repository</li>
     *     <li>{@code null} unknown state. The shard either does not exist at all or it was created by a node older than
     *     {@link org.elasticsearch.snapshots.SnapshotsService#SHARD_GEN_IN_REPO_DATA_VERSION}. If a caller expects a shard to exist in the
     *     repository but sees a {@code null} return, it should try to recover the generation by falling back to listing the contents
     *     of the respective shard directory.</li>
     * </ul>
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

    public List<String> getGens(IndexId indexId) {
        final List<String> existing = shardGenerations.get(indexId);
        return existing == null ? Collections.emptyList() : Collections.unmodifiableList(existing);
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

    @Override
    public String toString() {
        return "ShardGenerations{generations:" + this.shardGenerations + "}";
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private final Map<IndexId, Map<Integer, String>> generations = new HashMap<>();

        /**
         * Filters out all generations that don't belong to any of the supplied {@code indices} and prunes all {@link #DELETED_SHARD_GEN}
         * entries from the builder.
         *
         * @param indices indices to filter for
         * @return builder that contains only the given {@code indices} and no {@link #DELETED_SHARD_GEN} entries
         */
        public Builder retainIndicesAndPruneDeletes(Set<IndexId> indices) {
            generations.keySet().retainAll(indices);
            for (IndexId index : indices) {
                final Map<Integer, String> shards = generations.getOrDefault(index, Collections.emptyMap());
                final Iterator<Map.Entry<Integer, String>> iterator = shards.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<Integer, String> entry = iterator.next();
                    final String generation = entry.getValue();
                    if (generation.equals(DELETED_SHARD_GEN)) {
                        iterator.remove();
                    }
                }
                if (shards.isEmpty()) {
                    generations.remove(index);
                }
            }
            return this;
        }

        public Builder putAll(ShardGenerations shardGenerations) {
            shardGenerations.shardGenerations.forEach((indexId, gens) -> {
                for (int i = 0; i < gens.size(); i++) {
                    final String gen = gens.get(i);
                    if (gen != null) {
                        put(indexId, i, gens.get(i));
                    }
                }
            });
            return this;
        }

        public Builder put(IndexId indexId, int shardId, String generation) {
            generations.computeIfAbsent(indexId, i -> new HashMap<>()).put(shardId, generation);
            return this;
        }

        public ShardGenerations build() {
            return new ShardGenerations(generations.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                    final Set<Integer> shardIds = entry.getValue().keySet();
                    assert shardIds.isEmpty() == false;
                    final int size = shardIds.stream().mapToInt(i -> i).max().getAsInt() + 1;
                    // Create a list that can hold the highest shard id as index and leave null values for shards that don't have
                    // a map entry.
                    final String[] gens = new String[size];
                    entry.getValue().forEach((shardId, generation) -> gens[shardId] = generation);
                    return Arrays.asList(gens);
                }
            )));
        }
    }
}
