/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Represents the current {@link ShardGeneration} for each shard in a repository.
 */
public final class ShardGenerations {

    public static final ShardGenerations EMPTY = new ShardGenerations(Collections.emptyMap());

    /**
     * Special generation that signifies that a shard is new and the repository does not yet contain a valid
     * {@link org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots} blob for it.
     */
    public static final ShardGeneration NEW_SHARD_GEN = new ShardGeneration("_new");

    /**
     * Special generation that signifies that the shard has been deleted from the repository.
     * This generation is only used during computations. It should never be written to disk.
     */
    public static final ShardGeneration DELETED_SHARD_GEN = new ShardGeneration("_deleted");

    /**
     * For each index, the list of shard generations for its shards (i.e. the generation for shard i is at the i'th position of the list)
     */
    private final Map<IndexId, List<ShardGeneration>> shardGenerations;

    private ShardGenerations(Map<IndexId, List<ShardGeneration>> shardGenerations) {
        this.shardGenerations = Map.copyOf(shardGenerations);
    }

    private static final Pattern IS_NUMBER = Pattern.compile("^\\d+$");

    /**
     * Filters out unreliable numeric shard generations read from {@link RepositoryData} or {@link IndexShardSnapshotStatus}, returning
     * {@code null} in their place.
     * @see <a href="https://github.com/elastic/elasticsearch/issues/57798">Issue #57988</a>
     *
     * @param shardGeneration shard generation to fix
     * @return given shard generation or {@code null} if it was filtered out or {@code null} was passed
     */
    @Nullable
    public static ShardGeneration fixShardGeneration(@Nullable ShardGeneration shardGeneration) {
        if (shardGeneration == null) {
            return null;
        }
        return IS_NUMBER.matcher(shardGeneration.toString()).matches() ? null : shardGeneration;
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
        return shardGenerations.keySet();
    }

    /**
     * Computes the obsolete shard index generations that can be deleted once this instance was written to the repository.
     * Note: This method should only be used when finalizing a snapshot and we can safely assume that data has only been added but not
     *       removed from shard paths.
     *
     * @param previous Previous {@code ShardGenerations}
     * @return Map of obsolete shard index generations in indices that are still tracked by this instance
     */
    public Map<IndexId, Map<Integer, ShardGeneration>> obsoleteShardGenerations(ShardGenerations previous) {
        final Map<IndexId, Map<Integer, ShardGeneration>> result = new HashMap<>();
        previous.shardGenerations.forEach(((indexId, oldGens) -> {
            final List<ShardGeneration> updatedGenerations = shardGenerations.get(indexId);
            final Map<Integer, ShardGeneration> obsoleteShardIndices = new HashMap<>();
            assert updatedGenerations != null
                : "Index [" + indexId + "] present in previous shard generations, but missing from updated generations";
            for (int i = 0; i < Math.min(oldGens.size(), updatedGenerations.size()); i++) {
                final ShardGeneration oldGeneration = oldGens.get(i);
                final ShardGeneration updatedGeneration = updatedGenerations.get(i);
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
    public ShardGeneration getShardGen(IndexId indexId, int shardId) {
        final List<ShardGeneration> generations = shardGenerations.get(indexId);
        if (generations == null || generations.size() < shardId + 1) {
            return null;
        }
        return generations.get(shardId);
    }

    public List<ShardGeneration> getGens(IndexId indexId) {
        return shardGenerations.getOrDefault(indexId, Collections.emptyList());
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

        private final Map<IndexId, Map<Integer, ShardGeneration>> generations = new HashMap<>();

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
                final Map<Integer, ShardGeneration> shards = generations.getOrDefault(index, Collections.emptyMap());
                final Iterator<Map.Entry<Integer, ShardGeneration>> iterator = shards.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<Integer, ShardGeneration> entry = iterator.next();
                    final ShardGeneration generation = entry.getValue();
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
                    final ShardGeneration gen = gens.get(i);
                    if (gen != null) {
                        put(indexId, i, gen);
                    }
                }
            });
            return this;
        }

        public Builder put(IndexId indexId, int shardId, ShardGeneration generation) {
            ShardGeneration existingGeneration = generations.computeIfAbsent(indexId, i -> new HashMap<>()).put(shardId, generation);
            assert generation != null || existingGeneration == null
                : "must not overwrite existing generation with null generation [" + existingGeneration + "]";
            return this;
        }

        public ShardGenerations build() {
            return new ShardGenerations(generations.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                final Set<Integer> shardIds = entry.getValue().keySet();
                assert shardIds.isEmpty() == false;
                final int size = shardIds.stream().mapToInt(i -> i).max().getAsInt() + 1;
                // Create a list that can hold the highest shard id as index and leave null values for shards that don't have
                // a map entry.
                final ShardGeneration[] gens = new ShardGeneration[size];
                entry.getValue().forEach((shardId, generation) -> gens[shardId] = generation);
                return Collections.unmodifiableList(Arrays.asList(gens));
            })));
        }
    }
}
