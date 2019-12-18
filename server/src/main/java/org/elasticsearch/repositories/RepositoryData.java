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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A class that represents the data in a repository, as captured in the
 * repository's index blob.
 */
public final class RepositoryData {

    /**
     * The generation value indicating the repository has no index generational files.
     */
    public static final long EMPTY_REPO_GEN = -1L;

    /**
     * The generation value indicating that the repository generation is unknown.
     */
    public static final long UNKNOWN_REPO_GEN = -2L;

    /**
     * The generation value indicating that the repository generation could not be determined.
     */
    public static final long CORRUPTED_REPO_GEN = -3L;

    /**
     * An instance initialized for an empty repository.
     */
    public static final RepositoryData EMPTY = new RepositoryData(EMPTY_REPO_GEN,
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), ShardGenerations.EMPTY);

    /**
     * The generational id of the index file from which the repository data was read.
     */
    private final long genId;
    /**
     * The ids of the snapshots in the repository.
     */
    private final Map<String, SnapshotId> snapshotIds;
    /**
     * The states of each snapshot in the repository.
     */
    private final Map<String, SnapshotState> snapshotStates;
    /**
     * The indices found in the repository across all snapshots, as a name to {@link IndexId} mapping
     */
    private final Map<String, IndexId> indices;
    /**
     * The snapshots that each index belongs to.
     */
    private final Map<IndexId, Set<SnapshotId>> indexSnapshots;

    /**
     * Shard generations.
     */
    private final ShardGenerations shardGenerations;

    public RepositoryData(long genId, Map<String, SnapshotId> snapshotIds, Map<String, SnapshotState> snapshotStates,
                          Map<IndexId, Set<SnapshotId>> indexSnapshots, ShardGenerations shardGenerations) {
        this.genId = genId;
        this.snapshotIds = Collections.unmodifiableMap(snapshotIds);
        this.snapshotStates = Collections.unmodifiableMap(snapshotStates);
        this.indices = Collections.unmodifiableMap(indexSnapshots.keySet().stream()
            .collect(Collectors.toMap(IndexId::getName, Function.identity())));
        this.indexSnapshots = Collections.unmodifiableMap(indexSnapshots);
        this.shardGenerations = Objects.requireNonNull(shardGenerations);
        assert indices.values().containsAll(shardGenerations.indices()) : "ShardGenerations contained indices "
            + shardGenerations.indices() + " but snapshots only reference indices " + indices.values();
    }

    protected RepositoryData copy() {
        return new RepositoryData(genId, snapshotIds, snapshotStates, indexSnapshots, shardGenerations);
    }

    public ShardGenerations shardGenerations() {
        return shardGenerations;
    }

    /**
     * Gets the generational index file id from which this instance was read.
     */
    public long getGenId() {
        return genId;
    }

    /**
     * Returns an unmodifiable collection of the snapshot ids.
     */
    public Collection<SnapshotId> getSnapshotIds() {
        return Collections.unmodifiableCollection(snapshotIds.values());
    }

    /**
     * Returns the {@link SnapshotState} for the given snapshot.  Returns {@code null} if
     * there is no state for the snapshot.
     */
    @Nullable
    public SnapshotState getSnapshotState(final SnapshotId snapshotId) {
        return snapshotStates.get(snapshotId.getUUID());
    }

    /**
     * Returns an unmodifiable map of the index names to {@link IndexId} in the repository.
     */
    public Map<String, IndexId> getIndices() {
        return indices;
    }

    /**
     * Returns the list of {@link IndexId} that have their snapshots updated but not removed (because they are still referenced by other
     * snapshots) after removing the given snapshot from the repository.
     *
     * @param snapshotId SnapshotId to remove
     * @return List of indices that are changed but not removed
     */
    public List<IndexId> indicesToUpdateAfterRemovingSnapshot(SnapshotId snapshotId) {
        return indexSnapshots.entrySet().stream()
            .filter(entry -> entry.getValue().size() > 1 && entry.getValue().contains(snapshotId))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    }

    /**
     * Add a snapshot and its indices to the repository; returns a new instance.  If the snapshot
     * already exists in the repository data, this method throws an IllegalArgumentException.
     *
     * @param snapshotId       Id of the new snapshot
     * @param snapshotState    State of the new snapshot
     * @param shardGenerations Updated shard generations in the new snapshot. For each index contained in the snapshot an array of new
     *                         generations indexed by the shard id they correspond to must be supplied.
     */
    public RepositoryData addSnapshot(final SnapshotId snapshotId,
                                      final SnapshotState snapshotState,
                                      final ShardGenerations shardGenerations) {
        if (snapshotIds.containsKey(snapshotId.getUUID())) {
            // if the snapshot id already exists in the repository data, it means an old master
            // that is blocked from the cluster is trying to finalize a snapshot concurrently with
            // the new master, so we make the operation idempotent
            return this;
        }
        Map<String, SnapshotId> snapshots = new HashMap<>(snapshotIds);
        snapshots.put(snapshotId.getUUID(), snapshotId);
        Map<String, SnapshotState> newSnapshotStates = new HashMap<>(snapshotStates);
        newSnapshotStates.put(snapshotId.getUUID(), snapshotState);
        Map<IndexId, Set<SnapshotId>> allIndexSnapshots = new HashMap<>(indexSnapshots);
        for (final IndexId indexId : shardGenerations.indices()) {
            allIndexSnapshots.computeIfAbsent(indexId, k -> new LinkedHashSet<>()).add(snapshotId);
        }
        return new RepositoryData(genId, snapshots, newSnapshotStates, allIndexSnapshots,
            ShardGenerations.builder().putAll(this.shardGenerations).putAll(shardGenerations).build());
    }

    /**
     * Create a new instance with the given generation and all other fields equal to this instance.
     *
     * @param newGeneration New Generation
     * @return New instance
     */
    public RepositoryData withGenId(long newGeneration) {
        if (newGeneration == genId) {
            return this;
        }
        return new RepositoryData(newGeneration, this.snapshotIds, this.snapshotStates, this.indexSnapshots, this.shardGenerations);
    }

    /**
     * Remove a snapshot and remove any indices that no longer exist in the repository due to the deletion of the snapshot.
     *
     * @param snapshotId              Snapshot Id
     * @param updatedShardGenerations Shard generations that changed as a result of removing the snapshot.
     *                                The {@code String[]} passed for each {@link IndexId} contains the new shard generation id for each
     *                                changed shard indexed by its shardId
     */
    public RepositoryData removeSnapshot(final SnapshotId snapshotId, final ShardGenerations updatedShardGenerations) {
        Map<String, SnapshotId> newSnapshotIds = snapshotIds.values().stream()
            .filter(id -> !snapshotId.equals(id))
            .collect(Collectors.toMap(SnapshotId::getUUID, Function.identity()));
        if (newSnapshotIds.size() == snapshotIds.size()) {
            throw new ResourceNotFoundException("Attempting to remove non-existent snapshot [{}] from repository data", snapshotId);
        }
        Map<String, SnapshotState> newSnapshotStates = new HashMap<>(snapshotStates);
        newSnapshotStates.remove(snapshotId.getUUID());
        Map<IndexId, Set<SnapshotId>> indexSnapshots = new HashMap<>();
        for (final IndexId indexId : indices.values()) {
            Set<SnapshotId> set;
            Set<SnapshotId> snapshotIds = this.indexSnapshots.get(indexId);
            assert snapshotIds != null;
            if (snapshotIds.contains(snapshotId)) {
                if (snapshotIds.size() == 1) {
                    // removing the snapshot will mean no more snapshots
                    // have this index, so just skip over it
                    continue;
                }
                set = new LinkedHashSet<>(snapshotIds);
                set.remove(snapshotId);
            } else {
                set = snapshotIds;
            }
            indexSnapshots.put(indexId, set);
        }

        return new RepositoryData(genId, newSnapshotIds, newSnapshotStates, indexSnapshots,
            ShardGenerations.builder().putAll(shardGenerations).putAll(updatedShardGenerations)
                .retainIndicesAndPruneDeletes(indexSnapshots.keySet()).build()
        );
    }

    /**
     * Returns an immutable collection of the snapshot ids for the snapshots that contain the given index.
     */
    public Set<SnapshotId> getSnapshots(final IndexId indexId) {
        Set<SnapshotId> snapshotIds = indexSnapshots.get(indexId);
        if (snapshotIds == null) {
            throw new IllegalArgumentException("unknown snapshot index " + indexId);
        }
        return snapshotIds;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RepositoryData that = (RepositoryData) obj;
        return snapshotIds.equals(that.snapshotIds)
                   && snapshotStates.equals(that.snapshotStates)
                   && indices.equals(that.indices)
                   && indexSnapshots.equals(that.indexSnapshots)
                   && shardGenerations.equals(that.shardGenerations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotIds, snapshotStates, indices, indexSnapshots, shardGenerations);
    }

    /**
     * Resolve the index name to the index id specific to the repository,
     * throwing an exception if the index could not be resolved.
     */
    public IndexId resolveIndexId(final String indexName) {
        return Objects.requireNonNull(indices.get(indexName), () -> "Tried to resolve unknown index [" + indexName + "]");
    }

    /**
     * Resolve the given index names to index ids.
     */
    public List<IndexId> resolveIndices(final List<String> indices) {
        List<IndexId> resolvedIndices = new ArrayList<>(indices.size());
        for (final String indexName : indices) {
            resolvedIndices.add(resolveIndexId(indexName));
        }
        return resolvedIndices;
    }

    /**
     * Resolve the given index names to index ids, creating new index ids for
     * new indices in the repository.
     */
    public List<IndexId> resolveNewIndices(final List<String> indicesToResolve) {
        List<IndexId> snapshotIndices = new ArrayList<>();
        for (String index : indicesToResolve) {
            final IndexId indexId;
            if (indices.containsKey(index)) {
                indexId = indices.get(index);
            } else {
                indexId = new IndexId(index, UUIDs.randomBase64UUID());
            }
            snapshotIndices.add(indexId);
        }
        return snapshotIndices;
    }

    private static final String SHARD_GENERATIONS = "shard_generations";
    private static final String SNAPSHOTS = "snapshots";
    private static final String INDICES = "indices";
    private static final String INDEX_ID = "id";
    private static final String NAME = "name";
    private static final String UUID = "uuid";
    private static final String STATE = "state";

    /**
     * Writes the snapshots metadata and the related indices metadata to x-content.
     */
    public XContentBuilder snapshotsToXContent(final XContentBuilder builder, final boolean shouldWriteShardGens) throws IOException {
        builder.startObject();
        // write the snapshots list
        builder.startArray(SNAPSHOTS);
        for (final SnapshotId snapshot : getSnapshotIds()) {
            builder.startObject();
            builder.field(NAME, snapshot.getName());
            builder.field(UUID, snapshot.getUUID());
            if (snapshotStates.containsKey(snapshot.getUUID())) {
                builder.field(STATE, snapshotStates.get(snapshot.getUUID()).value());
            }
            builder.endObject();
        }
        builder.endArray();
        // write the indices map
        builder.startObject(INDICES);
        for (final IndexId indexId : getIndices().values()) {
            builder.startObject(indexId.getName());
            builder.field(INDEX_ID, indexId.getId());
            builder.startArray(SNAPSHOTS);
            Set<SnapshotId> snapshotIds = indexSnapshots.get(indexId);
            assert snapshotIds != null;
            for (final SnapshotId snapshotId : snapshotIds) {
                builder.value(snapshotId.getUUID());
            }
            builder.endArray();
            if (shouldWriteShardGens) {
                builder.startArray(SHARD_GENERATIONS);
                for (String gen : shardGenerations.getGens(indexId)) {
                    builder.value(gen);
                }
                builder.endArray();
            }
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    /**
     * Reads an instance of {@link RepositoryData} from x-content, loading the snapshots and indices metadata.
     */
    public static RepositoryData snapshotsFromXContent(final XContentParser parser, long genId) throws IOException {
        final Map<String, SnapshotId> snapshots = new HashMap<>();
        final Map<String, SnapshotState> snapshotStates = new HashMap<>();
        final Map<IndexId, Set<SnapshotId>> indexSnapshots = new HashMap<>();
        final ShardGenerations.Builder shardGenerations = ShardGenerations.builder();

        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                String field = parser.currentName();
                if (SNAPSHOTS.equals(field)) {
                    if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            String name = null;
                            String uuid = null;
                            SnapshotState state = null;
                            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                                String currentFieldName = parser.currentName();
                                parser.nextToken();
                                if (NAME.equals(currentFieldName)) {
                                    name = parser.text();
                                } else if (UUID.equals(currentFieldName)) {
                                    uuid = parser.text();
                                } else if (STATE.equals(currentFieldName)) {
                                    state = SnapshotState.fromValue(parser.numberValue().byteValue());
                                }
                            }
                            final SnapshotId snapshotId = new SnapshotId(name, uuid);
                            if (state != null) {
                                snapshotStates.put(uuid, state);
                            }
                            snapshots.put(snapshotId.getUUID(), snapshotId);
                        }
                    } else {
                        throw new ElasticsearchParseException("expected array for [" + field + "]");
                    }
                } else if (INDICES.equals(field)) {
                    if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                        throw new ElasticsearchParseException("start object expected [indices]");
                    }
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        final String indexName = parser.currentName();
                        final Set<SnapshotId> snapshotIds = new LinkedHashSet<>();
                        final List<String> gens = new ArrayList<>();

                        IndexId indexId = null;
                        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                            throw new ElasticsearchParseException("start object expected index[" + indexName + "]");
                        }
                        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                            final String indexMetaFieldName = parser.currentName();
                            parser.nextToken();
                            if (INDEX_ID.equals(indexMetaFieldName)) {
                                indexId = new IndexId(indexName, parser.text());
                            } else if (SNAPSHOTS.equals(indexMetaFieldName)) {
                                if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
                                    throw new ElasticsearchParseException("start array expected [snapshots]");
                                }
                                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                    String uuid = null;
                                    // the old format pre 5.4.1 which contains the snapshot name and uuid
                                    if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                                        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                                            String currentFieldName = parser.currentName();
                                            parser.nextToken();
                                            if (UUID.equals(currentFieldName)) {
                                                uuid = parser.text();
                                            }
                                        }
                                    } else {
                                        // the new format post 5.4.1 that only contains the snapshot uuid,
                                        // since we already have the name/uuid combo in the snapshots array
                                        uuid = parser.text();
                                    }

                                    SnapshotId snapshotId = snapshots.get(uuid);
                                    if (snapshotId != null) {
                                        snapshotIds.add(snapshotId);
                                    } else {
                                        // A snapshotted index references a snapshot which does not exist in
                                        // the list of snapshots. This can happen when multiple clusters in
                                        // different versions create or delete snapshot in the same repository.
                                        throw new ElasticsearchParseException("Detected a corrupted repository, index " + indexId
                                            + " references an unknown snapshot uuid [" + uuid + "]");
                                    }
                                }
                            } else if (SHARD_GENERATIONS.equals(indexMetaFieldName)) {
                                XContentParserUtils.ensureExpectedToken(
                                    XContentParser.Token.START_ARRAY, parser.currentToken(), parser::getTokenLocation);
                                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                    gens.add(parser.textOrNull());
                                }

                            }
                        }
                        assert indexId != null;
                        indexSnapshots.put(indexId, snapshotIds);
                        for (int i = 0; i < gens.size(); i++) {
                            shardGenerations.put(indexId, i, gens.get(i));
                        }
                    }
                } else {
                    throw new ElasticsearchParseException("unknown field name  [" + field + "]");
                }
            }
        } else {
            throw new ElasticsearchParseException("start object expected");
        }
        return new RepositoryData(genId, snapshots, snapshotStates, indexSnapshots, shardGenerations.build());
    }

}
