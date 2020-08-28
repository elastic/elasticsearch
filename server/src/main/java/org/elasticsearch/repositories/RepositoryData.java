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
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.snapshots.SnapshotsService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
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
    public static final RepositoryData EMPTY = new RepositoryData(EMPTY_REPO_GEN, Collections.emptyMap(), Collections.emptyMap(),
            Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), ShardGenerations.EMPTY,
            IndexMetaDataGenerations.EMPTY);

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
    private final Map<IndexId, List<SnapshotId>> indexSnapshots;

    private final Map<String, Version> snapshotVersions;

    /**
     * Index metadata generations.
     */
    private final IndexMetaDataGenerations indexMetaDataGenerations;

    /**
     * Shard generations.
     */
    private final ShardGenerations shardGenerations;

    public RepositoryData(long genId, Map<String, SnapshotId> snapshotIds, Map<String, SnapshotState> snapshotStates,
                          Map<String, Version> snapshotVersions, Map<IndexId, List<SnapshotId>> indexSnapshots,
                          ShardGenerations shardGenerations, IndexMetaDataGenerations indexMetaDataGenerations) {
        this(genId, Collections.unmodifiableMap(snapshotIds), Collections.unmodifiableMap(snapshotStates),
                Collections.unmodifiableMap(snapshotVersions),
                indexSnapshots.keySet().stream().collect(Collectors.toUnmodifiableMap(IndexId::getName, Function.identity())),
                Collections.unmodifiableMap(indexSnapshots), shardGenerations, indexMetaDataGenerations);
    }

    private RepositoryData(long genId, Map<String, SnapshotId> snapshotIds, Map<String, SnapshotState> snapshotStates,
                           Map<String, Version> snapshotVersions, Map<String, IndexId> indices,
                           Map<IndexId, List<SnapshotId>> indexSnapshots, ShardGenerations shardGenerations,
                           IndexMetaDataGenerations indexMetaDataGenerations) {
        this.genId = genId;
        this.snapshotIds = snapshotIds;
        this.snapshotStates = snapshotStates;
        this.indices = indices;
        this.indexSnapshots = indexSnapshots;
        this.shardGenerations = shardGenerations;
        this.indexMetaDataGenerations = indexMetaDataGenerations;
        this.snapshotVersions = snapshotVersions;
        assert indices.values().containsAll(shardGenerations.indices()) : "ShardGenerations contained indices "
            + shardGenerations.indices() + " but snapshots only reference indices " + indices.values();
        assert indexSnapshots.values().stream().noneMatch(snapshotIdList -> Set.copyOf(snapshotIdList).size() != snapshotIdList.size()) :
                "Found duplicate snapshot ids per index in [" + indexSnapshots + "]";
    }

    protected RepositoryData copy() {
        return new RepositoryData(
            genId, snapshotIds, snapshotStates, snapshotVersions, indexSnapshots, shardGenerations, indexMetaDataGenerations);
    }

    /**
     * Creates a copy of this instance that contains updated version data.
     * @param versions map of snapshot versions
     * @return copy with updated version data
     */
    public RepositoryData withVersions(Map<SnapshotId, Version> versions) {
        if (versions.isEmpty()) {
            return this;
        }
        final Map<String, Version> newVersions = new HashMap<>(snapshotVersions);
        versions.forEach((id, version) -> newVersions.put(id.getUUID(), version));
        return new RepositoryData(
            genId, snapshotIds, snapshotStates, newVersions, indexSnapshots, shardGenerations, indexMetaDataGenerations);
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
     * Returns the {@link Version} for the given snapshot or {@code null} if unknown.
     */
    @Nullable
    public Version getVersion(SnapshotId snapshotId) {
        return snapshotVersions.get(snapshotId.getUUID());
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
     * @param snapshotIds SnapshotId to remove
     * @return List of indices that are changed but not removed
     */
    public List<IndexId> indicesToUpdateAfterRemovingSnapshot(Collection<SnapshotId> snapshotIds) {
        return indexSnapshots.entrySet().stream()
            .filter(entry -> {
                final Collection<SnapshotId> existingIds = entry.getValue();
                if (snapshotIds.containsAll(existingIds)) {
                    return existingIds.size() > snapshotIds.size();
                }
                for (SnapshotId snapshotId : snapshotIds) {
                    if (entry.getValue().contains(snapshotId)) {
                        return true;
                    }
                }
                return false;
            }).map(Map.Entry::getKey).collect(Collectors.toList());
    }

    /**
     * Returns a map of {@link IndexId} to a collection of {@link String} containing all the {@link IndexId} and the
     * {@link org.elasticsearch.cluster.metadata.IndexMetadata} blob name in it that can be removed after removing the given snapshot from
     * the repository.
     * NOTE: Does not return a mapping for {@link IndexId} values that will be removed completely from the repository.
     *
     * @param snapshotIds SnapshotIds to remove
     * @return map of index to index metadata blob id to delete
     */
    public Map<IndexId, Collection<String>> indexMetaDataToRemoveAfterRemovingSnapshots(Collection<SnapshotId> snapshotIds) {
        Collection<IndexId> indicesForSnapshot = indicesToUpdateAfterRemovingSnapshot(snapshotIds);
        final Set<String> allRemainingIdentifiers = indexMetaDataGenerations.lookup.entrySet().stream()
                .filter(e -> snapshotIds.contains(e.getKey()) == false).flatMap(e -> e.getValue().values().stream())
                .map(indexMetaDataGenerations::getIndexMetaBlobId).collect(Collectors.toSet());
        final Map<IndexId, Collection<String>> toRemove = new HashMap<>();
        for (IndexId indexId : indicesForSnapshot) {
            for (SnapshotId snapshotId : snapshotIds) {
                final String identifier = indexMetaDataGenerations.indexMetaBlobId(snapshotId, indexId);
                if (allRemainingIdentifiers.contains(identifier) == false) {
                    toRemove.computeIfAbsent(indexId, k -> new HashSet<>()).add(identifier);
                }
            }
        }
        return toRemove;
    }

    /**
     * Add a snapshot and its indices to the repository; returns a new instance.  If the snapshot
     * already exists in the repository data, this method throws an IllegalArgumentException.
     *
     * @param snapshotId       Id of the new snapshot
     * @param snapshotState    State of the new snapshot
     * @param shardGenerations Updated shard generations in the new snapshot. For each index contained in the snapshot an array of new
     *                         generations indexed by the shard id they correspond to must be supplied.
     * @param indexMetaBlobs   Map of index metadata blob uuids
     * @param newIdentifiers   Map of new index metadata blob uuids keyed by the identifiers of the
     *                         {@link org.elasticsearch.cluster.metadata.IndexMetadata} in them
     */
    public RepositoryData addSnapshot(final SnapshotId snapshotId,
                                      final SnapshotState snapshotState,
                                      final Version version,
                                      final ShardGenerations shardGenerations,
                                      @Nullable final Map<IndexId, String> indexMetaBlobs,
                                      @Nullable final Map<String, String> newIdentifiers) {
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
        Map<String, Version> newSnapshotVersions = new HashMap<>(snapshotVersions);
        newSnapshotVersions.put(snapshotId.getUUID(), version);
        Map<IndexId, List<SnapshotId>> allIndexSnapshots = new HashMap<>(indexSnapshots);
        for (final IndexId indexId : shardGenerations.indices()) {
            final List<SnapshotId> snapshotIds = allIndexSnapshots.get(indexId);
            if (snapshotIds == null) {
                allIndexSnapshots.put(indexId, List.of(snapshotId));
            } else {
                final List<SnapshotId> copy = new ArrayList<>(snapshotIds.size() + 1);
                copy.addAll(snapshotIds);
                copy.add(snapshotId);
                allIndexSnapshots.put(indexId, Collections.unmodifiableList(copy));
            }
        }

        final IndexMetaDataGenerations newIndexMetaGenerations;
        if (indexMetaBlobs == null) {
            assert newIdentifiers == null : "Non-null new identifiers [" + newIdentifiers + "] for null lookup";
            assert indexMetaDataGenerations.lookup.isEmpty() :
                "Index meta generations should have been empty but was [" + indexMetaDataGenerations + "]";
            newIndexMetaGenerations = IndexMetaDataGenerations.EMPTY;
        } else {
            assert indexMetaBlobs.isEmpty() || shardGenerations.indices().equals(indexMetaBlobs.keySet()) :
                "Shard generations contained indices " + shardGenerations.indices()
                    + " but indexMetaData was given for " + indexMetaBlobs.keySet();
            newIndexMetaGenerations = indexMetaDataGenerations.withAddedSnapshot(snapshotId, indexMetaBlobs, newIdentifiers);
        }

        return new RepositoryData(genId, snapshots, newSnapshotStates, newSnapshotVersions, allIndexSnapshots,
            ShardGenerations.builder().putAll(this.shardGenerations).putAll(shardGenerations).build(),
            newIndexMetaGenerations);
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
        return new RepositoryData(newGeneration, snapshotIds, snapshotStates, snapshotVersions, indices, indexSnapshots, shardGenerations,
                indexMetaDataGenerations);
    }

    /**
     * Remove snapshots and remove any indices that no longer exist in the repository due to the deletion of the snapshots.
     *
     * @param snapshots               Snapshot ids to remove
     * @param updatedShardGenerations Shard generations that changed as a result of removing the snapshot.
     *                                The {@code String[]} passed for each {@link IndexId} contains the new shard generation id for each
     *                                changed shard indexed by its shardId
     */
    public RepositoryData removeSnapshots(final Collection<SnapshotId> snapshots, final ShardGenerations updatedShardGenerations) {
        Map<String, SnapshotId> newSnapshotIds = snapshotIds.values().stream().filter(Predicate.not(snapshots::contains))
            .collect(Collectors.toMap(SnapshotId::getUUID, Function.identity()));
        if (newSnapshotIds.size() != snapshotIds.size() - snapshots.size()) {
            final Collection<SnapshotId> notFound = new HashSet<>(snapshots);
            notFound.removeAll(snapshotIds.values());
            throw new ResourceNotFoundException("Attempting to remove non-existent snapshots {} from repository data", notFound);
        }
        Map<String, SnapshotState> newSnapshotStates = new HashMap<>(snapshotStates);
        final Map<String, Version> newSnapshotVersions = new HashMap<>(snapshotVersions);
        for (SnapshotId snapshotId : snapshots) {
            newSnapshotStates.remove(snapshotId.getUUID());
            newSnapshotVersions.remove(snapshotId.getUUID());
        }
        Map<IndexId, List<SnapshotId>> indexSnapshots = new HashMap<>();
        for (final IndexId indexId : indices.values()) {
            List<SnapshotId> snapshotIds = this.indexSnapshots.get(indexId);
            assert snapshotIds != null;
            List<SnapshotId> remaining = new ArrayList<>(snapshotIds);
            if (remaining.removeAll(snapshots)) {
                remaining = Collections.unmodifiableList(remaining);
            } else {
                remaining = snapshotIds;
            }
            if (remaining.isEmpty() == false) {
                indexSnapshots.put(indexId, remaining);
            }
        }

        return new RepositoryData(genId, newSnapshotIds, newSnapshotStates, newSnapshotVersions, indexSnapshots,
            ShardGenerations.builder().putAll(shardGenerations).putAll(updatedShardGenerations)
                .retainIndicesAndPruneDeletes(indexSnapshots.keySet()).build(),
            indexMetaDataGenerations.withRemovedSnapshots(snapshots)
        );
    }

    /**
     * Returns an immutable collection of the snapshot ids for the snapshots that contain the given index.
     */
    public List<SnapshotId> getSnapshots(final IndexId indexId) {
        List<SnapshotId> snapshotIds = indexSnapshots.get(indexId);
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
                   && snapshotVersions.equals(that.snapshotVersions)
                   && indices.equals(that.indices)
                   && indexSnapshots.equals(that.indexSnapshots)
                   && shardGenerations.equals(that.shardGenerations)
                   && indexMetaDataGenerations.equals(that.indexMetaDataGenerations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            snapshotIds, snapshotStates, snapshotVersions, indices, indexSnapshots, shardGenerations, indexMetaDataGenerations);
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
     *
     * @param indicesToResolve names of indices to resolve
     * @param inFlightIds      name to index mapping for currently in-flight snapshots not yet in the repository data to fall back to
     */
    public List<IndexId> resolveNewIndices(List<String> indicesToResolve, Map<String, IndexId> inFlightIds) {
        List<IndexId> snapshotIndices = new ArrayList<>();
        for (String index : indicesToResolve) {
            IndexId indexId = indices.get(index);
            if (indexId == null) {
                indexId = inFlightIds.get(index);
            }
            if (indexId == null) {
                indexId = new IndexId(index, UUIDs.randomBase64UUID());
            }
            snapshotIndices.add(indexId);
        }
        return snapshotIndices;
    }

    private static final String SHARD_GENERATIONS = "shard_generations";
    private static final String INDEX_METADATA_IDENTIFIERS = "index_metadata_identifiers";
    private static final String INDEX_METADATA_LOOKUP = "index_metadata_lookup";
    private static final String SNAPSHOTS = "snapshots";
    private static final String INDICES = "indices";
    private static final String INDEX_ID = "id";
    private static final String NAME = "name";
    private static final String UUID = "uuid";
    private static final String STATE = "state";
    private static final String VERSION = "version";
    private static final String MIN_VERSION = "min_version";

    /**
     * Writes the snapshots metadata and the related indices metadata to x-content.
     */
    public XContentBuilder snapshotsToXContent(final XContentBuilder builder, final Version repoMetaVersion) throws IOException {
        builder.startObject();
        // write the snapshots list
        builder.startArray(SNAPSHOTS);
        final boolean shouldWriteIndexGens = SnapshotsService.useIndexGenerations(repoMetaVersion);
        final boolean shouldWriteShardGens = SnapshotsService.useShardGenerations(repoMetaVersion);
        for (final SnapshotId snapshot : getSnapshotIds()) {
            builder.startObject();
            builder.field(NAME, snapshot.getName());
            builder.field(UUID, snapshot.getUUID());
            if (snapshotStates.containsKey(snapshot.getUUID())) {
                builder.field(STATE, snapshotStates.get(snapshot.getUUID()).value());
            }
            if (shouldWriteIndexGens) {
                builder.field(INDEX_METADATA_LOOKUP, indexMetaDataGenerations.lookup.getOrDefault(snapshot, Collections.emptyMap())
                    .entrySet().stream().collect(Collectors.toMap(entry -> entry.getKey().getId(), Map.Entry::getValue)));
            }
            if (snapshotVersions.containsKey(snapshot.getUUID())) {
                builder.field(VERSION, snapshotVersions.get(snapshot.getUUID()).toString());
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
            List<SnapshotId> snapshotIds = indexSnapshots.get(indexId);
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
        if (shouldWriteIndexGens) {
            builder.field(MIN_VERSION, SnapshotsService.INDEX_GEN_IN_REPO_DATA_VERSION.toString());
            builder.field(INDEX_METADATA_IDENTIFIERS, indexMetaDataGenerations.identifiers);
        } else if (shouldWriteShardGens) {
            // Add min version field to make it impossible for older ES versions to deserialize this object
            builder.field(MIN_VERSION, SnapshotsService.SHARD_GEN_IN_REPO_DATA_VERSION.toString());
        }
        builder.endObject();
        return builder;
    }

    public IndexMetaDataGenerations indexMetaDataGenerations() {
        return indexMetaDataGenerations;
    }

    /**
     * Reads an instance of {@link RepositoryData} from x-content, loading the snapshots and indices metadata.
     *
     * @param fixBrokenShardGens set to {@code true} to filter out broken shard generations read from the {@code parser} via
     *                           {@link ShardGenerations#fixShardGeneration}. Used to disable fixing broken generations when reading
     *                           from cached bytes that we trust to not contain broken generations.
     */
    public static RepositoryData snapshotsFromXContent(XContentParser parser, long genId, boolean fixBrokenShardGens) throws IOException {
        final Map<String, SnapshotId> snapshots = new HashMap<>();
        final Map<String, SnapshotState> snapshotStates = new HashMap<>();
        final Map<String, Version> snapshotVersions = new HashMap<>();
        final Map<IndexId, List<SnapshotId>> indexSnapshots = new HashMap<>();
        final ShardGenerations.Builder shardGenerations = ShardGenerations.builder();
        final Map<String, String> indexMetaIdentifiers = new HashMap<>();
        final Map<SnapshotId, Map<String, String>> indexMetaLookup = new HashMap<>();

        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                String field = parser.currentName();
                if (SNAPSHOTS.equals(field)) {
                    if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            String name = null;
                            String uuid = null;
                            SnapshotState state = null;
                            Map<String, String> metaGenerations = new HashMap<>();
                            Version version = null;
                            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                                String currentFieldName = parser.currentName();
                                parser.nextToken();
                                if (NAME.equals(currentFieldName)) {
                                    name = parser.text();
                                } else if (UUID.equals(currentFieldName)) {
                                    uuid = parser.text();
                                } else if (STATE.equals(currentFieldName)) {
                                    state = SnapshotState.fromValue(parser.numberValue().byteValue());
                                } else if (INDEX_METADATA_LOOKUP.equals(currentFieldName)) {
                                    metaGenerations.putAll(parser.mapStrings());
                                } else if (VERSION.equals(currentFieldName)) {
                                    version = Version.fromString(parser.text());
                                }
                            }
                            final SnapshotId snapshotId = new SnapshotId(name, uuid);
                            if (state != null) {
                                snapshotStates.put(uuid, state);
                            }
                            if (version != null) {
                                snapshotVersions.put(uuid, version);
                            }
                            snapshots.put(snapshotId.getUUID(), snapshotId);
                            if (metaGenerations.isEmpty() == false) {
                                indexMetaLookup.put(snapshotId, metaGenerations);
                            }
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
                        final List<SnapshotId> snapshotIds = new ArrayList<>();
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
                        indexSnapshots.put(indexId, Collections.unmodifiableList(snapshotIds));
                        for (int i = 0; i < gens.size(); i++) {
                            String parsedGen = gens.get(i);
                            if (fixBrokenShardGens) {
                                parsedGen = ShardGenerations.fixShardGeneration(parsedGen);
                            }
                            if (parsedGen != null) {
                                shardGenerations.put(indexId, i, parsedGen);
                            }
                        }
                    }
                } else if (INDEX_METADATA_IDENTIFIERS.equals(field)) {
                    if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                        throw new ElasticsearchParseException("start object expected [" + INDEX_METADATA_IDENTIFIERS + "]");
                    }
                    indexMetaIdentifiers.putAll(parser.mapStrings());
                } else if (MIN_VERSION.equals(field)) {
                    if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
                        throw new ElasticsearchParseException("version string expected [min_version]");
                    }
                    final Version version = Version.fromString(parser.text());
                    assert SnapshotsService.useShardGenerations(version);
                } else {
                    throw new ElasticsearchParseException("unknown field name  [" + field + "]");
                }
            }
        } else {
            throw new ElasticsearchParseException("start object expected");
        }
        final Map<String, IndexId> indexLookup =
            indexSnapshots.keySet().stream().collect(Collectors.toMap(IndexId::getId, Function.identity()));
        return new RepositoryData(genId, snapshots, snapshotStates, snapshotVersions, indexSnapshots, shardGenerations.build(),
            new IndexMetaDataGenerations(indexMetaLookup.entrySet().stream().collect(
                Collectors.toMap(Map.Entry::getKey, e -> e.getValue().entrySet().stream()
                    .collect(Collectors.toMap(entry -> indexLookup.get(entry.getKey()), Map.Entry::getValue)))), indexMetaIdentifiers));
    }

}
