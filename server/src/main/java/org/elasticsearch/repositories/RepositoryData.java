/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
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
     * Sentinel value for the repository UUID indicating that it is not set.
     */
    public static final String MISSING_UUID = "_na_";

    /**
     * An instance initialized for an empty repository.
     */
    public static final RepositoryData EMPTY = new RepositoryData(
        MISSING_UUID,
        EMPTY_REPO_GEN,
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        ShardGenerations.EMPTY,
        IndexMetaDataGenerations.EMPTY,
        MISSING_UUID
    );

    /**
     * A UUID that identifies this repository.
     */
    private final String uuid;

    /**
     * Cluster UUID as returned by {@link Metadata#clusterUUID()} of the cluster that wrote this instance to the repository.
     */
    private final String clusterUUID;

    /**
     * The generational id of the index file from which the repository data was read.
     */
    private final long genId;

    /**
     * The ids of the snapshots in the repository.
     */
    private final Map<String, SnapshotId> snapshotIds;

    /**
     * The details of each snapshot in the repository.
     */
    private final Map<String, SnapshotDetails> snapshotsDetails;

    /**
     * The indices found in the repository across all snapshots, as a name to {@link IndexId} mapping
     */
    private final Map<String, IndexId> indices;

    /**
     * The snapshots that each index belongs to.
     */
    private final Map<IndexId, List<SnapshotId>> indexSnapshots;

    /**
     * Index metadata generations.
     */
    private final IndexMetaDataGenerations indexMetaDataGenerations;

    /**
     * Shard generations.
     */
    private final ShardGenerations shardGenerations;

    public RepositoryData(
        String uuid,
        long genId,
        Map<String, SnapshotId> snapshotIds,
        Map<String, SnapshotDetails> snapshotsDetails,
        Map<IndexId, List<SnapshotId>> indexSnapshots,
        ShardGenerations shardGenerations,
        IndexMetaDataGenerations indexMetaDataGenerations,
        String clusterUUID
    ) {
        this(
            uuid,
            genId,
            Collections.unmodifiableMap(snapshotIds),
            Collections.unmodifiableMap(snapshotsDetails),
            indexSnapshots.keySet().stream().collect(Collectors.toUnmodifiableMap(IndexId::getName, Function.identity())),
            Collections.unmodifiableMap(indexSnapshots),
            shardGenerations,
            indexMetaDataGenerations,
            clusterUUID
        );
    }

    private RepositoryData(
        String uuid,
        long genId,
        Map<String, SnapshotId> snapshotIds,
        Map<String, SnapshotDetails> snapshotsDetails,
        Map<String, IndexId> indices,
        Map<IndexId, List<SnapshotId>> indexSnapshots,
        ShardGenerations shardGenerations,
        IndexMetaDataGenerations indexMetaDataGenerations,
        String clusterUUID
    ) {
        this.uuid = Objects.requireNonNull(uuid);
        this.genId = genId;
        this.snapshotIds = snapshotIds;
        this.snapshotsDetails = snapshotsDetails;
        this.indices = indices;
        this.indexSnapshots = indexSnapshots;
        this.shardGenerations = shardGenerations;
        this.indexMetaDataGenerations = indexMetaDataGenerations;
        this.clusterUUID = Objects.requireNonNull(clusterUUID);
        assert uuid.equals(MISSING_UUID) == clusterUUID.equals(MISSING_UUID)
            : "Either repository- and cluster UUID must both be missing"
                + " or neither of them must be missing but saw ["
                + uuid
                + "]["
                + clusterUUID
                + "]";
        assert indices.values().containsAll(shardGenerations.indices())
            : "ShardGenerations contained indices "
                + shardGenerations.indices()
                + " but snapshots only reference indices "
                + indices.values();
        assert indexSnapshots.values().stream().noneMatch(snapshotIdList -> Set.copyOf(snapshotIdList).size() != snapshotIdList.size())
            : "Found duplicate snapshot ids per index in [" + indexSnapshots + "]";
    }

    protected RepositoryData copy() {
        return new RepositoryData(
            uuid,
            genId,
            snapshotIds,
            snapshotsDetails,
            indices,
            indexSnapshots,
            shardGenerations,
            indexMetaDataGenerations,
            clusterUUID
        );
    }

    /**
     * Creates a copy of this instance that does not track any shard generations.
     *
     * @return repository data with empty shard generations
     */
    public RepositoryData withoutShardGenerations() {
        return new RepositoryData(
            uuid,
            genId,
            snapshotIds,
            snapshotsDetails,
            indices,
            indexSnapshots,
            ShardGenerations.EMPTY,
            indexMetaDataGenerations,
            clusterUUID
        );
    }

    /**
     * Creates a copy of this instance that contains additional details read from the per-snapshot metadata blobs
     * @param extraDetails map of snapshot details
     * @return copy with updated version data
     */
    public RepositoryData withExtraDetails(Map<SnapshotId, SnapshotDetails> extraDetails) {
        if (extraDetails.isEmpty()) {
            return this;
        }
        final Map<String, SnapshotDetails> newDetails = new HashMap<>(snapshotsDetails);
        extraDetails.forEach((id, extraDetail) -> newDetails.put(id.getUUID(), extraDetail));
        return new RepositoryData(
            uuid,
            genId,
            snapshotIds,
            newDetails,
            indices,
            indexSnapshots,
            shardGenerations,
            indexMetaDataGenerations,
            clusterUUID
        );
    }

    public ShardGenerations shardGenerations() {
        return shardGenerations;
    }

    /**
     * @return The UUID of this repository, or {@link RepositoryData#MISSING_UUID} if this repository has no UUID because it still
     * supports access from versions earlier than {@link SnapshotsService#UUIDS_IN_REPO_DATA_VERSION}.
     */
    public String getUuid() {
        return uuid;
    }

    /**
     * @return the cluster UUID of the cluster that wrote this instance to the repository or {@link RepositoryData#MISSING_UUID} if this
     * instance was written by a cluster older than {@link SnapshotsService#UUIDS_IN_REPO_DATA_VERSION}.
     */
    public String getClusterUUID() {
        return clusterUUID;
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
        return snapshotIds.values();
    }

    /**
     * @return whether some of the {@link SnapshotDetails} of the given snapshot are missing, due to BwC, so that they must be loaded from
     * the {@link SnapshotInfo} blob instead.
     */
    public boolean hasMissingDetails(SnapshotId snapshotId) {
        final SnapshotDetails snapshotDetails = getSnapshotDetails(snapshotId);
        return snapshotDetails == null
            || snapshotDetails.getVersion() == null
            || snapshotDetails.getStartTimeMillis() == -1
            || snapshotDetails.getEndTimeMillis() == -1;
    }

    /**
     * Returns the {@link SnapshotDetails} for the given snapshot. Returns {@code null} if there are no details for the snapshot.
     */
    @Nullable
    public SnapshotDetails getSnapshotDetails(final SnapshotId snapshotId) {
        return snapshotsDetails.get(snapshotId.getUUID());
    }

    /**
     * Returns the {@link SnapshotState} for the given snapshot.  Returns {@code null} if
     * there is no state for the snapshot.
     */
    @Nullable
    public SnapshotState getSnapshotState(final SnapshotId snapshotId) {
        return snapshotsDetails.getOrDefault(snapshotId.getUUID(), SnapshotDetails.EMPTY).getSnapshotState();
    }

    /**
     * Returns the {@link Version} for the given snapshot or {@code null} if unknown.
     */
    @Nullable
    public Version getVersion(SnapshotId snapshotId) {
        return snapshotsDetails.getOrDefault(snapshotId.getUUID(), SnapshotDetails.EMPTY).getVersion();
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
        return indexSnapshots.entrySet().stream().filter(entry -> {
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
     * {@link IndexMetadata} blob name in it that can be removed after removing the given snapshot from the repository.
     * NOTE: Does not return a mapping for {@link IndexId} values that will be removed completely from the repository.
     *
     * @param snapshotIds SnapshotIds to remove
     * @return map of index to index metadata blob id to delete
     */
    public Map<IndexId, Collection<String>> indexMetaDataToRemoveAfterRemovingSnapshots(Collection<SnapshotId> snapshotIds) {
        Collection<IndexId> indicesForSnapshot = indicesToUpdateAfterRemovingSnapshot(snapshotIds);
        final Set<String> allRemainingIdentifiers = indexMetaDataGenerations.lookup.entrySet()
            .stream()
            .filter(e -> snapshotIds.contains(e.getKey()) == false)
            .flatMap(e -> e.getValue().values().stream())
            .map(indexMetaDataGenerations::getIndexMetaBlobId)
            .collect(Collectors.toSet());
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
     * @param details          Details of the new snapshot
     * @param shardGenerations Updated shard generations in the new snapshot. For each index contained in the snapshot an array of new
     *                         generations indexed by the shard id they correspond to must be supplied.
     * @param indexMetaBlobs   Map of index metadata blob uuids
     * @param newIdentifiers   Map of new index metadata blob uuids keyed by the identifiers of the
     *                         {@link IndexMetadata} in them
     */
    public RepositoryData addSnapshot(
        final SnapshotId snapshotId,
        final SnapshotDetails details,
        final ShardGenerations shardGenerations,
        @Nullable final Map<IndexId, String> indexMetaBlobs,
        @Nullable final Map<String, String> newIdentifiers
    ) {
        if (snapshotIds.containsKey(snapshotId.getUUID())) {
            // if the snapshot id already exists in the repository data, it means an old master
            // that is blocked from the cluster is trying to finalize a snapshot concurrently with
            // the new master, so we make the operation idempotent
            return this;
        }
        Map<String, SnapshotId> snapshots = new HashMap<>(snapshotIds);
        snapshots.put(snapshotId.getUUID(), snapshotId);
        Map<String, SnapshotDetails> newSnapshotDetails = new HashMap<>(snapshotsDetails);
        newSnapshotDetails.put(snapshotId.getUUID(), details);
        Map<IndexId, List<SnapshotId>> allIndexSnapshots = new HashMap<>(indexSnapshots);
        for (final IndexId indexId : shardGenerations.indices()) {
            final List<SnapshotId> snapshotIds = allIndexSnapshots.get(indexId);
            if (snapshotIds == null) {
                allIndexSnapshots.put(indexId, List.of(snapshotId));
            } else {
                allIndexSnapshots.put(indexId, CollectionUtils.appendToCopy(snapshotIds, snapshotId));
            }
        }

        final IndexMetaDataGenerations newIndexMetaGenerations;
        if (indexMetaBlobs == null) {
            assert newIdentifiers == null : "Non-null new identifiers [" + newIdentifiers + "] for null lookup";
            assert indexMetaDataGenerations.lookup.isEmpty()
                : "Index meta generations should have been empty but was [" + indexMetaDataGenerations + "]";
            newIndexMetaGenerations = IndexMetaDataGenerations.EMPTY;
        } else {
            assert indexMetaBlobs.isEmpty() || shardGenerations.indices().equals(indexMetaBlobs.keySet())
                : "Shard generations contained indices "
                    + shardGenerations.indices()
                    + " but indexMetaData was given for "
                    + indexMetaBlobs.keySet();
            newIndexMetaGenerations = indexMetaDataGenerations.withAddedSnapshot(snapshotId, indexMetaBlobs, newIdentifiers);
        }

        return new RepositoryData(
            uuid,
            genId,
            snapshots,
            newSnapshotDetails,
            allIndexSnapshots,
            ShardGenerations.builder().putAll(this.shardGenerations).putAll(shardGenerations).build(),
            newIndexMetaGenerations,
            clusterUUID
        );
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
        return new RepositoryData(
            uuid,
            newGeneration,
            snapshotIds,
            snapshotsDetails,
            indices,
            indexSnapshots,
            shardGenerations,
            indexMetaDataGenerations,
            clusterUUID
        );
    }

    /**
     * For test purposes, make a copy of this instance with the cluster- and repository UUIDs removed and all other fields unchanged,
     * as if from an older version.
     */
    public RepositoryData withoutUUIDs() {
        return new RepositoryData(
            MISSING_UUID,
            genId,
            snapshotIds,
            snapshotsDetails,
            indices,
            indexSnapshots,
            shardGenerations,
            indexMetaDataGenerations,
            MISSING_UUID
        );
    }

    public RepositoryData withClusterUuid(String clusterUUID) {
        assert clusterUUID.equals(MISSING_UUID) == false;
        return new RepositoryData(
            uuid.equals(MISSING_UUID) ? UUIDs.randomBase64UUID() : uuid,
            genId,
            snapshotIds,
            snapshotsDetails,
            indices,
            indexSnapshots,
            shardGenerations,
            indexMetaDataGenerations,
            clusterUUID
        );
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
        Map<String, SnapshotId> newSnapshotIds = snapshotIds.values()
            .stream()
            .filter(Predicate.not(snapshots::contains))
            .collect(Collectors.toMap(SnapshotId::getUUID, Function.identity()));
        if (newSnapshotIds.size() != snapshotIds.size() - snapshots.size()) {
            final Collection<SnapshotId> notFound = new HashSet<>(snapshots);
            notFound.removeAll(snapshotIds.values());
            throw new ResourceNotFoundException("Attempting to remove non-existent snapshots {} from repository data", notFound);
        }
        final Map<String, SnapshotDetails> newSnapshotsDetails = new HashMap<>(snapshotsDetails);
        for (SnapshotId snapshotId : snapshots) {
            newSnapshotsDetails.remove(snapshotId.getUUID());
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

        return new RepositoryData(
            uuid,
            genId,
            newSnapshotIds,
            newSnapshotsDetails,
            indexSnapshots,
            ShardGenerations.builder()
                .putAll(shardGenerations)
                .putAll(updatedShardGenerations)
                .retainIndicesAndPruneDeletes(indexSnapshots.keySet())
                .build(),
            indexMetaDataGenerations.withRemovedSnapshots(snapshots),
            clusterUUID
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
            && snapshotsDetails.equals(that.snapshotsDetails)
            && indices.equals(that.indices)
            && indexSnapshots.equals(that.indexSnapshots)
            && shardGenerations.equals(that.shardGenerations)
            && indexMetaDataGenerations.equals(that.indexMetaDataGenerations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotIds, snapshotsDetails, indices, indexSnapshots, shardGenerations, indexMetaDataGenerations);
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
    public Map<String, IndexId> resolveIndices(final List<String> indices) {
        Map<String, IndexId> resolvedIndices = new HashMap<>(indices.size());
        for (final String indexName : indices) {
            final IndexId indexId = resolveIndexId(indexName);
            resolvedIndices.put(indexId.getName(), indexId);
        }
        return Collections.unmodifiableMap(resolvedIndices);
    }

    /**
     * Resolve the given index names to index ids, creating new index ids for
     * new indices in the repository.
     *
     * @param indicesToResolve names of indices to resolve
     * @param inFlightIds      name to index mapping for currently in-flight snapshots not yet in the repository data to fall back to
     */
    public Map<String, IndexId> resolveNewIndices(List<String> indicesToResolve, Map<String, IndexId> inFlightIds) {
        Map<String, IndexId> snapshotIndices = new HashMap<>(indicesToResolve.size());
        for (String index : indicesToResolve) {
            IndexId indexId = indices.get(index);
            if (indexId == null) {
                indexId = inFlightIds.get(index);
            }
            if (indexId == null) {
                indexId = new IndexId(index, UUIDs.randomBase64UUID());
            }
            snapshotIndices.put(indexId.getName(), indexId);
        }
        return Collections.unmodifiableMap(snapshotIndices);
    }

    private static final String SHARD_GENERATIONS = "shard_generations";
    private static final String INDEX_METADATA_IDENTIFIERS = "index_metadata_identifiers";
    private static final String INDEX_METADATA_LOOKUP = "index_metadata_lookup";
    private static final String SNAPSHOTS = "snapshots";
    private static final String INDICES = "indices";
    private static final String INDEX_ID = "id";
    private static final String NAME = "name";
    private static final String UUID = "uuid";
    private static final String CLUSTER_UUID = "cluster_id";
    private static final String STATE = "state";
    private static final String VERSION = "version";
    private static final String MIN_VERSION = "min_version";
    private static final String START_TIME_MILLIS = "start_time_millis";
    private static final String END_TIME_MILLIS = "end_time_millis";

    /**
     * Writes the snapshots metadata and the related indices metadata to x-content.
     */
    public XContentBuilder snapshotsToXContent(final XContentBuilder builder, final Version repoMetaVersion) throws IOException {
        return snapshotsToXContent(builder, repoMetaVersion, false);
    }

    /**
     * Writes the snapshots metadata and the related indices metadata to x-content.
     * @param permitMissingUuid indicates whether we permit the repository- and cluster UUIDs to be missing,
     *                          e.g. we are serializing for the in-memory cache or running tests
     */
    public XContentBuilder snapshotsToXContent(final XContentBuilder builder, final Version repoMetaVersion, boolean permitMissingUuid)
        throws IOException {

        final boolean shouldWriteUUIDS = SnapshotsService.includesUUIDs(repoMetaVersion);
        final boolean shouldWriteIndexGens = SnapshotsService.useIndexGenerations(repoMetaVersion);
        final boolean shouldWriteShardGens = SnapshotsService.useShardGenerations(repoMetaVersion);

        assert Boolean.compare(shouldWriteUUIDS, shouldWriteIndexGens) <= 0;
        assert Boolean.compare(shouldWriteIndexGens, shouldWriteShardGens) <= 0;

        builder.startObject();

        if (shouldWriteShardGens) {
            // Add min version field to make it impossible for older ES versions to deserialize this object
            final Version minVersion;
            if (shouldWriteUUIDS) {
                minVersion = SnapshotsService.UUIDS_IN_REPO_DATA_VERSION;
            } else if (shouldWriteIndexGens) {
                minVersion = SnapshotsService.INDEX_GEN_IN_REPO_DATA_VERSION;
            } else {
                minVersion = SnapshotsService.SHARD_GEN_IN_REPO_DATA_VERSION;
            }
            builder.field(MIN_VERSION, minVersion.toString());
        }

        if (shouldWriteUUIDS) {
            if (uuid.equals(MISSING_UUID)) {
                assert permitMissingUuid : "missing uuid";
            } else {
                builder.field(UUID, uuid);
            }
            if (clusterUUID.equals(MISSING_UUID)) {
                assert permitMissingUuid : "missing clusterUUID";
            } else {
                builder.field(CLUSTER_UUID, clusterUUID);
            }
        } else {
            assert uuid.equals(MISSING_UUID) : "lost uuid " + uuid;
            assert clusterUUID.equals(MISSING_UUID) : "lost clusterUUID " + clusterUUID;
        }

        // write the snapshots list

        builder.startArray(SNAPSHOTS);
        for (final SnapshotId snapshot : getSnapshotIds()) {
            builder.startObject();
            builder.field(NAME, snapshot.getName());
            final String snapshotUUID = snapshot.getUUID();
            builder.field(UUID, snapshotUUID);
            final SnapshotDetails snapshotDetails = snapshotsDetails.getOrDefault(snapshotUUID, SnapshotDetails.EMPTY);
            final SnapshotState state = snapshotDetails.getSnapshotState();
            if (state != null) {
                builder.field(STATE, state.value());
            }
            if (shouldWriteIndexGens) {
                builder.startObject(INDEX_METADATA_LOOKUP);
                for (Map.Entry<IndexId, String> entry : indexMetaDataGenerations.lookup.getOrDefault(snapshot, Collections.emptyMap())
                    .entrySet()) {
                    builder.field(entry.getKey().getId(), entry.getValue());
                }
                builder.endObject();
            }
            final Version version = snapshotDetails.getVersion();
            if (version != null) {
                builder.field(VERSION, version.toString());
            }

            if (snapshotDetails.getStartTimeMillis() != -1) {
                builder.field(START_TIME_MILLIS, snapshotDetails.getStartTimeMillis());
            }
            if (snapshotDetails.getEndTimeMillis() != -1) {
                builder.field(END_TIME_MILLIS, snapshotDetails.getEndTimeMillis());
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
            builder.field(INDEX_METADATA_IDENTIFIERS, indexMetaDataGenerations.identifiers);
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
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        final Map<String, SnapshotId> snapshots = new HashMap<>();
        final Map<String, SnapshotDetails> snapshotsDetails = new HashMap<>();
        final Map<IndexId, List<SnapshotId>> indexSnapshots = new HashMap<>();
        final Map<String, IndexId> indexLookup = new HashMap<>();
        final ShardGenerations.Builder shardGenerations = ShardGenerations.builder();
        final Map<SnapshotId, Map<String, String>> indexMetaLookup = new HashMap<>();
        Map<String, String> indexMetaIdentifiers = null;
        String uuid = MISSING_UUID;
        String clusterUUID = MISSING_UUID;
        while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
            final String field = parser.currentName();
            switch (field) {
                case SNAPSHOTS:
                    parseSnapshots(parser, snapshots, snapshotsDetails, indexMetaLookup);
                    break;
                case INDICES:
                    parseIndices(parser, fixBrokenShardGens, snapshots, indexSnapshots, indexLookup, shardGenerations);
                    break;
                case INDEX_METADATA_IDENTIFIERS:
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    indexMetaIdentifiers = parser.mapStrings();
                    break;
                case MIN_VERSION:
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.nextToken(), parser);
                    final Version version = Version.fromString(parser.text());
                    assert SnapshotsService.useShardGenerations(version);
                    if (version.after(Version.CURRENT)) {
                        throw new IllegalStateException(
                            "this snapshot repository format requires Elasticsearch version [" + version + "] or later"
                        );
                    }
                    break;
                case UUID:
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.nextToken(), parser);
                    uuid = parser.text();
                    assert uuid.equals(MISSING_UUID) == false;
                    break;
                case CLUSTER_UUID:
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.nextToken(), parser);
                    clusterUUID = parser.text();
                    assert clusterUUID.equals(MISSING_UUID) == false;
                    break;
                default:
                    XContentParserUtils.throwUnknownField(field, parser.getTokenLocation());
            }
        }

        // ensure we drained the stream completely
        XContentParserUtils.ensureExpectedToken(null, parser.nextToken(), parser);

        return new RepositoryData(
            uuid,
            genId,
            snapshots,
            snapshotsDetails,
            indexSnapshots,
            shardGenerations.build(),
            buildIndexMetaGenerations(indexMetaLookup, indexLookup, indexMetaIdentifiers),
            clusterUUID
        );
    }

    /**
     * Builds {@link IndexMetaDataGenerations} instance from the information parsed previously.
     *
     * @param indexMetaLookup      map of {@link SnapshotId} to map of index id (as returned by {@link IndexId#getId}) that defines the
     *                             index metadata generations for the snapshot that was parsed by {@link #parseSnapshots}
     * @param indexLookup          map of index uuid (as returned by {@link IndexId#getId}) to {@link IndexId} that was parsed by
     *                             {@link #parseIndices}
     * @param indexMetaIdentifiers map of index generation to index meta identifiers parsed by {@link #snapshotsFromXContent}
     * @return index meta generations instance
     */
    private static IndexMetaDataGenerations buildIndexMetaGenerations(
        Map<SnapshotId, Map<String, String>> indexMetaLookup,
        Map<String, IndexId> indexLookup,
        Map<String, String> indexMetaIdentifiers
    ) {
        if (indexMetaLookup.isEmpty()) {
            return IndexMetaDataGenerations.EMPTY;
        }
        // Build a new map that instead of indexing the per-snapshot index generations by index id string, is indexed by IndexId
        final Map<SnapshotId, Map<IndexId, String>> indexGenerations = new HashMap<>(indexMetaLookup.size());
        for (Map.Entry<SnapshotId, Map<String, String>> snapshotIdMapEntry : indexMetaLookup.entrySet()) {
            final Map<String, String> val = snapshotIdMapEntry.getValue();
            final Map<IndexId, String> forSnapshot = new HashMap<>(val.size());
            for (Map.Entry<String, String> generationEntry : val.entrySet()) {
                forSnapshot.put(indexLookup.get(generationEntry.getKey()), generationEntry.getValue());
            }
            indexGenerations.put(snapshotIdMapEntry.getKey(), forSnapshot);
        }
        return new IndexMetaDataGenerations(indexGenerations, indexMetaIdentifiers);
    }

    /**
     * Parses the "snapshots" field and fills maps for the various per snapshot properties. This method must run before
     * {@link #parseIndices} which will rely on the maps of snapshot properties to have been populated already.
     *
     * @param parser           x-content parse
     * @param snapshots        map of snapshot uuid to {@link SnapshotId}
     * @param snapshotsDetails map of snapshot uuid to {@link SnapshotDetails}
     * @param indexMetaLookup  map of {@link SnapshotId} to map of index id (as returned by {@link IndexId#getId}) that defines the index
     *                         metadata generations for the snapshot
     */
    private static void parseSnapshots(
        XContentParser parser,
        Map<String, SnapshotId> snapshots,
        Map<String, SnapshotDetails> snapshotsDetails,
        Map<SnapshotId, Map<String, String>> indexMetaLookup
    ) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser);
        final Map<String, String> stringDeduplicator = new HashMap<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            String name = null;
            String uuid = null;
            SnapshotState state = null;
            Map<String, String> metaGenerations = null;
            Version version = null;
            long startTimeMillis = -1;
            long endTimeMillis = -1;
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                String currentFieldName = parser.currentName();
                parser.nextToken();
                switch (currentFieldName) {
                    case NAME:
                        name = parser.text();
                        break;
                    case UUID:
                        uuid = parser.text();
                        break;
                    case STATE:
                        state = SnapshotState.fromValue((byte) parser.intValue());
                        break;
                    case INDEX_METADATA_LOOKUP:
                        metaGenerations = parser.map(HashMap::new, p -> stringDeduplicator.computeIfAbsent(p.text(), Function.identity()));
                        break;
                    case VERSION:
                        version = Version.fromString(parser.text());
                        break;
                    case START_TIME_MILLIS:
                        assert startTimeMillis == -1;
                        startTimeMillis = parser.longValue();
                        break;
                    case END_TIME_MILLIS:
                        assert endTimeMillis == -1;
                        endTimeMillis = parser.longValue();
                        break;
                }
            }
            assert (startTimeMillis == -1) == (endTimeMillis == -1) : "unexpected: " + startTimeMillis + ", " + endTimeMillis + ", ";
            final SnapshotId snapshotId = new SnapshotId(name, uuid);
            if (state != null || version != null) {
                snapshotsDetails.put(uuid, new SnapshotDetails(state, version, startTimeMillis, endTimeMillis));
            }
            snapshots.put(uuid, snapshotId);
            if (metaGenerations != null && metaGenerations.isEmpty() == false) {
                indexMetaLookup.put(snapshotId, metaGenerations);
            }
        }
    }

    /**
     * Parses information about all indices tracked in the repository and populates {@code indexSnapshots}, {@code indexLookup} and
     * {@code shardGenerations}.
     *
     * @param parser              x-content parser
     * @param fixBrokenShardGens  whether or not to fix broken shard generation (see {@link #snapshotsFromXContent} for details)
     * @param snapshots           map of snapshot uuid to {@link SnapshotId} that was populated by {@link #parseSnapshots}
     * @param indexSnapshots      map of {@link IndexId} to list of {@link SnapshotId} that contain the given index
     * @param indexLookup         map of index uuid (as returned by {@link IndexId#getId}) to {@link IndexId}
     * @param shardGenerations    shard generations builder that is populated index by this method
     */
    private static void parseIndices(
        XContentParser parser,
        boolean fixBrokenShardGens,
        Map<String, SnapshotId> snapshots,
        Map<IndexId, List<SnapshotId>> indexSnapshots,
        Map<String, IndexId> indexLookup,
        ShardGenerations.Builder shardGenerations
    ) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            final String indexName = parser.currentName();
            final List<SnapshotId> snapshotIds = new ArrayList<>();
            final List<String> gens = new ArrayList<>();

            IndexId indexId = null;
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                final String indexMetaFieldName = parser.currentName();
                final XContentParser.Token currentToken = parser.nextToken();
                switch (indexMetaFieldName) {
                    case INDEX_ID:
                        indexId = new IndexId(indexName, parser.text());
                        break;
                    case SNAPSHOTS:
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, currentToken, parser);
                        XContentParser.Token currToken;
                        while ((currToken = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            final String uuid;
                            // the old format pre 5.4.1 which contains the snapshot name and uuid
                            if (currToken == XContentParser.Token.START_OBJECT) {
                                uuid = parseLegacySnapshotUUID(parser);
                            } else {
                                // the new format post 5.4.1 that only contains the snapshot uuid,
                                // since we already have the name/uuid combo in the snapshots array
                                uuid = parser.text();
                            }

                            final SnapshotId snapshotId = snapshots.get(uuid);
                            if (snapshotId == null) {
                                // A snapshotted index references a snapshot which does not exist in
                                // the list of snapshots. This can happen when multiple clusters in
                                // different versions create or delete snapshot in the same repository.
                                throw new ElasticsearchParseException(
                                    "Detected a corrupted repository, index "
                                        + indexId
                                        + " references an unknown snapshot uuid ["
                                        + uuid
                                        + "]"
                                );
                            }
                            snapshotIds.add(snapshotId);
                        }
                        break;
                    case SHARD_GENERATIONS:
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, currentToken, parser);
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            gens.add(parser.textOrNull());
                        }
                        break;
                }
            }
            assert indexId != null;
            indexSnapshots.put(indexId, Collections.unmodifiableList(snapshotIds));
            indexLookup.put(indexId.getId(), indexId);
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
    }

    private static String parseLegacySnapshotUUID(XContentParser parser) throws IOException {
        String uuid = null;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String currentFieldName = parser.currentName();
            parser.nextToken();
            if (UUID.equals(currentFieldName)) {
                uuid = parser.text();
            }
        }
        return uuid;
    }

    /**
     * A few details of an individual snapshot stored in the top-level index blob, so they are readily accessible without having to load
     * the corresponding {@link SnapshotInfo} blob for each snapshot.
     */
    public static class SnapshotDetails {

        public static SnapshotDetails EMPTY = new SnapshotDetails(null, null, -1, -1);

        @Nullable // TODO forbid nulls here, this only applies to very old repositories
        private final SnapshotState snapshotState;

        @Nullable // may be omitted if pre-7.6 nodes were involved somewhere
        private final Version version;

        // May be -1 if unknown, which happens if the snapshot was taken before 7.14 and hasn't been updated yet
        private final long startTimeMillis;

        // May be -1 if unknown, which happens if the snapshot was taken before 7.14 and hasn't been updated yet
        private final long endTimeMillis;

        public SnapshotDetails(@Nullable SnapshotState snapshotState, @Nullable Version version, long startTimeMillis, long endTimeMillis) {
            this.snapshotState = snapshotState;
            this.version = version;
            this.startTimeMillis = startTimeMillis;
            this.endTimeMillis = endTimeMillis;
        }

        @Nullable
        public SnapshotState getSnapshotState() {
            return snapshotState;
        }

        @Nullable
        public Version getVersion() {
            return version;
        }

        /**
         * @return start time in millis since the epoch, or {@code -1} if unknown.
         */
        public long getStartTimeMillis() {
            return startTimeMillis;
        }

        /**
         * @return end time in millis since the epoch, or {@code -1} if unknown.
         */
        public long getEndTimeMillis() {
            return endTimeMillis;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SnapshotDetails that = (SnapshotDetails) o;
            return startTimeMillis == that.startTimeMillis
                && endTimeMillis == that.endTimeMillis
                && snapshotState == that.snapshotState
                && Objects.equals(version, that.version);
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshotState, version, startTimeMillis, endTimeMillis);
        }

    }

}
