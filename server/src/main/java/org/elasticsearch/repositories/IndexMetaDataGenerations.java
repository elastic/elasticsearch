/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.snapshots.SnapshotId;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Tracks the blob uuids of blobs containing {@link IndexMetadata} for snapshots as well an identifier for each of these blobs.
 * Before writing a new {@link IndexMetadata} blob during snapshot finalization in
 * {@link org.elasticsearch.repositories.blobstore.BlobStoreRepository#finalizeSnapshot} the identifier for an instance of
 * {@link IndexMetadata} should be computed and then used to check if it already exists in the repository via
 * {@link #getIndexMetaBlobId(String)}.
 */
public final class IndexMetaDataGenerations {

    public static final IndexMetaDataGenerations EMPTY = new IndexMetaDataGenerations(Collections.emptyMap(), Collections.emptyMap());

    /**
     * Map of {@link SnapshotId} to a map of the indices in a snapshot mapping {@link IndexId} to metadata identifiers.
     * The identifiers in the nested map can be mapped to the relevant blob uuid via {@link #getIndexMetaBlobId}.
     */
    final Map<SnapshotId, Map<IndexId, String>> lookup;

    /**
     * Map of index metadata identifier to blob uuid.
     */
    final Map<String, String> identifiers;

    IndexMetaDataGenerations(Map<SnapshotId, Map<IndexId, String>> lookup, Map<String, String> identifiers) {
        assert identifiers.keySet().equals(lookup.values().stream().flatMap(m -> m.values().stream()).collect(Collectors.toSet()))
            : "identifier mappings " + identifiers + " don't track the same blob ids as the lookup map " + lookup;
        assert lookup.values().stream().noneMatch(Map::isEmpty) : "Lookup contained empty map [" + lookup + "]";
        this.lookup = Map.copyOf(lookup);
        this.identifiers = Map.copyOf(identifiers);
    }

    public boolean isEmpty() {
        return identifiers.isEmpty();
    }

    /**
     * Gets the blob id by the identifier of {@link org.elasticsearch.cluster.metadata.IndexMetadata}
     * (computed via {@link #buildUniqueIdentifier}) or {@code null} if none is tracked for the identifier.
     *
     * @param metaIdentifier identifier for {@link IndexMetadata}
     * @return blob id for the given metadata identifier or {@code null} if the identifier is not part of the repository yet
     */
    @Nullable
    public String getIndexMetaBlobId(String metaIdentifier) {
        return identifiers.get(metaIdentifier);
    }

    /**
     * Get the blob id by {@link SnapshotId} and {@link IndexId} and fall back to the value of {@link SnapshotId#getUUID()} if none is
     * known to enable backwards compatibility with versions older than
     * {@link org.elasticsearch.snapshots.SnapshotsService#SHARD_GEN_IN_REPO_DATA_VERSION} which used the snapshot uuid as index metadata
     * blob uuid.
     *
     * @param snapshotId Snapshot Id
     * @param indexId    Index Id
     * @return blob id for the given index metadata
     */
    public String indexMetaBlobId(SnapshotId snapshotId, IndexId indexId) {
        final String identifier = snapshotIndexMetadataIdentifier(snapshotId, indexId);
        if (identifier == null) {
            return snapshotId.getUUID();
        } else {
            return identifiers.get(identifier);
        }
    }

    /**
     * Gets the {@link org.elasticsearch.cluster.metadata.IndexMetadata} identifier for the given snapshot
     * if the snapshot contains the referenced index, otherwise it returns {@code null}.
     */
    @Nullable
    public String snapshotIndexMetadataIdentifier(SnapshotId snapshotId, IndexId indexId) {
        return lookup.getOrDefault(snapshotId, Collections.emptyMap()).get(indexId);
    }

    /**
     * Create a new instance with the given snapshot and index metadata uuids and identifiers added.
     *
     * @param snapshotId SnapshotId
     * @param newLookup new mappings of index + snapshot to index metadata identifier
     * @param newIdentifiers new mappings of index metadata identifier to blob id
     * @return instance with added snapshot
     */
    public IndexMetaDataGenerations withAddedSnapshot(
        SnapshotId snapshotId,
        Map<IndexId, String> newLookup,
        Map<String, String> newIdentifiers
    ) {
        final Map<SnapshotId, Map<IndexId, String>> updatedIndexMetaLookup = new HashMap<>(this.lookup);
        final Map<String, String> updatedIndexMetaIdentifiers = new HashMap<>(identifiers);
        updatedIndexMetaIdentifiers.putAll(newIdentifiers);
        if (newLookup.isEmpty() == false) {
            final Map<String, String> identifierDeduplicator = new HashMap<>(this.identifiers.size());
            for (String identifier : identifiers.keySet()) {
                identifierDeduplicator.put(identifier, identifier);
            }
            final Map<IndexId, String> fixedLookup = new HashMap<>(newLookup.size());
            for (Map.Entry<IndexId, String> entry : newLookup.entrySet()) {
                final String generation = entry.getValue();
                fixedLookup.put(entry.getKey(), identifierDeduplicator.getOrDefault(generation, generation));
            }
            final Map<IndexId, String> existing = updatedIndexMetaLookup.put(snapshotId, Map.copyOf(fixedLookup));
            assert existing == null : "unexpected existing index generation mappings " + existing;
        }
        return new IndexMetaDataGenerations(updatedIndexMetaLookup, updatedIndexMetaIdentifiers);
    }

    /**
     * Create a new instance with the given snapshot removed.
     *
     * @param snapshotIds SnapshotIds to remove
     * @return new instance without the given snapshot
     */
    public IndexMetaDataGenerations withRemovedSnapshots(Collection<SnapshotId> snapshotIds) {
        final Map<SnapshotId, Map<IndexId, String>> updatedIndexMetaLookup = new HashMap<>(lookup);
        updatedIndexMetaLookup.keySet().removeAll(snapshotIds);
        final Map<String, String> updatedIndexMetaIdentifiers = new HashMap<>(identifiers);
        updatedIndexMetaIdentifiers.keySet()
            .removeIf(k -> updatedIndexMetaLookup.values().stream().noneMatch(identifiers -> identifiers.containsValue(k)));
        return new IndexMetaDataGenerations(updatedIndexMetaLookup, updatedIndexMetaIdentifiers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifiers, lookup);
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof IndexMetaDataGenerations == false) {
            return false;
        }
        final IndexMetaDataGenerations other = (IndexMetaDataGenerations) that;
        return lookup.equals(other.lookup) && identifiers.equals(other.identifiers);
    }

    @Override
    public String toString() {
        return "IndexMetaDataGenerations{lookup:" + lookup + "}{identifier:" + identifiers + "}";
    }

    /**
     * Compute identifier for {@link IndexMetadata} from its index- and history-uuid as well as its settings-, mapping- and alias-version.
     * If an index did not see a change in its settings, mappings or aliases between two points in time then the identifier will not change
     * between them either.
     *
     * @param indexMetaData IndexMetaData
     * @return identifier string
     */
    public static String buildUniqueIdentifier(IndexMetadata indexMetaData) {
        return indexMetaData.getIndexUUID()
            + "-"
            + indexMetaData.getSettings().get(IndexMetadata.SETTING_HISTORY_UUID, IndexMetadata.INDEX_UUID_NA_VALUE)
            + "-"
            + indexMetaData.getSettingsVersion()
            + "-"
            + indexMetaData.getMappingVersion()
            + "-"
            + indexMetaData.getAliasesVersion();
    }
}
