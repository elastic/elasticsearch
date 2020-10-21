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

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Nullable;
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
        assert identifiers.keySet().equals(lookup.values().stream().flatMap(m -> m.values().stream()).collect(Collectors.toSet())) :
            "identifier mappings " + identifiers + " don't track the same blob ids as the lookup map " + lookup;
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
        final String identifier = lookup.getOrDefault(snapshotId, Collections.emptyMap()).get(indexId);
        if (identifier == null) {
            return snapshotId.getUUID();
        } else {
            return identifiers.get(identifier);
        }
    }

    /**
     * Create a new instance with the given snapshot and index metadata uuids and identifiers added.
     *
     * @param snapshotId SnapshotId
     * @param newLookup new mappings of index + snapshot to index metadata identifier
     * @param newIdentifiers new mappings of index metadata identifier to blob id
     * @return instance with added snapshot
     */
    public IndexMetaDataGenerations withAddedSnapshot(SnapshotId snapshotId, Map<IndexId, String> newLookup,
                                                      Map<String, String> newIdentifiers) {
        final Map<SnapshotId, Map<IndexId, String>> updatedIndexMetaLookup = new HashMap<>(this.lookup);
        final Map<String, String> updatedIndexMetaIdentifiers = new HashMap<>(identifiers);
        updatedIndexMetaIdentifiers.putAll(newIdentifiers);
        updatedIndexMetaLookup.compute(snapshotId, (snId, lookup) -> {
            if (lookup == null) {
                if (newLookup.isEmpty()) {
                    return null;
                }
                return Map.copyOf(newLookup);
            } else {
                final Map<IndexId, String> updated = new HashMap<>(lookup);
                updated.putAll(newLookup);
                return Map.copyOf(updated);
            }
        });
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
        updatedIndexMetaIdentifiers.keySet().removeIf(
            k -> updatedIndexMetaLookup.values().stream().noneMatch(identifiers -> identifiers.containsValue(k)));
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
        return indexMetaData.getIndexUUID() +
                "-" + indexMetaData.getSettings().get(IndexMetadata.SETTING_HISTORY_UUID, IndexMetadata.INDEX_UUID_NA_VALUE) +
                "-" + indexMetaData.getSettingsVersion() + "-" + indexMetaData.getMappingVersion() +
                "-" + indexMetaData.getAliasesVersion();
    }
}
