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

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Tracks the blob uuids of blobs containing {@link IndexMetaData} for snapshots as well as the hash of
 * each of these blobs.
 * Before writing a new {@link IndexMetaData} blob during snapshot finalization in
 * {@link org.elasticsearch.repositories.blobstore.BlobStoreRepository#finalizeSnapshot} an instance of {@link IndexMetaData} should be
 * hashed and then used to check if it already exists in the repository via {@link #getIndexMetaBlobId(String)}.
 */
public final class IndexMetaDataGenerations {

    public static final IndexMetaDataGenerations EMPTY = new IndexMetaDataGenerations(Collections.emptyMap(), Collections.emptyMap());

    /**
     * Map of {@link SnapshotId} to a map of the indices in a snapshot mapping {@link IndexId} to metadata hash.
     * The hashes in the nested map can be mapped to the relevant blob uuid via {@link #getIndexMetaBlobId}.
     */
    final Map<SnapshotId, Map<IndexId, String>> lookup;

    /**
     * Map of index metadata hash to blob uuid.
     */
    final Map<String, String> hashes;

    IndexMetaDataGenerations(Map<SnapshotId, Map<IndexId, String>> lookup, Map<String, String> hashes) {
        assert hashes.keySet().equals(lookup.values().stream().flatMap(m -> m.values().stream()).collect(Collectors.toSet())) :
            "Hash mappings " + hashes +" don't track the same blob ids as the lookup map " + lookup;
        assert lookup.values().stream().noneMatch(Map::isEmpty) : "Lookup contained empty map [" + lookup + "]";
        this.lookup = Map.copyOf(lookup);
        this.hashes = Map.copyOf(hashes);
    }

    public boolean isEmpty() {
        return hashes.isEmpty();
    }

    /**
     * Gets the blob id by the hash of {@link IndexMetaData} (computed via {@link #hashIndexMetaData}) or {@code null} if none is
     * tracked for the hash.
     *
     * @param metaHash hash of {@link IndexMetaData}
     * @return blob id for the given metadata hash or {@code null} if the hash is not part of the repository yet
     */
    @Nullable
    public String getIndexMetaBlobId(String metaHash) {
        return hashes.get(metaHash);
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
        final String hash = lookup.getOrDefault(snapshotId, Collections.emptyMap()).get(indexId);
        if (hash == null) {
            return snapshotId.getUUID();
        } else {
            return hashes.get(hash);
        }
    }

    /**
     * Create a new instance with the given snapshot and index metadata uuids and hashes added.
     *
     * @param snapshotId SnapshotId
     * @param newLookup new mappings of index + snapshot to index metadata hash
     * @param newHashes new mappings of index metadata hash to blob id
     * @return instance with added snapshot
     */
    public IndexMetaDataGenerations withAddedSnapshot(SnapshotId snapshotId, Map<IndexId, String> newLookup,
                                                      Map<String, String> newHashes) {
        final Map<SnapshotId, Map<IndexId, String>> updatedIndexMetaLookup = new HashMap<>(this.lookup);
        final Map<String, String> updatedIndexMetaHashes = new HashMap<>(hashes);
        updatedIndexMetaHashes.putAll(newHashes);
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
        return new IndexMetaDataGenerations(updatedIndexMetaLookup, updatedIndexMetaHashes);
    }

    /**
     * Create a new instance with the given snapshot removed.
     *
     * @param snapshotId SnapshotId
     * @return new instance without the given snapshot
     */
    public IndexMetaDataGenerations withRemovedSnapshot(SnapshotId snapshotId) {
        final Map<SnapshotId, Map<IndexId, String>> updatedIndexMetaLookup = new HashMap<>(lookup);
        updatedIndexMetaLookup.remove(snapshotId);
        final Map<String, String> updatedIndexMetaDataHashes = new HashMap<>(hashes);
        updatedIndexMetaDataHashes.keySet().removeIf(
            k -> updatedIndexMetaLookup.values().stream().noneMatch(hashes -> hashes.containsValue(k)));
        return new IndexMetaDataGenerations(updatedIndexMetaLookup, updatedIndexMetaDataHashes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hashes, lookup);
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
        return lookup.equals(other.lookup) && hashes.equals(other.hashes);
    }

    @Override
    public String toString() {
        return "IndexMetaDataGenerations{lookup:" + lookup + "}{hashes:" + hashes + "}";
    }

    /**
     * Serialize and return the hex encoded SHA256 of {@link IndexMetaData}.
     *
     * @param indexMetaData IndexMetaData
     * @return hex encoded SHA-256
     */
    public static String hashIndexMetaData(IndexMetaData indexMetaData) {
        // Remove primary terms and version from the metadata to get a consistent hash across allocation changes
        final IndexMetaData.Builder normalized = IndexMetaData.builder(indexMetaData).version(1L);
        for (int i = 0; i < indexMetaData.getNumberOfShards(); i++) {
            normalized.primaryTerm(i, 1L);
        }
        // IndexMetaData is serialized as a number of nested maps without any ordering guarantees. To get a consistent hash we first turn
        // it into a nested tree-map to get deterministic ordering across all fields and then hash the nested tree-map
        // TODO: Do the conversion of metadata to map more efficiently without the serialization round-trip
        final Map<String, Object> normalizedMap = deepSort(
            XContentHelper.convertToMap(XContentType.JSON.xContent(), Strings.toString(normalized.build()), true));
        MessageDigest digest = MessageDigests.sha256();
        try (StreamOutput hashOut = new OutputStreamStreamOutput(new OutputStream() {
            @Override
            public void write(int b) {
                digest.update((byte) b);
            }

            @Override
            public void write(byte[] b, int off, int len) {
                digest.update(b, off, len);
            }
        })) {
            hashOut.writeMap(normalizedMap);
        } catch (IOException e) {
            throw new AssertionError("No actual IO happens here", e);
        }
        return MessageDigests.toHexString(digest.digest());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> deepSort(Map<String, Object> map) {
        final TreeMap<String, Object> result = new TreeMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            final Object value = entry.getValue();
            result.put(entry.getKey(), value instanceof Map ? deepSort((Map<String, Object>) value) : value);
        }
        return result;
    }
}
