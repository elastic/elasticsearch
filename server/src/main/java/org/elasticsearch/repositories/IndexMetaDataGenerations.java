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

import org.elasticsearch.snapshots.SnapshotId;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public final class IndexMetaDataGenerations {

    public static final IndexMetaDataGenerations EMPTY = new IndexMetaDataGenerations(Collections.emptyMap(), Collections.emptyMap());

    /**
     * Map of {@code index_id + "-" snapshot_id} to blob uuid.
     */
    final Map<String, String> lookup;

    /**
     * Map of index metadata hash to blob uuid.
     */
    final Map<String, String> hashes;

    IndexMetaDataGenerations(Map<String, String> lookup, Map<String, String> hashes) {
        this.lookup = Map.copyOf(lookup);
        this.hashes = Map.copyOf(hashes);
    }

    public String getIndexMetaKey(String metaHash) {
        return hashes.get(metaHash);
    }

    public String indexMetaBlobId(SnapshotId snapshotId, IndexId indexId) {
        final String key = snapshotId.getUUID() + "-" + indexId.getId();
        final String hash = lookup.get(key);
        if (hash == null) {
            return snapshotId.getUUID();
        } else {
            return hashes.get(hash);
        }
    }

    public IndexMetaDataGenerations withAddedSnapshot(SnapshotId snapshotId, Map<IndexId, String> newLookup,
                                                      Map<String, String> newHashes) {
        final Map<String, String> updatedIndexMetaLookup = new HashMap<>(this.lookup);
        final Map<String, String> updatedIndexMetaHashes = new HashMap<>(hashes);
        updatedIndexMetaHashes.putAll(newHashes);
        updatedIndexMetaLookup.putAll(newLookup.entrySet().stream().collect(
            Collectors.toMap(e -> IndexMetaDataGenerations.lookupKey(snapshotId, e.getKey()), Map.Entry::getValue)));
        return new IndexMetaDataGenerations(updatedIndexMetaLookup, updatedIndexMetaHashes);
    }

    public IndexMetaDataGenerations withRemovedSnapshot(SnapshotId snapshotId, Collection<IndexId> indexIds) {
        final Map<String, String> updatedIndexMetaLookup = new HashMap<>(lookup);
        for (final IndexId indexId : indexIds) {
            updatedIndexMetaLookup.remove(lookupKey(snapshotId, indexId));
        }
        final Map<String, String> updatedIndexMetaDataHashes = new HashMap<>(hashes);
        updatedIndexMetaDataHashes.keySet().removeIf(k -> updatedIndexMetaLookup.containsValue(k) == false);
        return new IndexMetaDataGenerations(updatedIndexMetaLookup, updatedIndexMetaDataHashes);
    }

    private static String lookupKey(SnapshotId snapshotId, IndexId indexId) {
        return snapshotId.getUUID() + "-" + indexId.getId();
    }
}
