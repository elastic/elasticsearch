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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
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
     * An instance initialized for an empty repository.
     */
    public static final RepositoryData EMPTY = new RepositoryData(EMPTY_REPO_GEN,
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyList());

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
     * The snapshots that are no longer compatible with the current cluster ES version.
     */
    private final List<SnapshotId> incompatibleSnapshotIds;

    public RepositoryData(long genId,
                          Map<String, SnapshotId> snapshotIds,
                          Map<String, SnapshotState> snapshotStates,
                          Map<IndexId, Set<SnapshotId>> indexSnapshots,
                          List<SnapshotId> incompatibleSnapshotIds) {
        this.genId = genId;
        this.snapshotIds = Collections.unmodifiableMap(snapshotIds);
        this.snapshotStates = Collections.unmodifiableMap(snapshotStates);
        this.indices = Collections.unmodifiableMap(indexSnapshots.keySet().stream()
            .collect(Collectors.toMap(IndexId::getName, Function.identity())));
        this.indexSnapshots = Collections.unmodifiableMap(indexSnapshots);
        this.incompatibleSnapshotIds = Collections.unmodifiableList(incompatibleSnapshotIds);
    }

    protected RepositoryData copy() {
        return new RepositoryData(genId, snapshotIds, snapshotStates, indexSnapshots, incompatibleSnapshotIds);
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
     * Returns an immutable collection of the snapshot ids in the repository that are incompatible with the
     * current ES version.
     */
    public Collection<SnapshotId> getIncompatibleSnapshotIds() {
        return incompatibleSnapshotIds;
    }

    /**
     * Returns an immutable collection of all the snapshot ids in the repository, both active and
     * incompatible snapshots.
     */
    public Collection<SnapshotId> getAllSnapshotIds() {
        List<SnapshotId> allSnapshotIds = new ArrayList<>(snapshotIds.size() + incompatibleSnapshotIds.size());
        allSnapshotIds.addAll(snapshotIds.values());
        allSnapshotIds.addAll(incompatibleSnapshotIds);
        return Collections.unmodifiableList(allSnapshotIds);
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
     * Add a snapshot and its indices to the repository; returns a new instance.  If the snapshot
     * already exists in the repository data, this method throws an IllegalArgumentException.
     */
    public RepositoryData addSnapshot(final SnapshotId snapshotId,
                                      final SnapshotState snapshotState,
                                      final List<IndexId> snapshottedIndices) {
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
        for (final IndexId indexId : snapshottedIndices) {
            if (allIndexSnapshots.containsKey(indexId)) {
                Set<SnapshotId> ids = allIndexSnapshots.get(indexId);
                if (ids == null) {
                    ids = new LinkedHashSet<>();
                    allIndexSnapshots.put(indexId, ids);
                }
                ids.add(snapshotId);
            } else {
                Set<SnapshotId> ids = new LinkedHashSet<>();
                ids.add(snapshotId);
                allIndexSnapshots.put(indexId, ids);
            }
        }
        return new RepositoryData(genId, snapshots, newSnapshotStates, allIndexSnapshots, incompatibleSnapshotIds);
    }

    /**
     * Remove a snapshot and remove any indices that no longer exist in the repository due to the deletion of the snapshot.
     */
    public RepositoryData removeSnapshot(final SnapshotId snapshotId) {
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

        return new RepositoryData(genId, newSnapshotIds, newSnapshotStates, indexSnapshots, incompatibleSnapshotIds);
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

    /**
     * Initializes the indices in the repository metadata; returns a new instance.
     */
    public RepositoryData initIndices(final Map<IndexId, Set<SnapshotId>> indexSnapshots) {
        return new RepositoryData(genId, snapshotIds, snapshotStates, indexSnapshots, incompatibleSnapshotIds);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        @SuppressWarnings("unchecked") RepositoryData that = (RepositoryData) obj;
        return snapshotIds.equals(that.snapshotIds)
                   && snapshotStates.equals(that.snapshotStates)
                   && indices.equals(that.indices)
                   && indexSnapshots.equals(that.indexSnapshots)
                   && incompatibleSnapshotIds.equals(that.incompatibleSnapshotIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotIds, snapshotStates, indices, indexSnapshots, incompatibleSnapshotIds);
    }

    /**
     * Resolve the index name to the index id specific to the repository,
     * throwing an exception if the index could not be resolved.
     */
    public IndexId resolveIndexId(final String indexName) {
        if (indices.containsKey(indexName)) {
            return indices.get(indexName);
        } else {
            // on repositories created before 5.0, there was no indices information in the index
            // blob, so if the repository hasn't been updated with new snapshots, no new index blob
            // would have been written, so we only have old snapshots without the index information.
            // in this case, the index id is just the index name
            return new IndexId(indexName, indexName);
        }
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

    private static final String SNAPSHOTS = "snapshots";
    private static final String INCOMPATIBLE_SNAPSHOTS = "incompatible-snapshots";
    private static final String INDICES = "indices";
    private static final String INDEX_ID = "id";
    private static final String NAME = "name";
    private static final String UUID = "uuid";
    private static final String STATE = "state";

    /**
     * Writes the snapshots metadata and the related indices metadata to x-content, omitting the
     * incompatible snapshots.
     */
    public XContentBuilder snapshotsToXContent(final XContentBuilder builder, final ToXContent.Params params) throws IOException {
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
        Map<String, SnapshotId> snapshots = new HashMap<>();
        Map<String, SnapshotState> snapshotStates = new HashMap<>();
        Map<IndexId, Set<SnapshotId>> indexSnapshots = new HashMap<>();
        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                String field = parser.currentName();
                if (SNAPSHOTS.equals(field)) {
                    if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            final SnapshotId snapshotId;
                            // the new format from 5.0 which contains the snapshot name and uuid
                            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
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
                                snapshotId = new SnapshotId(name, uuid);
                                if (state != null) {
                                    snapshotStates.put(uuid, state);
                                }
                            } else {
                                // the old format pre 5.0 that only contains the snapshot name, use the name as the uuid too
                                final String name = parser.text();
                                snapshotId = new SnapshotId(name, name);
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
                        String indexName = parser.currentName();
                        String indexId = null;
                        Set<SnapshotId> snapshotIds = new LinkedHashSet<>();
                        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                            throw new ElasticsearchParseException("start object expected index[" + indexName + "]");
                        }
                        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                            String indexMetaFieldName = parser.currentName();
                            parser.nextToken();
                            if (INDEX_ID.equals(indexMetaFieldName)) {
                                indexId = parser.text();
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
                                    snapshotIds.add(snapshots.get(uuid));
                                }
                            }
                        }
                        assert indexId != null;
                        indexSnapshots.put(new IndexId(indexName, indexId), snapshotIds);
                    }
                } else {
                    throw new ElasticsearchParseException("unknown field name  [" + field + "]");
                }
            }
        } else {
            throw new ElasticsearchParseException("start object expected");
        }
        return new RepositoryData(genId, snapshots, snapshotStates, indexSnapshots, Collections.emptyList());
    }

    /**
     * Writes the incompatible snapshot ids to x-content.
     */
    public XContentBuilder incompatibleSnapshotsToXContent(final XContentBuilder builder, final ToXContent.Params params)
        throws IOException {

        builder.startObject();
        // write the incompatible snapshots list
        builder.startArray(INCOMPATIBLE_SNAPSHOTS);
        for (final SnapshotId snapshot : getIncompatibleSnapshotIds()) {
            snapshot.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    /**
     * Reads the incompatible snapshot ids from x-content, loading them into a new instance of {@link RepositoryData}
     * that is created from the invoking instance, plus the incompatible snapshots that are read from x-content.
     */
    public RepositoryData incompatibleSnapshotsFromXContent(final XContentParser parser) throws IOException {
        List<SnapshotId> incompatibleSnapshotIds = new ArrayList<>();
        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                String currentFieldName = parser.currentName();
                if (INCOMPATIBLE_SNAPSHOTS.equals(currentFieldName)) {
                    if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            incompatibleSnapshotIds.add(SnapshotId.fromXContent(parser));
                        }
                    } else {
                        throw new ElasticsearchParseException("expected array for [" + currentFieldName + "]");
                    }
                } else {
                    throw new ElasticsearchParseException("unknown field name  [" + currentFieldName + "]");
                }
            }
        } else {
            throw new ElasticsearchParseException("start object expected");
        }
        return new RepositoryData(this.genId, this.snapshotIds, this.snapshotStates, this.indexSnapshots, incompatibleSnapshotIds);
    }

}
