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
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.IOException;
import java.util.ArrayList;
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
public final class RepositoryData implements ToXContent {

    public static final RepositoryData EMPTY = new RepositoryData(Collections.emptyList(), Collections.emptyMap());

    /**
     * The ids of the snapshots in the repository.
     */
    private final List<SnapshotId> snapshotIds;
    /**
     * The indices found in the repository across all snapshots, as a name to {@link IndexId} mapping
     */
    private final Map<String, IndexId> indices;
    /**
     * The snapshots that each index belongs to.
     */
    private final Map<IndexId, Set<SnapshotId>> indexSnapshots;

    public RepositoryData(List<SnapshotId> snapshotIds, Map<IndexId, Set<SnapshotId>> indexSnapshots) {
        this.snapshotIds = Collections.unmodifiableList(snapshotIds);
        this.indices = Collections.unmodifiableMap(indexSnapshots.keySet()
                                                       .stream()
                                                       .collect(Collectors.toMap(IndexId::getName, Function.identity())));
        this.indexSnapshots = Collections.unmodifiableMap(indexSnapshots);
    }

    protected RepositoryData copy() {
        return new RepositoryData(snapshotIds, indexSnapshots);
    }

    /**
     * Returns an unmodifiable list of the snapshot ids.
     */
    public List<SnapshotId> getSnapshotIds() {
        return snapshotIds;
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
    public RepositoryData addSnapshot(final SnapshotId snapshotId, final List<IndexId> snapshottedIndices) {
        if (snapshotIds.contains(snapshotId)) {
            throw new IllegalArgumentException("[" + snapshotId + "] already exists in the repository data");
        }
        List<SnapshotId> snapshots = new ArrayList<>(snapshotIds);
        snapshots.add(snapshotId);
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
        return new RepositoryData(snapshots, allIndexSnapshots);
    }

    /**
     * Initializes the indices in the repository metadata; returns a new instance.
     */
    public RepositoryData initIndices(final Map<IndexId, Set<SnapshotId>> indexSnapshots) {
        return new RepositoryData(snapshotIds, indexSnapshots);
    }

    /**
     * Remove a snapshot and remove any indices that no longer exist in the repository due to the deletion of the snapshot.
     */
    public RepositoryData removeSnapshot(final SnapshotId snapshotId) {
        List<SnapshotId> newSnapshotIds = snapshotIds
                                              .stream()
                                              .filter(id -> snapshotId.equals(id) == false)
                                              .collect(Collectors.toList());
        Map<IndexId, Set<SnapshotId>> indexSnapshots = new HashMap<>();
        for (final IndexId indexId : indices.values()) {
            Set<SnapshotId> set;
            Set<SnapshotId> snapshotIds = this.indexSnapshots.get(indexId);
            assert snapshotIds != null;
            if (snapshotIds.contains(snapshotId)) {
                if (snapshotIds.size() == 1) {
                    // removing the snapshot will mean no more snapshots have this index, so just skip over it
                    continue;
                }
                set = new LinkedHashSet<>(snapshotIds);
                set.remove(snapshotId);
            } else {
                set = snapshotIds;
            }
            indexSnapshots.put(indexId, set);
        }

        return new RepositoryData(newSnapshotIds, indexSnapshots);
    }

    /**
     * Returns an immutable collection of the snapshot ids for the snapshots that contain the given index.
     */
    public Set<SnapshotId> getSnapshots(final IndexId indexId) {
        Set<SnapshotId> snapshotIds = indexSnapshots.get(indexId);
        if (snapshotIds == null) {
            throw new IllegalArgumentException("unknown snapshot index " + indexId + "");
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
        @SuppressWarnings("unchecked") RepositoryData that = (RepositoryData) obj;
        return snapshotIds.equals(that.snapshotIds)
                   && indices.equals(that.indices)
                   && indexSnapshots.equals(that.indexSnapshots);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotIds, indices, indexSnapshots);
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
    private static final String INDICES = "indices";
    private static final String INDEX_ID = "id";

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        // write the snapshots list
        builder.startArray(SNAPSHOTS);
        for (final SnapshotId snapshot : getSnapshotIds()) {
            snapshot.toXContent(builder, params);
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
                snapshotId.toXContent(builder, params);
            }
            builder.endArray();
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public static RepositoryData fromXContent(final XContentParser parser) throws IOException {
        List<SnapshotId> snapshots = new ArrayList<>();
        Map<IndexId, Set<SnapshotId>> indexSnapshots = new HashMap<>();
        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                String currentFieldName = parser.currentName();
                if (SNAPSHOTS.equals(currentFieldName)) {
                    if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            snapshots.add(SnapshotId.fromXContent(parser));
                        }
                    } else {
                        throw new ElasticsearchParseException("expected array for [" + currentFieldName + "]");
                    }
                } else if (INDICES.equals(currentFieldName)) {
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
                                    snapshotIds.add(SnapshotId.fromXContent(parser));
                                }
                            }
                        }
                        assert indexId != null;
                        indexSnapshots.put(new IndexId(indexName, indexId), snapshotIds);
                    }
                } else {
                    throw new ElasticsearchParseException("unknown field name  [" + currentFieldName + "]");
                }
            }
        } else {
            throw new ElasticsearchParseException("start object expected");
        }
        return new RepositoryData(snapshots, indexSnapshots);
    }

}
