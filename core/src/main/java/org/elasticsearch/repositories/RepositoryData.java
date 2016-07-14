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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
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
     * The indices found in the repository across all snapshots.
     */
    private final Map<String, IndexMeta> indices;

    public RepositoryData(List<SnapshotId> snapshotIds, Map<String, IndexMeta> indices) {
        this.snapshotIds = Collections.unmodifiableList(snapshotIds);
        this.indices = Collections.unmodifiableMap(indices);
    }

    /**
     * Returns an unmodifiable list of the snapshot ids.
     */
    public List<SnapshotId> getSnapshotIds() {
        return snapshotIds;
    }

    /**
     * Returns an unmodifiable map of the index names to index metadata in the repository.
     */
    public Map<String, IndexMeta> getIndices() {
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
        Map<String, IndexMeta> indexMetaMap = getIndices();
        Map<String, IndexMeta> addedIndices = new HashMap<>();
        for (IndexId indexId : snapshottedIndices) {
            final String indexName = indexId.getName();
            IndexMeta newIndexMeta;
            if (indexMetaMap.containsKey(indexName)) {
                newIndexMeta = indexMetaMap.get(indexName).addSnapshot(snapshotId);
            } else {
                Set<SnapshotId> ids = new LinkedHashSet<>();
                ids.add(snapshotId);
                newIndexMeta = new IndexMeta(indexId, ids);
            }
            addedIndices.put(indexName, newIndexMeta);
        }
        Map<String, IndexMeta> allIndices = new HashMap<>(indices);
        allIndices.putAll(addedIndices);
        return new RepositoryData(snapshots, allIndices);
    }

    /**
     * Add indices to the repository metadata; returns a new instance.
     */
    public RepositoryData addIndices(final Map<String, IndexMeta> newIndices) {
        Map<String, IndexMeta> map = new HashMap<>(indices);
        map.putAll(newIndices);
        return new RepositoryData(snapshotIds, map);
    }

    /**
     * Remove a snapshot and remove any indices that no longer exist in the repository due to the deletion of the snapshot.
     */
    public RepositoryData removeSnapshot(final SnapshotId snapshotId) {
        List<SnapshotId> newSnapshotIds = snapshotIds
                                              .stream()
                                              .filter(id -> snapshotId.equals(id) == false)
                                              .collect(Collectors.toList());
        Map<String, IndexMeta> newIndices = new HashMap<>();
        for (IndexMeta indexMeta : indices.values()) {
            Set<SnapshotId> set;
            if (indexMeta.getSnapshotIds().contains(snapshotId)) {
                if (indexMeta.getSnapshotIds().size() == 1) {
                    // removing the snapshot will mean no more snapshots have this index, so just skip over it
                    continue;
                }
                set = new LinkedHashSet<>(indexMeta.getSnapshotIds());
                set.remove(snapshotId);
            } else {
                set = indexMeta.getSnapshotIds();
            }
            newIndices.put(indexMeta.getName(), new IndexMeta(indexMeta.getIndexId(), set));
        }

        return new RepositoryData(newSnapshotIds, newIndices);
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
        return snapshotIds.equals(that.snapshotIds) && indices.equals(that.indices);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotIds, indices);
    }

    /**
     * Resolve the index name to the index id specific to the repository,
     * throwing an exception if the index could not be resolved.
     */
    public IndexId resolveIndexId(final String indexName) {
        if (indices.containsKey(indexName)) {
            return indices.get(indexName).getIndexId();
        } else {
            // on repositories created before 5.0, there was no indices information in the index
            // blob, so if the repository hasn't been updated with new snapshots, no new index blob
            // would have been written, so we only have old snapshots without the index information.
            // in this case, the index id is just the index name
            return new IndexId(indexName, indexName);
        }
    }

    /**
     * Resolve the given index names to index ids, throwing an exception
     * if any of the indices could not be resolved.
     */
    public List<IndexId> resolveIndices(final List<String> indices) {
        List<IndexId> resolvedIndices = new ArrayList<>(indices.size());
        for (final String indexName : indices) {
            resolvedIndices.add(resolveIndexId(indexName));
        }
        return resolvedIndices;
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
        for (final IndexMeta indexMeta : getIndices().values()) {
            builder.startObject(indexMeta.getName());
            builder.field(INDEX_ID, indexMeta.getId());
            builder.startArray(SNAPSHOTS);
            for (final SnapshotId snapshotId : indexMeta.getSnapshotIds()) {
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
        Map<String, IndexMeta> indices = new HashMap<>();
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
                        indices.put(indexName, new IndexMeta(indexName, indexId, snapshotIds));
                    }
                } else {
                    throw new ElasticsearchParseException("unknown field name  [" + currentFieldName + "]");
                }
            }
        } else {
            throw new ElasticsearchParseException("start object expected");
        }
        return new RepositoryData(snapshots, indices);
    }

    /**
     * Represents information about a single index snapshotted in a repository.
     */
    public static final class IndexMeta implements Writeable {
        private final IndexId indexId;
        private final Set<SnapshotId> snapshotIds;

        public IndexMeta(final String name, final String id, final Set<SnapshotId> snapshotIds) {
            this(new IndexId(name, id), snapshotIds);
        }

        public IndexMeta(final IndexId indexId, final Set<SnapshotId> snapshotIds) {
            this.indexId = indexId;
            this.snapshotIds = Collections.unmodifiableSet(snapshotIds);
        }

        public IndexMeta(final StreamInput in) throws IOException {
            indexId = new IndexId(in);
            final int size = in.readVInt();
            Set<SnapshotId> ids = new LinkedHashSet<>();
            for (int i = 0; i < size; i++) {
                ids.add(new SnapshotId(in));
            }
            snapshotIds = Collections.unmodifiableSet(ids);
        }

        /**
         * The name of the index.
         */
        public String getName() {
            return indexId.getName();
        }

        /**
         * The unique ID for the index within the repository.  This is *not* the same as the
         * index's UUID, but merely a unique file/URL friendly identifier that a repository can
         * use to name blobs for the index.
         */
        public String getId() {
            return indexId.getId();
        }

        /**
         * An unmodifiable set of snapshot ids that contain this index as part of its snapshot.
         */
        public Set<SnapshotId> getSnapshotIds() {
            return snapshotIds;
        }

        /**
         * The snapshotted index id.
         */
        public IndexId getIndexId() {
            return indexId;
        }

        /**
         * Add a snapshot id to the list of snapshots that contain this index.
         */
        public IndexMeta addSnapshot(final SnapshotId snapshotId) {
            Set<SnapshotId> withAdded = new LinkedHashSet<>(snapshotIds);
            withAdded.add(snapshotId);
            return new IndexMeta(indexId, withAdded);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            indexId.writeTo(out);
            out.writeVInt(snapshotIds.size());
            for (SnapshotId snapshotId : snapshotIds) {
                snapshotId.writeTo(out);
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            @SuppressWarnings("unchecked") IndexMeta that = (IndexMeta) obj;
            return indexId.equals(that.indexId) && snapshotIds.equals(that.snapshotIds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexId, snapshotIds);
        }
    }
}
