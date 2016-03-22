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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.MetaDataStateFormat;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

/**
 * Index metadata that is persisted on disk.
 */
final public class IndexStateMetaData implements ToXContent {

    public static final String INDEX_STATE_FILE_PREFIX = "state-";
    public static final String CLUSTER_UUID_NA_VALUE = "_na_";
    private static final String CLUSTER_UUID_FIELD = "clusterUUID";
    private static final String INDEX_METADATA_FIELD = "indexMetaData";

    private final IndexMetaData indexMetaData;
    private final String clusterUUID;

    public IndexStateMetaData(final IndexMetaData indexMetaData, final String clusterUUID) {
        this.indexMetaData = indexMetaData;
        this.clusterUUID = clusterUUID;
    }

    /**
     * The cluster state's index metadata
     */
    public IndexMetaData getIndexMetaData() {
        return indexMetaData;
    }

    /**
     * The cluster UUID of the cluster that last created or updated the index
     */
    public String getClusterUUID() {
        return clusterUUID;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || this.getClass() != other.getClass()) {
            return false;
        }
        final IndexStateMetaData that = (IndexStateMetaData) other;
        return Objects.equals(clusterUUID, that.clusterUUID) && Objects.equals(indexMetaData, that.indexMetaData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterUUID, indexMetaData);
    }

    public static IndexStateMetaData fromXContent(final XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("IndexStateMetaData must start as an object");
        }
        XContentParser.Token token = parser.nextToken();
        // first field is not the cluster UUID, so it must be the old way of storing index metadata
        if (token != XContentParser.Token.FIELD_NAME || parser.currentName().equals(CLUSTER_UUID_FIELD) == false) {
            // set the cluster UUID with _na_ so we can save the updated index metadata format later
            return new IndexStateMetaData(IndexMetaData.Builder.fromXContent(parser), CLUSTER_UUID_NA_VALUE);
        }

        if (token != XContentParser.Token.FIELD_NAME) {
            throw new IllegalArgumentException("expected field name but got a " + parser.currentToken());
        }
        String currentFieldName = null;
        String clusterUUID = null;
        IndexMetaData indexMetaData = null;
        do {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (INDEX_METADATA_FIELD.equals(currentFieldName)) {
                    indexMetaData = IndexMetaData.Builder.fromXContent(parser);
                } else {
                    throw new IllegalArgumentException("Unexpected object [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (CLUSTER_UUID_FIELD.equals(currentFieldName)) {
                    clusterUUID = parser.text();
                } else {
                    throw new IllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                }
            } else {
                throw new IllegalArgumentException("Unexpected token " + token);
            }
        } while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT);

        if (indexMetaData == null) {
            throw new ElasticsearchParseException("Could not parse indexMetaData from x-content");
        }
        if (clusterUUID == null) {
            clusterUUID = IndexStateMetaData.CLUSTER_UUID_NA_VALUE;
        }
        return new IndexStateMetaData(indexMetaData, clusterUUID);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(CLUSTER_UUID_FIELD, getClusterUUID());
        builder.startObject(INDEX_METADATA_FIELD);
        IndexMetaData.Builder.toXContent(getIndexMetaData(), builder, params);
        builder.endObject();
        return builder;
    }

    /**
     * Test whether the current cluster UUID is the same as the given one.
     */
    public boolean isSameClusterUUID(final String otherClusterUUID) {
        assert clusterUUID != null;
        assert otherClusterUUID != null;
        return clusterUUID.equals(otherClusterUUID);
    }

    @Override
    public String toString() {
        return "IndexStateMetaData[clusterUUID=" + clusterUUID + ", indexUUID=" + indexMetaData.getIndexUUID() +
               ", indexName=" + indexMetaData.getIndex().getName() + ", state=" + indexMetaData.getState() +
               ", noOfShards=" + indexMetaData.getNumberOfShards() + ", noOfReplicas=" + indexMetaData.getNumberOfReplicas() + "]";
    }

    private static final ToXContent.Params FORMAT_PARAMS = new MapParams(Collections.singletonMap("binary", "true"));

    /**
     * StateFormat that can read and write {@link IndexStateMetaData}
     */
    public static MetaDataStateFormat<IndexStateMetaData> FORMAT =
        new MetaDataStateFormat<IndexStateMetaData>(XContentType.SMILE, INDEX_STATE_FILE_PREFIX) {
            @Override
            public void toXContent(final XContentBuilder builder, final IndexStateMetaData persistedIndex) throws IOException {
                persistedIndex.toXContent(builder, FORMAT_PARAMS);
            }
            @Override
            public IndexStateMetaData fromXContent(final XContentParser parser) throws IOException {
                return IndexStateMetaData.fromXContent(parser);
            }
        };

}
