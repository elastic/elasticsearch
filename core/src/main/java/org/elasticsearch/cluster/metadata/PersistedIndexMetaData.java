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
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.FromXContentBuilder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.MetaDataStateFormat;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

/**
 * Index metadata that is persisted on disk.
 */
final public class PersistedIndexMetaData implements FromXContentBuilder<PersistedIndexMetaData>, ToXContent {

    public static final PersistedIndexMetaData PROTOTYPE = new PersistedIndexMetaData(null, null);
    public static final String INDEX_STATE_FILE_PREFIX = "state-";
    public static final String CLUSTER_UUID_NA_VALUE = "_na_";
    private static final String CLUSTER_UUID_FIELD = "clusterUUID";
    private static final String INDEX_METADATA_FIELD = "indexMetaData";

    private final IndexMetaData indexMetaData;
    private final String clusterUUID;

    public PersistedIndexMetaData(final IndexMetaData indexMetaData, final String clusterUUID) {
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
        final PersistedIndexMetaData that = (PersistedIndexMetaData) other;
        return Objects.equals(clusterUUID, that.clusterUUID) && Objects.equals(indexMetaData, that.indexMetaData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterUUID, indexMetaData);
    }

    @Override
    public PersistedIndexMetaData fromXContent(XContentParser parser, ParseFieldMatcher parseFieldMatcher) throws IOException {
        if (parser.currentToken() == null) { // fresh parser? move to the first token
            parser.nextToken();
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {  // on a start object move to next token
            parser.nextToken();
        }

        XContentParser.Token token = parser.currentToken();
        // first field is not the cluster UUID, so it must be the old way of storing index metadata
        if (token != XContentParser.Token.FIELD_NAME || parser.currentName().equals(CLUSTER_UUID_FIELD) == false) {
            // set the cluster UUID with _na_ so we can save the updated index metadata format later
            return new PersistedIndexMetaData(IndexMetaData.Builder.fromXContent(parser), CLUSTER_UUID_NA_VALUE);
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
            clusterUUID = PersistedIndexMetaData.CLUSTER_UUID_NA_VALUE;
        }
        return new PersistedIndexMetaData(indexMetaData, clusterUUID);
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
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }

    private static final ToXContent.Params FORMAT_PARAMS = new MapParams(Collections.singletonMap("binary", "true"));

    /**
     * StateFormat that can read and write {@link PersistedIndexMetaData}
     */
    public static MetaDataStateFormat<PersistedIndexMetaData> FORMAT =
        new MetaDataStateFormat<PersistedIndexMetaData>(XContentType.SMILE, INDEX_STATE_FILE_PREFIX) {
            @Override
            public void toXContent(final XContentBuilder builder, final PersistedIndexMetaData persistedIndex) throws IOException {
                persistedIndex.toXContent(builder, FORMAT_PARAMS);
            }
            @Override
            public PersistedIndexMetaData fromXContent(final XContentParser parser) throws IOException {
                return PersistedIndexMetaData.PROTOTYPE.fromXContent(parser, ParseFieldMatcher.EMPTY);
            }
        };

}
