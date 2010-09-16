/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.indexer.metadata;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.indexer.IndexerName;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;

/**
 * @author kimchy (shay.banon)
 */
public class IndexersMetaData implements Iterable<IndexerMetaData> {

    private final ImmutableMap<IndexerName, IndexerMetaData> indexers;

    private final boolean recoveredFromGateway;

    private IndexersMetaData(ImmutableMap<IndexerName, IndexerMetaData> indexers, boolean recoveredFromGateway) {
        this.indexers = indexers;
        this.recoveredFromGateway = recoveredFromGateway;
    }

    @Override public Iterator<IndexerMetaData> iterator() {
        return indexers.values().iterator();
    }

    public IndexerMetaData indexer(IndexerName indexerName) {
        return indexers.get(indexerName);
    }

    public boolean recoveredFromGateway() {
        return recoveredFromGateway;
    }

    public static class Builder {
        private MapBuilder<IndexerName, IndexerMetaData> indexers = MapBuilder.newMapBuilder();

        private boolean recoveredFromGateway = false;

        public Builder put(IndexerMetaData.Builder builder) {
            return put(builder.build());
        }

        public Builder put(IndexerMetaData indexerMetaData) {
            indexers.put(indexerMetaData.indexerName(), indexerMetaData);
            return this;
        }

        public IndexerMetaData get(IndexerName indexerName) {
            return indexers.get(indexerName);
        }

        public Builder remove(IndexerName indexerName) {
            indexers.remove(indexerName);
            return this;
        }

        public Builder metaData(IndexersMetaData metaData) {
            this.indexers.putAll(metaData.indexers);
            this.recoveredFromGateway = metaData.recoveredFromGateway;
            return this;
        }

        /**
         * Indicates that this cluster state has been recovered from the gateawy.
         */
        public Builder markAsRecoveredFromGateway() {
            this.recoveredFromGateway = true;
            return this;
        }

        public IndexersMetaData build() {
            return new IndexersMetaData(indexers.immutableMap(), recoveredFromGateway);
        }

        public static String toXContent(IndexersMetaData metaData) throws IOException {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.startObject();
            toXContent(metaData, builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        }

        public static void toXContent(IndexersMetaData metaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject("meta-data");

            builder.startObject("indexers");
            for (IndexerMetaData indexMetaData : metaData) {
                IndexerMetaData.Builder.toXContent(indexMetaData, builder, params);
            }
            builder.endObject();

            builder.endObject();
        }

        public static IndexersMetaData fromXContent(XContentParser parser, @Nullable Settings globalSettings) throws IOException {
            Builder builder = new Builder();

            XContentParser.Token token = parser.currentToken();
            String currentFieldName = parser.currentName();
            if (!"meta-data".equals(currentFieldName)) {
                token = parser.nextToken();
                currentFieldName = parser.currentName();
                if (token == null) {
                    // no data...
                    return builder.build();
                }
            }

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("indexers".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.put(IndexerMetaData.Builder.fromXContent(parser, globalSettings));
                        }
                    }
                }
            }
            return builder.build();
        }

        public static IndexersMetaData readFrom(StreamInput in, @Nullable Settings globalSettings) throws IOException {
            Builder builder = new Builder();
            // we only serialize it using readFrom, not in to/from XContent
            builder.recoveredFromGateway = in.readBoolean();
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                builder.put(IndexerMetaData.Builder.readFrom(in, globalSettings));
            }
            return builder.build();
        }

        public static void writeTo(IndexersMetaData metaData, StreamOutput out) throws IOException {
            out.writeBoolean(metaData.recoveredFromGateway());
            out.writeVInt(metaData.indexers.size());
            for (IndexerMetaData indexMetaData : metaData) {
                IndexerMetaData.Builder.writeTo(indexMetaData, out);
            }
        }

    }
}
