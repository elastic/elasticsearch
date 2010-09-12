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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.indexer.IndexerName;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class IndexerMetaData {

    private final IndexerName indexerName;

    private final Settings settings;

    private IndexerMetaData(IndexerName indexerName, Settings settings) {
        this.indexerName = indexerName;
        this.settings = settings;
    }

    public IndexerName indexerName() {
        return indexerName;
    }

    public Settings settings() {
        return settings;
    }

    public static class Builder {

        private IndexerName indexerName;

        private Settings settings = ImmutableSettings.Builder.EMPTY_SETTINGS;

        public Builder(IndexerName indexerName) {
            this.indexerName = indexerName;
        }

        public Builder settings(Settings.Builder settings) {
            this.settings = settings.build();
            return this;
        }

        public Builder settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public IndexerMetaData build() {
            return new IndexerMetaData(indexerName, settings);
        }

        public static void toXContent(IndexerMetaData indexerMetaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject(indexerMetaData.indexerName().name());

            builder.field("type", indexerMetaData.indexerName().type());

            builder.startObject("settings");
            for (Map.Entry<String, String> entry : indexerMetaData.settings().getAsMap().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();

            builder.endObject();
        }

        public static IndexerMetaData fromXContent(XContentParser parser, @Nullable Settings globalSettings) throws IOException {
            String name = parser.currentName();
            ImmutableSettings.Builder settingsBuilder = null;
            String type = null;

            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("settings".equals(currentFieldName)) {
                        settingsBuilder = ImmutableSettings.settingsBuilder().globalSettings(globalSettings);
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            String key = parser.currentName();
                            token = parser.nextToken();
                            String value = parser.text();
                            settingsBuilder.put(key, value);
                        }
                    }
                } else if (token.isValue()) {
                    if ("type".equals(currentFieldName)) {
                        type = parser.text();
                    }
                }
            }
            Builder builder = new Builder(new IndexerName(name, type));
            if (settingsBuilder != null) {
                builder.settings(settingsBuilder);
            }
            return builder.build();
        }

        public static IndexerMetaData readFrom(StreamInput in, Settings globalSettings) throws IOException {
            Builder builder = new Builder(new IndexerName(in.readUTF(), in.readUTF()));
            builder.settings(ImmutableSettings.readSettingsFromStream(in, globalSettings));
            return builder.build();
        }

        public static void writeTo(IndexerMetaData indexerMetaData, StreamOutput out) throws IOException {
            out.writeUTF(indexerMetaData.indexerName().type());
            out.writeUTF(indexerMetaData.indexerName().name());
            ImmutableSettings.writeSettingsToStream(indexerMetaData.settings(), out);
        }
    }
}
