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

import org.elasticsearch.ElasticSearchGenerationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.Immutable;
import org.elasticsearch.common.xcontent.*;

import java.io.IOException;
import java.util.Map;

/**
 * @author imotov
 */
@Immutable
public class AliasMetaData {

    private final String alias;

    private final CompressedString filter;

    private AliasMetaData(String alias, CompressedString filter) {
        this.alias = alias;
        this.filter = filter;
    }

    public String alias() {
        return alias;
    }

    public String getAlias() {
        return alias();
    }

    public CompressedString filter() {
        return filter;
    }

    public CompressedString getFilter() {
        return filter();
    }

    public static Builder newAliasMetaDataBuilder(String alias) {
        return new Builder(alias);
    }

    public static class Builder {

        private String alias;

        private CompressedString filter;

        public Builder(String alias) {
            this.alias = alias;
        }

        public Builder(AliasMetaData aliasMetaData) {
            this(aliasMetaData.alias());
            filter = aliasMetaData.filter();
        }

        public String alias() {
            return alias;
        }

        public Builder filter(String filter) {
            if (!Strings.hasLength(filter)) {
                this.filter = null;
                return this;
            }
            try {
                XContentParser parser = XContentFactory.xContent(filter).createParser(filter);
                try {
                    filter(parser.mapOrdered());
                } finally {
                    parser.close();
                }
                return this;
            } catch (IOException e) {
                throw new ElasticSearchGenerationException("Failed to generate [" + filter + "]", e);
            }
        }

        public Builder filter(Map<String, Object> filter) {
            if (filter == null || filter.isEmpty()) {
                this.filter = null;
                return this;
            }
            try {
                this.filter = new CompressedString(XContentFactory.jsonBuilder().map(filter).string());
                return this;
            } catch (IOException e) {
                throw new ElasticSearchGenerationException("Failed to build json for alias request", e);
            }
        }

        public Builder filter(XContentBuilder filterBuilder) {
            try {
                return filter(filterBuilder.string());
            } catch (IOException e) {
                throw new ElasticSearchGenerationException("Failed to build json for alias request", e);
            }
        }

        public AliasMetaData build() {
            return new AliasMetaData(alias, filter);
        }

        public static void toXContent(AliasMetaData aliasMetaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject(aliasMetaData.alias(), XContentBuilder.FieldCaseConversion.NONE);

            if (aliasMetaData.filter() != null) {
                byte[] data = aliasMetaData.filter().uncompressed();
                XContentParser parser = XContentFactory.xContent(data).createParser(data);
                Map<String, Object> filter = parser.mapOrdered();
                parser.close();
                builder.field("filter", filter);
            }

            builder.endObject();
        }

        public static AliasMetaData fromXContent(XContentParser parser) throws IOException {
            Builder builder = new Builder(parser.currentName());

            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken();
            if (token == null) {
                // no data...
                return builder.build();
            }
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("filter".equals(currentFieldName)) {
                        Map<String, Object> filter = parser.mapOrdered();
                        builder.filter(filter);
                    }
                }
            }
            return builder.build();
        }

        public static void writeTo(AliasMetaData aliasMetaData, StreamOutput out) throws IOException {
            out.writeUTF(aliasMetaData.alias());
            if (aliasMetaData.filter() != null) {
                out.writeBoolean(true);
                aliasMetaData.filter.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }

        public static AliasMetaData readFrom(StreamInput in) throws IOException {
            String alias = in.readUTF();
            CompressedString filter = null;
            if (in.readBoolean()) {
                filter = CompressedString.readCompressedString(in);
            }
            return new AliasMetaData(alias, filter);
        }
    }

}
