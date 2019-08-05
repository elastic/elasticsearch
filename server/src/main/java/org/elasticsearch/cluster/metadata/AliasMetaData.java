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

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;

public class AliasMetaData extends AbstractDiffable<AliasMetaData> implements ToXContentFragment {

    private final String alias;

    private final CompressedXContent filter;

    private final String indexRouting;

    private final String searchRouting;

    private final Set<String> searchRoutingValues;

    @Nullable
    private final Boolean writeIndex;

    private AliasMetaData(String alias, CompressedXContent filter, String indexRouting, String searchRouting, Boolean writeIndex) {
        this.alias = alias;
        this.filter = filter;
        this.indexRouting = indexRouting;
        this.searchRouting = searchRouting;
        if (searchRouting != null) {
            searchRoutingValues = Collections.unmodifiableSet(Sets.newHashSet(Strings.splitStringByCommaToArray(searchRouting)));
        } else {
            searchRoutingValues = emptySet();
        }
        this.writeIndex = writeIndex;
    }

    private AliasMetaData(AliasMetaData aliasMetaData, String alias) {
        this(alias, aliasMetaData.filter(), aliasMetaData.indexRouting(), aliasMetaData.searchRouting(), aliasMetaData.writeIndex());
    }

    public String alias() {
        return alias;
    }

    public String getAlias() {
        return alias();
    }

    public CompressedXContent filter() {
        return filter;
    }

    public CompressedXContent getFilter() {
        return filter();
    }

    public boolean filteringRequired() {
        return filter != null;
    }

    public String getSearchRouting() {
        return searchRouting();
    }

    public String searchRouting() {
        return searchRouting;
    }

    public String getIndexRouting() {
        return indexRouting();
    }

    public String indexRouting() {
        return indexRouting;
    }

    public Set<String> searchRoutingValues() {
        return searchRoutingValues;
    }

    public Boolean writeIndex() {
        return writeIndex;
    }

    public static Builder builder(String alias) {
        return new Builder(alias);
    }

    public static Builder newAliasMetaDataBuilder(String alias) {
        return new Builder(alias);
    }

    /**
     * Creates a new AliasMetaData instance with same content as the given one, but with a different alias name
     */
    public static AliasMetaData newAliasMetaData(AliasMetaData aliasMetaData, String newAlias) {
        return new AliasMetaData(aliasMetaData, newAlias);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final AliasMetaData that = (AliasMetaData) o;

        if (alias != null ? !alias.equals(that.alias) : that.alias != null) return false;
        if (filter != null ? !filter.equals(that.filter) : that.filter != null) return false;
        if (indexRouting != null ? !indexRouting.equals(that.indexRouting) : that.indexRouting != null) return false;
        if (searchRouting != null ? !searchRouting.equals(that.searchRouting) : that.searchRouting != null) return false;
        if (writeIndex != null ? writeIndex != that.writeIndex : that.writeIndex != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = alias != null ? alias.hashCode() : 0;
        result = 31 * result + (filter != null ? filter.hashCode() : 0);
        result = 31 * result + (indexRouting != null ? indexRouting.hashCode() : 0);
        result = 31 * result + (searchRouting != null ? searchRouting.hashCode() : 0);
        result = 31 * result + (writeIndex != null ? writeIndex.hashCode() : 0);
        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(alias());
        if (filter() != null) {
            out.writeBoolean(true);
            filter.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        if (indexRouting() != null) {
            out.writeBoolean(true);
            out.writeString(indexRouting());
        } else {
            out.writeBoolean(false);
        }
        if (searchRouting() != null) {
            out.writeBoolean(true);
            out.writeString(searchRouting());
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalBoolean(writeIndex());
    }

    public AliasMetaData(StreamInput in) throws IOException {
        alias = in.readString();
        if (in.readBoolean()) {
            filter = CompressedXContent.readCompressedString(in);
        } else {
            filter = null;
        }
        if (in.readBoolean()) {
            indexRouting = in.readString();
        } else {
            indexRouting = null;
        }
        if (in.readBoolean()) {
            searchRouting = in.readString();
            searchRoutingValues = Collections.unmodifiableSet(Sets.newHashSet(Strings.splitStringByCommaToArray(searchRouting)));
        } else {
            searchRouting = null;
            searchRoutingValues = emptySet();
        }
        writeIndex = in.readOptionalBoolean();
    }

    public static Diff<AliasMetaData> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(AliasMetaData::new, in);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        AliasMetaData.Builder.toXContent(this, builder, params);
        return builder;
    }

    public static class Builder {

        private final String alias;

        private CompressedXContent filter;

        private String indexRouting;

        private String searchRouting;

        @Nullable
        private Boolean writeIndex;


        public Builder(String alias) {
            this.alias = alias;
        }

        public String alias() {
            return alias;
        }

        public Builder filter(CompressedXContent filter) {
            this.filter = filter;
            return this;
        }

        public Builder filter(String filter) {
            if (!Strings.hasLength(filter)) {
                this.filter = null;
                return this;
            }
            return filter(XContentHelper.convertToMap(XContentFactory.xContent(filter), filter, true));
        }

        public Builder filter(Map<String, Object> filter) {
            if (filter == null || filter.isEmpty()) {
                this.filter = null;
                return this;
            }
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder().map(filter);
                this.filter = new CompressedXContent(BytesReference.bytes(builder));
                return this;
            } catch (IOException e) {
                throw new ElasticsearchGenerationException("Failed to build json for alias request", e);
            }
        }

        public Builder routing(String routing) {
            this.indexRouting = routing;
            this.searchRouting = routing;
            return this;
        }

        public Builder indexRouting(String indexRouting) {
            this.indexRouting = indexRouting;
            return this;
        }

        public Builder searchRouting(String searchRouting) {
            this.searchRouting = searchRouting;
            return this;
        }

        public Builder writeIndex(@Nullable Boolean writeIndex) {
            this.writeIndex = writeIndex;
            return this;
        }

        public AliasMetaData build() {
            return new AliasMetaData(alias, filter, indexRouting, searchRouting, writeIndex);
        }

        public static void toXContent(AliasMetaData aliasMetaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject(aliasMetaData.alias());

            boolean binary = params.paramAsBoolean("binary", false);

            if (aliasMetaData.filter() != null) {
                if (binary) {
                    builder.field("filter", aliasMetaData.filter.compressed());
                } else {
                    builder.field("filter", XContentHelper.convertToMap(new BytesArray(aliasMetaData.filter().uncompressed()), true).v2());
                }
            }
            if (aliasMetaData.indexRouting() != null) {
                builder.field("index_routing", aliasMetaData.indexRouting());
            }
            if (aliasMetaData.searchRouting() != null) {
                builder.field("search_routing", aliasMetaData.searchRouting());
            }

            if (aliasMetaData.writeIndex() != null) {
                builder.field("is_write_index", aliasMetaData.writeIndex());
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
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                    if ("filter".equals(currentFieldName)) {
                        builder.filter(new CompressedXContent(parser.binaryValue()));
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if ("routing".equals(currentFieldName)) {
                        builder.routing(parser.text());
                    } else if ("index_routing".equals(currentFieldName) || "indexRouting".equals(currentFieldName)) {
                        builder.indexRouting(parser.text());
                    } else if ("search_routing".equals(currentFieldName) || "searchRouting".equals(currentFieldName)) {
                        builder.searchRouting(parser.text());
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    parser.skipChildren();
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    if ("is_write_index".equals(currentFieldName)) {
                        builder.writeIndex(parser.booleanValue());
                    }
                }
            }
            return builder.build();
        }
    }
}
