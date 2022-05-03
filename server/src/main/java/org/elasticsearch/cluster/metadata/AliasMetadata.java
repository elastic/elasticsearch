/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptySet;

public class AliasMetadata implements SimpleDiffable<AliasMetadata>, ToXContentFragment {

    private final String alias;

    private final CompressedXContent filter;

    private final String indexRouting;

    private final String searchRouting;

    private final Set<String> searchRoutingValues;

    @Nullable
    private final Boolean writeIndex;

    @Nullable
    private final Boolean isHidden;

    private AliasMetadata(
        String alias,
        CompressedXContent filter,
        String indexRouting,
        String searchRouting,
        Boolean writeIndex,
        @Nullable Boolean isHidden
    ) {
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
        this.isHidden = isHidden;
    }

    private AliasMetadata(AliasMetadata aliasMetadata, String alias) {
        this(
            alias,
            aliasMetadata.filter(),
            aliasMetadata.indexRouting(),
            aliasMetadata.searchRouting(),
            aliasMetadata.writeIndex(),
            aliasMetadata.isHidden
        );
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

    @Nullable
    public Boolean isHidden() {
        return isHidden;
    }

    public static Builder builder(String alias) {
        return new Builder(alias);
    }

    public static Builder newAliasMetadataBuilder(String alias) {
        return new Builder(alias);
    }

    /**
     * Creates a new AliasMetadata instance with same content as the given one, but with a different alias name
     */
    public static AliasMetadata newAliasMetadata(AliasMetadata aliasMetadata, String newAlias) {
        return new AliasMetadata(aliasMetadata, newAlias);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final AliasMetadata that = (AliasMetadata) o;

        if (Objects.equals(alias, that.alias) == false) return false;
        if (Objects.equals(filter, that.filter) == false) return false;
        if (Objects.equals(indexRouting, that.indexRouting) == false) return false;
        if (Objects.equals(searchRouting, that.searchRouting) == false) return false;
        if (Objects.equals(writeIndex, that.writeIndex) == false) return false;
        if (Objects.equals(isHidden, that.isHidden) == false) return false;

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
        out.writeOptionalBoolean(isHidden);
    }

    public AliasMetadata(StreamInput in) throws IOException {
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
        isHidden = in.readOptionalBoolean();
    }

    public static Diff<AliasMetadata> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(AliasMetadata::new, in);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        AliasMetadata.Builder.toXContent(this, builder, params);
        return builder;
    }

    public static AliasMetadata getFirstAliasMetadata(Metadata metadata, IndexAbstraction ia) {
        if (ia.getType() != IndexAbstraction.Type.ALIAS) {
            throw new IllegalArgumentException("unexpected type: [" + ia.getType() + "]");
        }

        IndexMetadata firstIndex = metadata.index(ia.getIndices().get(0));
        return firstIndex.getAliases().get(ia.getName());
    }

    public static class Builder {

        private final String alias;

        private CompressedXContent filter;

        private String indexRouting;

        private String searchRouting;

        @Nullable
        private Boolean writeIndex;

        @Nullable
        private Boolean isHidden;

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
            if (Strings.hasLength(filter) == false) {
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
                this.filter = new CompressedXContent(filter);
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

        public Builder isHidden(@Nullable Boolean isHidden) {
            this.isHidden = isHidden;
            return this;
        }

        public AliasMetadata build() {
            return new AliasMetadata(alias, filter, indexRouting, searchRouting, writeIndex, isHidden);
        }

        public static void toXContent(AliasMetadata aliasMetadata, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject(aliasMetadata.alias());

            boolean binary = params.paramAsBoolean("binary", false);

            if (aliasMetadata.filter() != null) {
                if (binary) {
                    builder.field("filter", aliasMetadata.filter.compressed());
                } else {
                    aliasMetadata.filter.copyTo(builder.field("filter"));
                }
            }
            if (aliasMetadata.indexRouting() != null) {
                builder.field("index_routing", aliasMetadata.indexRouting());
            }
            if (aliasMetadata.searchRouting() != null) {
                builder.field("search_routing", aliasMetadata.searchRouting());
            }

            if (aliasMetadata.writeIndex() != null) {
                builder.field("is_write_index", aliasMetadata.writeIndex());
            }

            if (aliasMetadata.isHidden != null) {
                builder.field("is_hidden", aliasMetadata.isHidden());
            }

            builder.endObject();
        }

        public static AliasMetadata fromXContent(XContentParser parser) throws IOException {
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
                    } else if ("filter".equals(currentFieldName)) {
                        builder.filter(new CompressedXContent(parser.binaryValue()));
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    parser.skipChildren();
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    if ("is_write_index".equals(currentFieldName)) {
                        builder.writeIndex(parser.booleanValue());
                    } else if ("is_hidden".equals(currentFieldName)) {
                        builder.isHidden(parser.booleanValue());
                    }
                }
            }
            return builder.build();
        }
    }
}
