/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.alias;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;

/**
 * Represents an alias, to be associated with an index
 */
public class Alias implements Writeable, ToXContentFragment {

    private static final ParseField FILTER = new ParseField("filter");
    private static final ParseField ROUTING = new ParseField("routing");
    private static final ParseField INDEX_ROUTING = new ParseField("index_routing", "indexRouting", "index-routing");
    private static final ParseField SEARCH_ROUTING = new ParseField("search_routing", "searchRouting", "search-routing");
    private static final ParseField IS_WRITE_INDEX = new ParseField("is_write_index");
    private static final ParseField IS_HIDDEN = new ParseField("is_hidden");

    private String name;

    @Nullable
    private String filter;

    @Nullable
    private String indexRouting;

    @Nullable
    private String searchRouting;

    @Nullable
    private Boolean writeIndex;

    @Nullable
    private Boolean isHidden;

    public Alias(StreamInput in) throws IOException {
        name = in.readString();
        filter = in.readOptionalString();
        indexRouting = in.readOptionalString();
        searchRouting = in.readOptionalString();
        writeIndex = in.readOptionalBoolean();
        isHidden = in.readOptionalBoolean();
    }

    public Alias(String name) {
        this.name = name;
    }

    /**
     * Returns the alias name
     */
    public String name() {
        return name;
    }

    /**
      Modify the alias name only
     */
    public Alias name(String name){
        this.name = name;
        return this;
    }

    /**
     * Returns the filter associated with the alias
     */
    public String filter() {
        return filter;
    }

    /**
     * Associates a filter to the alias
     */
    public Alias filter(String filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Associates a filter to the alias
     */
    public Alias filter(Map<String, Object> filter) {
        if (filter == null || filter.isEmpty()) {
            this.filter = null;
            return this;
        }
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(filter);
            this.filter = Strings.toString(builder);
            return this;
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + filter + "]", e);
        }
    }

    /**
     * Associates a filter to the alias
     */
    public Alias filter(QueryBuilder filterBuilder) {
        if (filterBuilder == null) {
            this.filter = null;
            return this;
        }
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            filterBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.close();
            this.filter = Strings.toString(builder);
            return this;
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to build json for alias request", e);
        }
    }

    /**
     * Associates a routing value to the alias
     */
    public Alias routing(String routing) {
        this.indexRouting = routing;
        this.searchRouting = routing;
        return this;
    }

    /**
     * Returns the index routing value associated with the alias
     */
    public String indexRouting() {
        return indexRouting;
    }

    /**
     * Associates an index routing value to the alias
     */
    public Alias indexRouting(String indexRouting) {
        this.indexRouting = indexRouting;
        return this;
    }

    /**
     * Returns the search routing value associated with the alias
     */
    public String searchRouting() {
        return searchRouting;
    }

    /**
     * Associates a search routing value to the alias
     */
    public Alias searchRouting(String searchRouting) {
        this.searchRouting = searchRouting;
        return this;
    }

    /**
     * @return the write index flag for the alias
     */
    public Boolean writeIndex() {
        return writeIndex;
    }

    /**
     *  Sets whether an alias is pointing to a write-index
     */
    public Alias writeIndex(@Nullable Boolean writeIndex) {
        this.writeIndex = writeIndex;
        return this;
    }

    /**
     * @return whether this alias is hidden or not
     */
    public Boolean isHidden() {
        return isHidden;
    }

    /**
     * Sets whether this alias is hidden
     */
    public Alias isHidden(@Nullable Boolean isHidden) {
        this.isHidden = isHidden;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeOptionalString(filter);
        out.writeOptionalString(indexRouting);
        out.writeOptionalString(searchRouting);
        out.writeOptionalBoolean(writeIndex);
        out.writeOptionalBoolean(isHidden);
    }

    /**
     * Parses an alias and returns its parsed representation
     */
    public static Alias fromXContent(XContentParser parser) throws IOException {
        Alias alias = new Alias(parser.currentName());

        String currentFieldName = null;
        XContentParser.Token token = parser.nextToken();
        if (token == null) {
            throw new IllegalArgumentException("No alias is specified");
        }
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (FILTER.match(currentFieldName, parser.getDeprecationHandler())) {
                    Map<String, Object> filter = parser.mapOrdered();
                    alias.filter(filter);
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (ROUTING.match(currentFieldName, parser.getDeprecationHandler())) {
                    alias.routing(parser.text());
                } else if (INDEX_ROUTING.match(currentFieldName, parser.getDeprecationHandler())) {
                    alias.indexRouting(parser.text());
                } else if (SEARCH_ROUTING.match(currentFieldName, parser.getDeprecationHandler())) {
                    alias.searchRouting(parser.text());
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if (IS_WRITE_INDEX.match(currentFieldName, parser.getDeprecationHandler())) {
                    alias.writeIndex(parser.booleanValue());
                } else if (IS_HIDDEN.match(currentFieldName, parser.getDeprecationHandler())) {
                    alias.isHidden(parser.booleanValue());
                }
            }
        }
        return alias;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);

        if (filter != null) {
            try (InputStream stream = new BytesArray(filter).streamInput()) {
                builder.rawField(FILTER.getPreferredName(), stream, XContentType.JSON);
            }
        }

        if (indexRouting != null && indexRouting.equals(searchRouting)) {
            builder.field(ROUTING.getPreferredName(), indexRouting);
        } else {
            if (indexRouting != null) {
                builder.field(INDEX_ROUTING.getPreferredName(), indexRouting);
            }
            if (searchRouting != null) {
                builder.field(SEARCH_ROUTING.getPreferredName(), searchRouting);
            }
        }

        builder.field(IS_WRITE_INDEX.getPreferredName(), writeIndex);

        if (isHidden != null) {
            builder.field(IS_HIDDEN.getPreferredName(), isHidden);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Alias alias = (Alias) o;

        return Objects.equals(name, alias.name);
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }
}
