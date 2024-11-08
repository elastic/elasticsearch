/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.sort;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.search.sort.SortBuilder.parseNestedFilter;

public class NestedSortBuilder implements Writeable, ToXContentObject {
    public static final ParseField NESTED_FIELD = new ParseField("nested");
    public static final ParseField PATH_FIELD = new ParseField("path");
    public static final ParseField FILTER_FIELD = new ParseField("filter");
    public static final ParseField MAX_CHILDREN_FIELD = new ParseField("max_children");

    private final String path;
    private QueryBuilder filter;
    private int maxChildren = Integer.MAX_VALUE;
    private NestedSortBuilder nestedSort;

    public NestedSortBuilder(String path) {
        this.path = path;
    }

    public NestedSortBuilder(StreamInput in) throws IOException {
        path = in.readOptionalString();
        filter = in.readOptionalNamedWriteable(QueryBuilder.class);
        nestedSort = in.readOptionalWriteable(NestedSortBuilder::new);
        maxChildren = in.readVInt();
    }

    public String getPath() {
        return path;
    }

    public QueryBuilder getFilter() {
        return filter;
    }

    public int getMaxChildren() {
        return maxChildren;
    }

    public NestedSortBuilder setFilter(final QueryBuilder filter) {
        this.filter = filter;
        return this;
    }

    public NestedSortBuilder setMaxChildren(final int maxChildren) {
        this.maxChildren = maxChildren;
        return this;
    }

    public NestedSortBuilder getNestedSort() {
        return nestedSort;
    }

    public NestedSortBuilder setNestedSort(final NestedSortBuilder nestedSortBuilder) {
        this.nestedSort = nestedSortBuilder;
        return this;
    }

    /**
     * Write this object's fields to a {@linkplain StreamOutput}.
     */
    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeOptionalString(path);
        out.writeOptionalNamedWriteable(filter);
        out.writeOptionalWriteable(nestedSort);
        out.writeVInt(maxChildren);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        if (path != null) {
            builder.field(PATH_FIELD.getPreferredName(), path);
        }
        if (filter != null) {
            builder.field(FILTER_FIELD.getPreferredName(), filter);
        }

        if (maxChildren != Integer.MAX_VALUE) {
            builder.field(MAX_CHILDREN_FIELD.getPreferredName(), maxChildren);
        }

        if (nestedSort != null) {
            builder.field(NESTED_FIELD.getPreferredName(), nestedSort);
        }
        builder.endObject();
        return builder;
    }

    public static NestedSortBuilder fromXContent(XContentParser parser) throws IOException {
        String path = null;
        QueryBuilder filter = null;
        int maxChildren = Integer.MAX_VALUE;
        NestedSortBuilder nestedSort = null;

        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_OBJECT) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String currentName = parser.currentName();
                    parser.nextToken();
                    if (currentName.equals(PATH_FIELD.getPreferredName())) {
                        path = parser.text();
                    } else if (currentName.equals(FILTER_FIELD.getPreferredName())) {
                        filter = parseNestedFilter(parser);
                    } else if (currentName.equals(MAX_CHILDREN_FIELD.getPreferredName())) {
                        maxChildren = parser.intValue();
                    } else if (currentName.equals(NESTED_FIELD.getPreferredName())) {
                        nestedSort = NestedSortBuilder.fromXContent(parser);
                    } else {
                        throw new IllegalArgumentException("malformed nested sort format, unknown field name [" + currentName + "]");
                    }
                } else {
                    throw new IllegalArgumentException("malformed nested sort format, only field names are allowed");
                }
            }
        } else {
            throw new IllegalArgumentException("malformed nested sort format, must start with an object");
        }

        return new NestedSortBuilder(path).setFilter(filter).setMaxChildren(maxChildren).setNestedSort(nestedSort);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        NestedSortBuilder that = (NestedSortBuilder) obj;
        return Objects.equals(path, that.path)
            && Objects.equals(filter, that.filter)
            && Objects.equals(maxChildren, that.maxChildren)
            && Objects.equals(nestedSort, that.nestedSort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, filter, nestedSort, maxChildren);
    }

    public NestedSortBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        if (filter == null && nestedSort == null) {
            return this;
        }
        QueryBuilder rewriteFilter = this.filter;
        NestedSortBuilder rewriteNested = this.nestedSort;
        if (filter != null) {
            rewriteFilter = filter.rewrite(ctx);
        }
        if (nestedSort != null) {
            rewriteNested = nestedSort.rewrite(ctx);
        }
        if (rewriteFilter != this.filter || rewriteNested != this.nestedSort) {
            return new NestedSortBuilder(this.path).setFilter(rewriteFilter).setMaxChildren(this.maxChildren).setNestedSort(rewriteNested);
        } else {
            return this;
        }
    }
}
