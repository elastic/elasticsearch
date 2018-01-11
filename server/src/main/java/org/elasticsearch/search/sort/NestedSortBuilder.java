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

package org.elasticsearch.search.sort;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.search.sort.SortBuilder.parseNestedFilter;

public class NestedSortBuilder implements Writeable, ToXContentObject {
    public static final ParseField NESTED_FIELD = new ParseField("nested");
    public static final ParseField PATH_FIELD = new ParseField("path");
    public static final ParseField FILTER_FIELD = new ParseField("filter");

    private final String path;
    private QueryBuilder filter;
    private NestedSortBuilder nestedSort;

    public NestedSortBuilder(String path) {
        this.path = path;
    }

    public NestedSortBuilder(StreamInput in) throws IOException {
        path = in.readOptionalString();
        filter = in.readOptionalNamedWriteable(QueryBuilder.class);
        nestedSort = in.readOptionalWriteable(NestedSortBuilder::new);
    }

    public String getPath() {
        return path;
    }

    public QueryBuilder getFilter() {
        return filter;
    }

    public NestedSortBuilder setFilter(final QueryBuilder filter) {
        this.filter = filter;
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
        if (nestedSort != null) {
            builder.field(NESTED_FIELD.getPreferredName(), nestedSort);
        }
        builder.endObject();
        return builder;
    }

    public static NestedSortBuilder fromXContent(XContentParser parser) throws IOException {
        String path = null;
        QueryBuilder filter = null;
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

        return new NestedSortBuilder(path).setFilter(filter).setNestedSort(nestedSort);
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
            && Objects.equals(nestedSort, that.nestedSort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, filter, nestedSort);
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
            return new NestedSortBuilder(this.path).setFilter(rewriteFilter).setNestedSort(rewriteNested);
        } else {
            return this;
        }
    }
}
