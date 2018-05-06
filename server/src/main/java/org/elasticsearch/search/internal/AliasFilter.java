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

package org.elasticsearch.search.internal;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Represents a {@link QueryBuilder} and a list of alias names that filters the builder is composed of.
 */
public final class AliasFilter implements Writeable, Rewriteable<AliasFilter>, ToXContentObject {

    public static final AliasFilter EMPTY = new AliasFilter(null, Strings.EMPTY_ARRAY);

    private static final ParseField ALIASES = new ParseField("aliases");
    private static final ParseField FILTER = new ParseField("filter");

    private static final ConstructingObjectParser<AliasFilter, String> PARSER =
        new ConstructingObjectParser<>("alias_filter", false,
            a -> {
                final String[] aliases = (String[]) a[0];
                final QueryBuilder queryBuilder = (QueryBuilder) a[1];
                return new AliasFilter(queryBuilder, aliases);
            });

    static {
        PARSER.declareField(optionalConstructorArg(),
            (p, c) -> p.list().toArray(Strings.EMPTY_ARRAY), ALIASES, ObjectParser.ValueType.STRING_ARRAY);
        PARSER.declareField(optionalConstructorArg(),
            (p, c) -> AbstractQueryBuilder.parseInnerQueryBuilder(p), FILTER, ObjectParser.ValueType.OBJECT);
    }

    private final String[] aliases;
    private final QueryBuilder filter;

    public AliasFilter(QueryBuilder filter, String... aliases) {
        this.aliases = aliases == null ? Strings.EMPTY_ARRAY : aliases;
        this.filter = filter;
    }

    public AliasFilter(StreamInput input) throws IOException {
        aliases = input.readStringArray();
        filter = input.readOptionalNamedWriteable(QueryBuilder.class);
    }

    @Override
    public AliasFilter rewrite(QueryRewriteContext context) throws IOException {
        QueryBuilder queryBuilder = this.filter;
        if (queryBuilder != null) {
            QueryBuilder rewrite = Rewriteable.rewrite(queryBuilder, context);
            if (rewrite != queryBuilder) {
                return new AliasFilter(rewrite, aliases);
            }
        }
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(aliases);
        out.writeOptionalNamedWriteable(filter);
    }

    /**
     * Returns the aliases patters that are used to compose the {@link QueryBuilder}
     * returned from {@link #getQueryBuilder()}
     */
    public String[] getAliases() {
        return aliases;
    }

    /**
     * Returns the alias filter {@link QueryBuilder} or <code>null</code> if there is no such filter
     */
    public QueryBuilder getQueryBuilder() {
        return filter;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (aliases.length > 0) {
            final String[] copy = Arrays.copyOf(aliases, aliases.length);
            Arrays.sort(copy); // we want consistent ordering here and these values might be generated from a set / map
            builder.array(ALIASES, copy);
            if (filter != null) { // might be null if we include non-filtering aliases
                builder.field(FILTER, filter, params);
            }
        }
        return builder;
    }

    public static AliasFilter fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AliasFilter that = (AliasFilter) o;
        return Arrays.equals(aliases, that.aliases) &&
            Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(aliases), filter);
    }

    @Override
    public String toString() {
        return "AliasFilter{" +
            "aliases=" + Arrays.toString(aliases) +
            ", filter=" + filter +
            '}';
    }
}
