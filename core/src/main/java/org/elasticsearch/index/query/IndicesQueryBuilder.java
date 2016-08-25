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

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

/**
 * A query that will execute the wrapped query only for the specified indices,
 * and "match_all" when it does not match those indices (by default).
 *
 * @deprecated instead search on the `_index` field
 */
@Deprecated
// TODO remove this class in 6.0
public class IndicesQueryBuilder extends AbstractQueryBuilder<IndicesQueryBuilder> {

    public static final String NAME = "indices";

    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField NO_MATCH_QUERY = new ParseField("no_match_query");
    private static final ParseField INDEX_FIELD = new ParseField("index");
    private static final ParseField INDICES_FIELD = new ParseField("indices");

    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(Loggers.getLogger(IndicesQueryBuilder.class));

    private final QueryBuilder innerQuery;

    private final String[] indices;

    private QueryBuilder noMatchQuery = defaultNoMatchQuery();

    /**
     * @deprecated instead search on the `_index` field
     */
    @Deprecated
    public IndicesQueryBuilder(QueryBuilder innerQuery, String... indices) {
        DEPRECATION_LOGGER.deprecated("{} query is deprecated. Instead search on the '_index' field", NAME);
        if (innerQuery == null) {
            throw new IllegalArgumentException("inner query cannot be null");
        }
        if (indices == null || indices.length == 0) {
            throw new IllegalArgumentException("list of indices cannot be null or empty");
        }
        this.innerQuery = Objects.requireNonNull(innerQuery);
        this.indices = indices;
    }

    /**
     * Read from a stream.
     */
    public IndicesQueryBuilder(StreamInput in) throws IOException {
        super(in);
        innerQuery = in.readNamedWriteable(QueryBuilder.class);
        indices = in.readStringArray();
        noMatchQuery = in.readNamedWriteable(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(innerQuery);
        out.writeStringArray(indices);
        out.writeNamedWriteable(noMatchQuery);
    }

    public QueryBuilder innerQuery() {
        return this.innerQuery;
    }

    public String[] indices() {
        return this.indices;
    }

    /**
     * Sets the query to use when it executes on an index that does not match the indices provided.
     */
    public IndicesQueryBuilder noMatchQuery(QueryBuilder noMatchQuery) {
        if (noMatchQuery == null) {
            throw new IllegalArgumentException("noMatch query cannot be null");
        }
        this.noMatchQuery = noMatchQuery;
        return this;
    }

    /**
     * Sets the no match query, can either be <tt>all</tt> or <tt>none</tt>.
     */
    public IndicesQueryBuilder noMatchQuery(String type) {
        this.noMatchQuery = parseNoMatchQuery(type);
        return this;
    }

    public QueryBuilder noMatchQuery() {
        return this.noMatchQuery;
    }

    private static QueryBuilder defaultNoMatchQuery() {
        return QueryBuilders.matchAllQuery();
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(INDICES_FIELD.getPreferredName(), indices);
        builder.field(QUERY_FIELD.getPreferredName());
        innerQuery.toXContent(builder, params);
        builder.field(NO_MATCH_QUERY.getPreferredName());
        noMatchQuery.toXContent(builder, params);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static Optional<IndicesQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException, ParsingException {
        XContentParser parser = parseContext.parser();

        QueryBuilder innerQuery = null;
        Collection<String> indices = new ArrayList<>();
        QueryBuilder noMatchQuery = defaultNoMatchQuery();

        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, QUERY_FIELD)) {
                    // the 2.0 behaviour when encountering "query" : {} is to return no docs for matching indices
                    innerQuery = parseContext.parseInnerQueryBuilder().orElse(new MatchNoneQueryBuilder());
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, NO_MATCH_QUERY)) {
                    noMatchQuery = parseContext.parseInnerQueryBuilder().orElse(defaultNoMatchQuery());
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[indices] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, INDICES_FIELD)) {
                    if (indices.isEmpty() == false) {
                        throw new ParsingException(parser.getTokenLocation(), "[indices] indices or index already specified");
                    }
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        String value = parser.textOrNull();
                        if (value == null) {
                            throw new ParsingException(parser.getTokenLocation(), "[indices] no value specified for 'indices' entry");
                        }
                        indices.add(value);
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[indices] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, INDEX_FIELD)) {
                    if (indices.isEmpty() == false) {
                        throw new ParsingException(parser.getTokenLocation(), "[indices] indices or index already specified");
                    }
                    indices.add(parser.text());
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, NO_MATCH_QUERY)) {
                    noMatchQuery = parseNoMatchQuery(parser.text());
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[indices] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (innerQuery == null) {
            throw new ParsingException(parser.getTokenLocation(), "[indices] requires 'query' element");
        }
        if (indices.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), "[indices] requires 'indices' or 'index' element");
        }
        return Optional.of(new IndicesQueryBuilder(innerQuery, indices.toArray(new String[indices.size()]))
                .noMatchQuery(noMatchQuery)
                .boost(boost)
                .queryName(queryName));
    }

    static QueryBuilder parseNoMatchQuery(String type) {
        if ("all".equals(type)) {
            return QueryBuilders.matchAllQuery();
        } else if ("none".equals(type)) {
            return new MatchNoneQueryBuilder();
        }
        throw new IllegalArgumentException("query type can only be [all] or [none] but not " + "[" + type + "]");
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        if (context.matchesIndices(indices)) {
            return innerQuery.toQuery(context);
        }
        return noMatchQuery.toQuery(context);
    }

    @Override
    public int doHashCode() {
        return Objects.hash(innerQuery, noMatchQuery, Arrays.hashCode(indices));
    }

    @Override
    protected boolean doEquals(IndicesQueryBuilder other) {
        return Objects.equals(innerQuery, other.innerQuery) &&
                Arrays.equals(indices, other.indices) &&  // otherwise we are comparing pointers
                Objects.equals(noMatchQuery, other.noMatchQuery);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        QueryBuilder newInnnerQuery = innerQuery.rewrite(queryShardContext);
        QueryBuilder newNoMatchQuery = noMatchQuery.rewrite(queryShardContext);
        if (newInnnerQuery != innerQuery || newNoMatchQuery != noMatchQuery) {
            return new IndicesQueryBuilder(innerQuery, indices).noMatchQuery(noMatchQuery);
        }
        return this;
    }
}
