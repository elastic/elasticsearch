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

import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A query that generates the union of documents produced by its sub-queries, and that scores each document
 * with the maximum score for that document as produced by any sub-query, plus a tie breaking increment for any
 * additional matching sub-queries.
 */
public class DisMaxQueryBuilder extends AbstractQueryBuilder<DisMaxQueryBuilder> {
    public static final String NAME = "dis_max";

    /** Default multiplication factor for breaking ties in document scores.*/
    public static final float DEFAULT_TIE_BREAKER = 0.0f;

    private static final ParseField TIE_BREAKER_FIELD = new ParseField("tie_breaker");
    private static final ParseField QUERIES_FIELD = new ParseField("queries");

    private final List<QueryBuilder> queries = new ArrayList<>();

    private float tieBreaker = DEFAULT_TIE_BREAKER;

    public DisMaxQueryBuilder() {
    }

    /**
     * Read from a stream.
     */
    public DisMaxQueryBuilder(StreamInput in) throws IOException {
        super(in);
        queries.addAll(readQueries(in));
        tieBreaker = in.readFloat();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        writeQueries(out, queries);
        out.writeFloat(tieBreaker);
    }

    /**
     * Add a sub-query to this disjunction.
     */
    public DisMaxQueryBuilder add(QueryBuilder queryBuilder) {
        if (queryBuilder == null) {
            throw new IllegalArgumentException("inner dismax query clause cannot be null");
        }
        queries.add(queryBuilder);
        return this;
    }

    /**
     * @return an immutable list copy of the current sub-queries of this disjunction
     */
    public List<QueryBuilder> innerQueries() {
        return this.queries;
    }

    /**
     * The score of each non-maximum disjunct for a document is multiplied by this weight
     * and added into the final score.  If non-zero, the value should be small, on the order of 0.1, which says that
     * 10 occurrences of word in a lower-scored field that is also in a higher scored field is just as good as a unique
     * word in the lower scored field (i.e., one that is not in any higher scored field.
     */
    public DisMaxQueryBuilder tieBreaker(float tieBreaker) {
        this.tieBreaker = tieBreaker;
        return this;
    }

    /**
     * @return the tie breaker score
     * @see DisMaxQueryBuilder#tieBreaker(float)
     */
    public float tieBreaker() {
        return this.tieBreaker;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(TIE_BREAKER_FIELD.getPreferredName(), tieBreaker);
        builder.startArray(QUERIES_FIELD.getPreferredName());
        for (QueryBuilder queryBuilder : queries) {
            queryBuilder.toXContent(builder, params);
        }
        builder.endArray();
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static DisMaxQueryBuilder fromXContent(XContentParser parser) throws IOException {
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        float tieBreaker = DisMaxQueryBuilder.DEFAULT_TIE_BREAKER;

        final List<QueryBuilder> queries = new ArrayList<>();
        boolean queriesFound = false;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (QUERIES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queriesFound = true;
                    queries.add(parseInnerQueryBuilder(parser));
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[dis_max] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (QUERIES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queriesFound = true;
                    while (token != XContentParser.Token.END_ARRAY) {
                        queries.add(parseInnerQueryBuilder(parser));
                        token = parser.nextToken();
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[dis_max] query does not support [" + currentFieldName + "]");
                }
            } else {
                if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (TIE_BREAKER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    tieBreaker = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[dis_max] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (!queriesFound) {
            throw new ParsingException(parser.getTokenLocation(), "[dis_max] requires 'queries' field with at least one clause");
        }

        DisMaxQueryBuilder disMaxQuery = new DisMaxQueryBuilder();
        disMaxQuery.tieBreaker(tieBreaker);
        disMaxQuery.queryName(queryName);
        disMaxQuery.boost(boost);
        for (QueryBuilder query : queries) {
            disMaxQuery.add(query);
        }
        return disMaxQuery;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        // return null if there are no queries at all
        Collection<Query> luceneQueries = toQueries(queries, context);
        if (luceneQueries.isEmpty()) {
            return Queries.newMatchNoDocsQuery("no clauses for dismax query.");
        }

        return new DisjunctionMaxQuery(luceneQueries, tieBreaker);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        DisMaxQueryBuilder newBuilder = new DisMaxQueryBuilder();
        boolean changed = false;
        for (QueryBuilder query : queries) {
            QueryBuilder result = query.rewrite(queryShardContext);
            if (result != query) {
                changed = true;
            }
            newBuilder.add(result);
        }
        if (changed) {
            newBuilder.queryName(queryName);
            newBuilder.boost(boost);
            newBuilder.tieBreaker(tieBreaker);
            return newBuilder;
        } else {
            return this;
        }
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(queries, tieBreaker);
    }

    @Override
    protected boolean doEquals(DisMaxQueryBuilder other) {
        return Objects.equals(queries, other.queries) &&
               Objects.equals(tieBreaker, other.tieBreaker);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {
        for (QueryBuilder query : queries) {
            InnerHitContextBuilder.extractInnerHits(query, innerHits);
        }
    }
}
