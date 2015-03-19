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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.common.lucene.search.Queries.fixNegativeQueryIfNeeded;

/**
 * A Query that matches documents matching boolean combinations of other queries.
 */
public class BoolQueryBuilder extends BaseQueryBuilder implements QueryParser, BoostableQueryBuilder<BoolQueryBuilder> {

    public static final String NAME = "bool";

    private ArrayList<QueryBuilder> mustClauses = new ArrayList<>();

    private ArrayList<QueryBuilder> mustNotClauses = new ArrayList<>();

    private ArrayList<QueryBuilder> shouldClauses = new ArrayList<>();

    private float boost = -1;

    private Boolean disableCoord;

    private String minimumShouldMatch;

    private Boolean adjustPureNegative;

    private String queryName;

    @Inject
    public BoolQueryBuilder(Settings settings) {
        BooleanQuery.setMaxClauseCount(settings.getAsInt("index.query.bool.max_clause_count", settings.getAsInt("indices.query.bool.max_clause_count", BooleanQuery.getMaxClauseCount())));
    }

    public BoolQueryBuilder() {
        // for tests
    }

    /**
     * Adds a query that <b>must</b> appear in the matching documents.
     */
    public BoolQueryBuilder must(QueryBuilder queryBuilder) {
        mustClauses.add(queryBuilder);
        return this;
    }

    /**
     * Adds a query that <b>must not</b> appear in the matching documents.
     */
    public BoolQueryBuilder mustNot(QueryBuilder queryBuilder) {
        mustNotClauses.add(queryBuilder);
        return this;
    }

    /**
     * Adds a query that <i>should</i> appear in the matching documents. For a boolean query with no
     * <tt>MUST</tt> clauses one or more <code>SHOULD</code> clauses must match a document
     * for the BooleanQuery to match.
     *
     * @see #minimumNumberShouldMatch(int)
     */
    public BoolQueryBuilder should(QueryBuilder queryBuilder) {
        shouldClauses.add(queryBuilder);
        return this;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @Override
    public BoolQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Disables <tt>Similarity#coord(int,int)</tt> in scoring. Defaults to <tt>false</tt>.
     */
    public BoolQueryBuilder disableCoord(boolean disableCoord) {
        this.disableCoord = disableCoord;
        return this;
    }

    /**
     * Specifies a minimum number of the optional (should) boolean clauses which must be satisfied.
     * <p/>
     * <p>By default no optional clauses are necessary for a match
     * (unless there are no required clauses).  If this method is used,
     * then the specified number of clauses is required.
     * <p/>
     * <p>Use of this method is totally independent of specifying that
     * any specific clauses are required (or prohibited).  This number will
     * only be compared against the number of matching optional clauses.
     *
     * @param minimumNumberShouldMatch the number of optional clauses that must match
     */
    public BoolQueryBuilder minimumNumberShouldMatch(int minimumNumberShouldMatch) {
        this.minimumShouldMatch = Integer.toString(minimumNumberShouldMatch);
        return this;
    }

    /**
     * Sets the minimum should match using the special syntax (for example, supporting percentage).
     */
    public BoolQueryBuilder minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    /**
     * Returns <code>true</code> iff this query builder has at least one should, must or mustNot clause.
     * Otherwise <code>false</code>.
     */
    public boolean hasClauses() {
        return !(mustClauses.isEmpty() && shouldClauses.isEmpty() && mustNotClauses.isEmpty());
    }

    /**
     * If a boolean query contains only negative ("must not") clauses should the
     * BooleanQuery be enhanced with a {@link MatchAllDocsQuery} in order to act
     * as a pure exclude. The default is <code>true</code>.
     */
    public BoolQueryBuilder adjustPureNegative(boolean adjustPureNegative) {
        this.adjustPureNegative = adjustPureNegative;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public BoolQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("bool");
        doXArrayContent("must", mustClauses, builder, params);
        doXArrayContent("must_not", mustNotClauses, builder, params);
        doXArrayContent("should", shouldClauses, builder, params);
        if (boost != -1) {
            builder.field("boost", boost);
        }
        if (disableCoord != null) {
            builder.field("disable_coord", disableCoord);
        }
        if (minimumShouldMatch != null) {
            builder.field("minimum_should_match", minimumShouldMatch);
        }
        if (adjustPureNegative != null) {
            builder.field("adjust_pure_negative", adjustPureNegative);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }

    private void doXArrayContent(String field, List<QueryBuilder> clauses, XContentBuilder builder, Params params) throws IOException {
        if (clauses.isEmpty()) {
            return;
        }
        if (clauses.size() == 1) {
            builder.field(field);
            clauses.get(0).toXContent(builder, params);
        } else {
            builder.startArray(field);
            for (QueryBuilder clause : clauses) {
                clause.toXContent(builder, params);
            }
            builder.endArray();
        }
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        boolean disableCoord = false;
        float boost = 1.0f;
        String minimumShouldMatch = null;

        List<BooleanClause> clauses = newArrayList();
        boolean adjustPureNegative = true;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("must".equals(currentFieldName)) {
                    Query query = parseContext.parseInnerQuery();
                    if (query != null) {
                        clauses.add(new BooleanClause(query, BooleanClause.Occur.MUST));
                    }
                } else if ("must_not".equals(currentFieldName) || "mustNot".equals(currentFieldName)) {
                    Query query = parseContext.parseInnerQuery();
                    if (query != null) {
                        clauses.add(new BooleanClause(query, BooleanClause.Occur.MUST_NOT));
                    }
                } else if ("should".equals(currentFieldName)) {
                    Query query = parseContext.parseInnerQuery();
                    if (query != null) {
                        clauses.add(new BooleanClause(query, BooleanClause.Occur.SHOULD));
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[bool] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("must".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        Query query = parseContext.parseInnerQuery();
                        if (query != null) {
                            clauses.add(new BooleanClause(query, BooleanClause.Occur.MUST));
                        }
                    }
                } else if ("must_not".equals(currentFieldName) || "mustNot".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        Query query = parseContext.parseInnerQuery();
                        if (query != null) {
                            clauses.add(new BooleanClause(query, BooleanClause.Occur.MUST_NOT));
                        }
                    }
                } else if ("should".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        Query query = parseContext.parseInnerQuery();
                        if (query != null) {
                            clauses.add(new BooleanClause(query, BooleanClause.Occur.SHOULD));
                        }
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "bool query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("disable_coord".equals(currentFieldName) || "disableCoord".equals(currentFieldName)) {
                    disableCoord = parser.booleanValue();
                } else if ("minimum_should_match".equals(currentFieldName) || "minimumShouldMatch".equals(currentFieldName)) {
                    minimumShouldMatch = parser.textOrNull();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("minimum_number_should_match".equals(currentFieldName) || "minimumNumberShouldMatch".equals(currentFieldName)) {
                    minimumShouldMatch = parser.textOrNull();
                } else if ("adjust_pure_negative".equals(currentFieldName) || "adjustPureNegative".equals(currentFieldName)) {
                    adjustPureNegative = parser.booleanValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[bool] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (clauses.isEmpty()) {
            return new MatchAllDocsQuery();
        }

        BooleanQuery booleanQuery = new BooleanQuery(disableCoord);
        for (BooleanClause clause : clauses) {
            booleanQuery.add(clause);
        }
        booleanQuery.setBoost(boost);
        Queries.applyMinimumShouldMatch(booleanQuery, minimumShouldMatch);
        Query query = adjustPureNegative ? fixNegativeQueryIfNeeded(booleanQuery) : booleanQuery;
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }
}
