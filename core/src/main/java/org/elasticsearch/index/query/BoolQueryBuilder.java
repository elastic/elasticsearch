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
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.lucene.search.Queries.fixNegativeQueryIfNeeded;

/**
 * A Query that matches documents matching boolean combinations of other queries.
 */
public class BoolQueryBuilder extends AbstractQueryBuilder<BoolQueryBuilder> implements BoostableQueryBuilder<BoolQueryBuilder> {

    public static final String NAME = "bool";

    static final boolean ADJUST_PURE_NEGATIVE_DEFAULT = true;

    static final boolean DISABLE_COORD_DEFAULT = false;

    static final BoolQueryBuilder PROTOTYPE = new BoolQueryBuilder();

    private final List<QueryBuilder> mustClauses = new ArrayList<>();

    private final List<QueryBuilder> mustNotClauses = new ArrayList<>();

    private final List<QueryBuilder> filterClauses = new ArrayList<>();

    private final List<QueryBuilder> shouldClauses = new ArrayList<>();

    private float boost = 1.0f;

    private boolean disableCoord = DISABLE_COORD_DEFAULT;

    private boolean adjustPureNegative = ADJUST_PURE_NEGATIVE_DEFAULT;

    private String minimumShouldMatch;

    private String queryName;

    /**
     * Adds a query that <b>must</b> appear in the matching documents and will
     * contribute to scoring.
     */
    public BoolQueryBuilder must(QueryBuilder queryBuilder) {
        mustClauses.add(queryBuilder);
        return this;
    }

    /**
     * Gets the queries that <b>must</b> appear in the matching documents.
     */
    public List<QueryBuilder> must() {
        return this.mustClauses;
    }

    /**
     * Adds a query that <b>must</b> appear in the matching documents but will
     * not contribute to scoring.
     */
    public BoolQueryBuilder filter(QueryBuilder queryBuilder) {
        filterClauses.add(queryBuilder);
        return this;
    }

    /**
     * Gets the queries that <b>must</b> appear in the matching documents but don't conntribute to scoring
     */
    public List<QueryBuilder> filter() {
        return this.filterClauses;
    }

    /**
     * Adds a query that <b>must not</b> appear in the matching documents.
     */
    public BoolQueryBuilder mustNot(QueryBuilder queryBuilder) {
        mustNotClauses.add(queryBuilder);
        return this;
    }

    /**
     * Gets the queries that <b>must not</b> appear in the matching documents.
     */
    public List<QueryBuilder> mustNot() {
        return this.mustNotClauses;
    }

    /**
     * Adds a clause that <i>should</i> be matched by the returned documents. For a boolean query with no
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
     * Gets the list of clauses that <b>should</b> be matched by the returned documents.
     *
     * @see #should(QueryBuilder)
     *  @see #minimumNumberShouldMatch(int)
     */
    public List<QueryBuilder> should() {
        return this.shouldClauses;
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
     * Get the boost for this query.
     */
    public float boost() {
        return this.boost;
    }

    /**
     * Disables <tt>Similarity#coord(int,int)</tt> in scoring. Defaults to <tt>false</tt>.
     */
    public BoolQueryBuilder disableCoord(boolean disableCoord) {
        this.disableCoord = disableCoord;
        return this;
    }

    /**
     * @return whether the <tt>Similarity#coord(int,int)</tt> in scoring are disabled. Defaults to <tt>false</tt>.
     */
    public boolean disableCoord() {
        return this.disableCoord;
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
     * Specifies a minimum number of the optional (should) boolean clauses which must be satisfied.
     * @see BoolQueryBuilder#minimumNumberShouldMatch(int)
     */
    public BoolQueryBuilder minimumNumberShouldMatch(String minimumNumberShouldMatch) {
        this.minimumShouldMatch = minimumNumberShouldMatch;
        return this;
    }

    /**
     * @return the string representation of the minimumShouldMatch settings for this query
     */
    public String minimumNumberShouldMatch() {
        return this.minimumShouldMatch;
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
     * @return the setting for the adjust_pure_negative setting in this query
     */
    public boolean adjustPureNegative() {
        return this.adjustPureNegative;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public BoolQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    /**
     * Gets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public String queryName() {
        return this.queryName;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        doXArrayContent("must", mustClauses, builder, params);
        doXArrayContent("filter", filterClauses, builder, params);
        doXArrayContent("must_not", mustNotClauses, builder, params);
        doXArrayContent("should", shouldClauses, builder, params);
        builder.field("boost", boost);
        builder.field("disable_coord", disableCoord);
        builder.field("adjust_pure_negative", adjustPureNegative);
        if (minimumShouldMatch != null) {
            builder.field("minimum_should_match", minimumShouldMatch);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }

    private static void doXArrayContent(String field, List<QueryBuilder> clauses, XContentBuilder builder, Params params) throws IOException {
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
    public String getName() {
        return NAME;
    }

    @Override
    public Query toQuery(QueryParseContext parseContext) throws QueryParsingException, IOException {
        if (!hasClauses()) {
            return new MatchAllDocsQuery();
        }

        BooleanQuery booleanQuery = new BooleanQuery(this.disableCoord);
        addBooleanClauses(parseContext, booleanQuery, this.mustClauses, BooleanClause.Occur.MUST);
        addBooleanClauses(parseContext, booleanQuery, this.mustNotClauses, BooleanClause.Occur.MUST_NOT);
        addBooleanClauses(parseContext, booleanQuery, this.shouldClauses, BooleanClause.Occur.SHOULD);
        addBooleanClauses(parseContext, booleanQuery, this.filterClauses, BooleanClause.Occur.FILTER);

        booleanQuery.setBoost(this.boost);
        Queries.applyMinimumShouldMatch(booleanQuery, this.minimumShouldMatch);
        Query query = this.adjustPureNegative ? fixNegativeQueryIfNeeded(booleanQuery) : booleanQuery;
        if (this.queryName != null) {
            parseContext.addNamedQuery(this.queryName, query);
        }
        return query;
    }

    @Override
    public QueryValidationException validate() {
        // nothing to validate, clauses are optional, see hasClauses(), other parameters have defaults
        return null;
    }

    private static void addBooleanClauses(QueryParseContext parseContext, BooleanQuery booleanQuery, List<QueryBuilder> clauses, Occur occurs)
            throws IOException {
        for (QueryBuilder query : clauses) {
            Query luceneQuery = query.toQuery(parseContext);
            if (luceneQuery != null) {
                booleanQuery.add(new BooleanClause(luceneQuery, occurs));
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(boost, adjustPureNegative, disableCoord,
                minimumShouldMatch, queryName, mustClauses, shouldClauses, mustNotClauses, filterClauses);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        BoolQueryBuilder other = (BoolQueryBuilder) obj;
        return Objects.equals(boost, other.boost) &&
                Objects.equals(adjustPureNegative, other.adjustPureNegative) &&
                Objects.equals(disableCoord, other.disableCoord) &&
                Objects.equals(minimumShouldMatch, other.minimumShouldMatch) &&
                Objects.equals(queryName, other.queryName) &&
                Objects.equals(mustClauses, other.mustClauses) &&
                Objects.equals(shouldClauses, other.shouldClauses) &&
                Objects.equals(mustNotClauses, other.mustNotClauses) &&
                Objects.equals(filterClauses, other.filterClauses);
    }

    @Override
    public BoolQueryBuilder readFrom(StreamInput in) throws IOException {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        List<QueryBuilder> queryBuilders = in.readNamedWritableList();
        boolQueryBuilder.mustClauses.addAll(queryBuilders);
        queryBuilders = in.readNamedWritableList();
        boolQueryBuilder.mustNotClauses.addAll(queryBuilders);
        queryBuilders = in.readNamedWritableList();
        boolQueryBuilder.shouldClauses.addAll(queryBuilders);
        queryBuilders = in.readNamedWritableList();
        boolQueryBuilder.filterClauses.addAll(queryBuilders);
        boolQueryBuilder.boost = in.readFloat();
        boolQueryBuilder.adjustPureNegative = in.readBoolean();
        boolQueryBuilder.disableCoord = in.readBoolean();
        boolQueryBuilder.queryName = in.readOptionalString();
        boolQueryBuilder.minimumShouldMatch = in.readOptionalString();
        return boolQueryBuilder;

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWritableList(this.mustClauses);
        out.writeNamedWritableList(this.mustNotClauses);
        out.writeNamedWritableList(this.shouldClauses);
        out.writeNamedWritableList(this.filterClauses);
        out.writeFloat(this.boost);
        out.writeBoolean(this.adjustPureNegative);
        out.writeBoolean(this.disableCoord);
        out.writeOptionalString(queryName);
        out.writeOptionalString(this.minimumShouldMatch);
    }
}
