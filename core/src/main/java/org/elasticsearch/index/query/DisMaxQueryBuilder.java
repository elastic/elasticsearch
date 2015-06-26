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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * A query that generates the union of documents produced by its sub-queries, and that scores each document
 * with the maximum score for that document as produced by any sub-query, plus a tie breaking increment for any
 * additional matching sub-queries.
 */
public class DisMaxQueryBuilder extends AbstractQueryBuilder<DisMaxQueryBuilder> implements BoostableQueryBuilder<DisMaxQueryBuilder> {

    public static final String NAME = "dis_max";

    private final ArrayList<QueryBuilder> queries = new ArrayList<>();

    private float boost = 1.0f;

    /** Default multiplication factor for breaking ties in document scores.*/
    public static float DEFAULT_TIE_BREAKER = 0.0f;
    private float tieBreaker = DEFAULT_TIE_BREAKER;

    private String queryName;

    static final DisMaxQueryBuilder PROTOTYPE = new DisMaxQueryBuilder();

    /**
     * Add a sub-query to this disjunction.
     */
    public DisMaxQueryBuilder add(QueryBuilder queryBuilder) {
        queries.add(Objects.requireNonNull(queryBuilder));
        return this;
    }

    /**
     * @return an immutable list copy of the current sub-queries of this disjunction
     */
    public List<QueryBuilder> queries() {
        return this.queries;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @Override
    public DisMaxQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * @return the boost for this query
     */
    public float boost() {
        return this.boost;
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

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public DisMaxQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    /**
     * @return the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public String queryName() {
        return this.queryName;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("tie_breaker", tieBreaker);
        builder.field("boost", boost);
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.startArray("queries");
        for (QueryBuilder queryBuilder : queries) {
            queryBuilder.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
    }

    @Override
    public Query toQuery(QueryParseContext parseContext) throws QueryParsingException, IOException {
        // return null if there are no queries at all
        Collection<Query> luceneQueries = toQueries(queries, parseContext);
        if (luceneQueries.isEmpty()) {
            return null;
        }

        DisjunctionMaxQuery query = new DisjunctionMaxQuery(luceneQueries, tieBreaker);
        query.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }

    @Override
    public QueryValidationException validate() {
        return validateInnerQueries(queries, null);
    }

    @Override
    public DisMaxQueryBuilder readFrom(StreamInput in) throws IOException {
        DisMaxQueryBuilder disMax = new DisMaxQueryBuilder();
        List<QueryBuilder> queryBuilders = in.readNamedWriteableList();
        disMax.queries.addAll(queryBuilders);
        disMax.tieBreaker = in.readFloat();
        disMax.queryName = in.readOptionalString();
        disMax.boost = in.readFloat();
        return disMax;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableList(queries);
        out.writeFloat(tieBreaker);
        out.writeOptionalString(queryName);
        out.writeFloat(boost);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queries, tieBreaker, boost, queryName);
    }

    @Override
    public boolean doEquals(DisMaxQueryBuilder other) {
        return Objects.equals(queries, other.queries) &&
               Objects.equals(tieBreaker, other.tieBreaker) &&
               Objects.equals(boost, other.boost) &&
               Objects.equals(queryName, other.queryName);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
