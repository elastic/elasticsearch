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
public class DisMaxQueryBuilder extends AbstractQueryBuilder<DisMaxQueryBuilder> {

    public static final String NAME = "dis_max";

    private final ArrayList<QueryBuilder> queries = new ArrayList<>();

    /** Default multiplication factor for breaking ties in document scores.*/
    public static float DEFAULT_TIE_BREAKER = 0.0f;
    private float tieBreaker = DEFAULT_TIE_BREAKER;

    static final DisMaxQueryBuilder PROTOTYPE = new DisMaxQueryBuilder();

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
        builder.field(DisMaxQueryParser.TIE_BREAKER_FIELD.getPreferredName(), tieBreaker);
        builder.startArray(DisMaxQueryParser.QUERIES_FIELD.getPreferredName());
        for (QueryBuilder queryBuilder : queries) {
            queryBuilder.toXContent(builder, params);
        }
        builder.endArray();
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        // return null if there are no queries at all
        Collection<Query> luceneQueries = toQueries(queries, context);
        if (luceneQueries.isEmpty()) {
            return null;
        }

        return new DisjunctionMaxQuery(luceneQueries, tieBreaker);
    }

    @Override
    protected DisMaxQueryBuilder doReadFrom(StreamInput in) throws IOException {
        DisMaxQueryBuilder disMax = new DisMaxQueryBuilder();
        List<QueryBuilder> queryBuilders = readQueries(in);
        disMax.queries.addAll(queryBuilders);
        disMax.tieBreaker = in.readFloat();
        return disMax;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        writeQueries(out, queries);
        out.writeFloat(tieBreaker);
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
}
