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

import org.apache.lucene.queries.BoostingQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * The BoostingQuery class can be used to effectively demote results that match a given query.
 * Unlike the "NOT" clause, this still selects documents that contain undesirable terms,
 * but reduces their overall score:
 * <p>
 * Query balancedQuery = new BoostingQuery(positiveQuery, negativeQuery, 0.01f);
 * In this scenario the positiveQuery contains the mandatory, desirable criteria which is used to
 * select all matching documents, and the negativeQuery contains the undesirable elements which
 * are simply used to lessen the scores. Documents that match the negativeQuery have their score
 * multiplied by the supplied "boost" parameter, so this should be less than 1 to achieve a
 * demoting effect
 */
public class BoostingQueryBuilder extends AbstractQueryBuilder<BoostingQueryBuilder> {

    public static final String NAME = "boosting";

    private final QueryBuilder positiveQuery;

    private final QueryBuilder negativeQuery;

    private float negativeBoost = -1;

    static final BoostingQueryBuilder PROTOTYPE = new BoostingQueryBuilder(EmptyQueryBuilder.PROTOTYPE, EmptyQueryBuilder.PROTOTYPE);

    /**
     * Create a new {@link BoostingQueryBuilder}
     *
     * @param positiveQuery the positive query for this boosting query.
     * @param negativeQuery the negative query for this boosting query.
     */
    public BoostingQueryBuilder(QueryBuilder positiveQuery, QueryBuilder negativeQuery) {
        if (positiveQuery == null) {
            throw new IllegalArgumentException("inner clause [positive] cannot be null.");
        }
        if (negativeQuery == null) {
            throw new IllegalArgumentException("inner clause [negative] cannot be null.");
        }
        this.positiveQuery = positiveQuery;
        this.negativeQuery = negativeQuery;
    }

    /**
     * Get the positive query for this boosting query.
     */
    public QueryBuilder positiveQuery() {
        return this.positiveQuery;
    }

    /**
     * Get the negative query for this boosting query.
     */
    public QueryBuilder negativeQuery() {
        return this.negativeQuery;
    }

    /**
     * Set the negative boost factor.
     */
    public BoostingQueryBuilder negativeBoost(float negativeBoost) {
        if (negativeBoost < 0) {
            throw new IllegalArgumentException("query requires negativeBoost to be set to positive value");
        }
        this.negativeBoost = negativeBoost;
        return this;
    }

    /**
     * Get the negative boost factor.
     */
    public float negativeBoost() {
        return this.negativeBoost;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(BoostingQueryParser.POSITIVE_FIELD.getPreferredName());
        positiveQuery.toXContent(builder, params);
        builder.field(BoostingQueryParser.NEGATIVE_FIELD.getPreferredName());
        negativeQuery.toXContent(builder, params);
        builder.field(BoostingQueryParser.NEGATIVE_BOOST_FIELD.getPreferredName(), negativeBoost);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query positive = positiveQuery.toQuery(context);
        Query negative = negativeQuery.toQuery(context);
        // make upstream queries ignore this query by returning `null`
        // if either inner query builder returns null
        if (positive == null || negative == null) {
            return null;
        }

        return new BoostingQuery(positive, negative, negativeBoost);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(negativeBoost, positiveQuery, negativeQuery);
    }

    @Override
    protected boolean doEquals(BoostingQueryBuilder other) {
        return Objects.equals(negativeBoost, other.negativeBoost) &&
                Objects.equals(positiveQuery, other.positiveQuery) &&
                Objects.equals(negativeQuery, other.negativeQuery);
    }

    @Override
    protected BoostingQueryBuilder doReadFrom(StreamInput in) throws IOException {
        QueryBuilder positiveQuery = in.readQuery();
        QueryBuilder negativeQuery = in.readQuery();
        BoostingQueryBuilder boostingQuery = new BoostingQueryBuilder(positiveQuery, negativeQuery);
        boostingQuery.negativeBoost = in.readFloat();
        return boostingQuery;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeQuery(positiveQuery);
        out.writeQuery(negativeQuery);
        out.writeFloat(negativeBoost);
    }
}
