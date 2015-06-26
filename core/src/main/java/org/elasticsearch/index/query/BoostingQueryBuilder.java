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
import org.apache.lucene.queries.BoostingQuery;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * The BoostingQuery class can be used to effectively demote results that match a given query.
 * Unlike the "NOT" clause, this still selects documents that contain undesirable terms,
 * but reduces their overall score:
 * <p/>
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

    static final BoostingQueryBuilder PROTOTYPE = new BoostingQueryBuilder();

    /**
     * this constructor only used for prototype
     */
    private BoostingQueryBuilder() {
        this.positiveQuery = null;
        this.negativeQuery = null;
    }

    /**
     * Create a new {@link BoostingQueryBuilder}
     *
     * @param positiveQuery the positive query for this boosting query.
     * @param negativeQuery the negative query for this boosting query.
     */
    public BoostingQueryBuilder(QueryBuilder positiveQuery, QueryBuilder negativeQuery) {
        this.positiveQuery = Objects.requireNonNull(positiveQuery);
        this.negativeQuery = Objects.requireNonNull(negativeQuery);
    }

    /**
     * Get the positive query for this boosting query.
     */
    public QueryBuilder positive() {
        return this.positiveQuery;
    }

    /**
     * Get the negative query for this boosting query.
     */
    public QueryBuilder negative() {
        return this.negativeQuery;
    }

    /**
     * Set the negative boost factor.
     */
    public BoostingQueryBuilder negativeBoost(float negativeBoost) {
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
        builder.field("positive");
        positiveQuery.toXContent(builder, params);
        builder.field("negative");
        negativeQuery.toXContent(builder, params);
        builder.field("negative_boost", negativeBoost);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    public QueryValidationException validate() {
        QueryValidationException validationException = null;
        if (negativeBoost < 0) {
            validationException = addValidationError("query requires negativeBoost to be set to positive value", validationException);
        }
        validationException = validateInnerQuery(negativeQuery, validationException);
        validationException = validateInnerQuery(positiveQuery, validationException);
        return validationException;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryParseContext parseContext) throws IOException {
        Query positive = positiveQuery.toQuery(parseContext);
        Query negative = negativeQuery.toQuery(parseContext);
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
        QueryBuilder positiveQuery = in.readNamedWriteable();
        QueryBuilder negativeQuery = in.readNamedWriteable();
        BoostingQueryBuilder boostingQuery = new BoostingQueryBuilder(positiveQuery, negativeQuery);
        boostingQuery.negativeBoost = in.readFloat();
        return boostingQuery;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(positiveQuery);
        out.writeNamedWriteable(negativeQuery);
        out.writeFloat(negativeBoost);
    }
}
