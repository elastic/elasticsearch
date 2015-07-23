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
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Matches spans which are near one another. One can specify slop, the maximum number
 * of intervening unmatched positions, as well as whether matches are required to be in-order.
 * The span near query maps to Lucene {@link SpanNearQuery}.
 */
public class SpanNearQueryBuilder extends AbstractQueryBuilder<SpanNearQueryBuilder> implements SpanQueryBuilder<SpanNearQueryBuilder> {

    public static final String NAME = "span_near";

    /** Default for flag controlling whether matches are required to be in-order */
    public static boolean DEFAULT_IN_ORDER = true;

    /** Default for flag controlling whether payloads are collected */
    public static boolean DEFAULT_COLLECT_PAYLOADS = true;

    private final ArrayList<SpanQueryBuilder> clauses = new ArrayList<>();

    private final int slop;

    private boolean inOrder = DEFAULT_IN_ORDER;

    private boolean collectPayloads = DEFAULT_COLLECT_PAYLOADS;

    static final SpanNearQueryBuilder PROTOTYPE = new SpanNearQueryBuilder();

    /**
     * @param slop controls the maximum number of intervening unmatched positions permitted
     */
    public SpanNearQueryBuilder(int slop) {
        this.slop = slop;
    }

    /**
     * only used for prototype
     */
    private SpanNearQueryBuilder() {
        this.slop = 0;
    }

    /**
     * @return the maximum number of intervening unmatched positions permitted
     */
    public int slop() {
        return this.slop;
    }

    public SpanNearQueryBuilder clause(SpanQueryBuilder clause) {
        clauses.add(clause);
        return this;
    }

    /**
     * @return the {@link SpanQueryBuilder} clauses that were set for this query
     */
    public List<SpanQueryBuilder> clauses() {
        return this.clauses;
    }

    /**
     * When <code>inOrder</code> is true, the spans from each clause
     * must be in the same order as in <code>clauses</code> and must be non-overlapping.
     * Defaults to <code>true</code>
     */
    public SpanNearQueryBuilder inOrder(boolean inOrder) {
        this.inOrder = inOrder;
        return this;
    }

    /**
     * @see SpanNearQueryBuilder#inOrder(boolean))
     */
    public boolean inOrder() {
        return this.inOrder;
    }

    /**
     * @param collectPayloads flag controlling whether payloads are collected
     */
    public SpanNearQueryBuilder collectPayloads(boolean collectPayloads) {
        this.collectPayloads = collectPayloads;
        return this;
    }

    /**
     * @see SpanNearQueryBuilder#collectPayloads(boolean))
     */
    public boolean collectPayloads() {
        return this.collectPayloads;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray("clauses");
        for (SpanQueryBuilder clause : clauses) {
            clause.toXContent(builder, params);
        }
        builder.endArray();
        builder.field("slop", slop);
        builder.field("in_order", inOrder);
        builder.field("collect_payloads", collectPayloads);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryParseContext parseContext) throws IOException {
        SpanQuery[] spanQueries = new SpanQuery[clauses.size()];
        for (int i = 0; i < clauses.size(); i++) {
            Query query = clauses.get(i).toQuery(parseContext);
            assert query instanceof SpanQuery;
            spanQueries[i] = (SpanQuery) query;
        }
        return new SpanNearQuery(spanQueries, slop, inOrder, collectPayloads);
    }

    @Override
    public QueryValidationException validate() {
        QueryValidationException validationException = null;
        if (clauses.isEmpty()) {
            validationException =  addValidationError("query must include [clauses]", validationException);
        }
        for (SpanQueryBuilder innerClause : clauses) {
            if (innerClause == null) {
                validationException =  addValidationError("[clauses] contains null element", validationException);
            } else {
                validationException = validateInnerQuery(innerClause, validationException);
            }
        }
        return validationException;
    }

    @Override
    protected SpanNearQueryBuilder doReadFrom(StreamInput in) throws IOException {
        SpanNearQueryBuilder queryBuilder = new SpanNearQueryBuilder(in.readVInt());
        List<SpanQueryBuilder> clauses = in.readNamedWriteableList();
        for (SpanQueryBuilder subClause : clauses) {
            queryBuilder.clause(subClause);
        }
        queryBuilder.collectPayloads = in.readBoolean();
        queryBuilder.inOrder = in.readBoolean();
        return queryBuilder;

    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(slop);
        out.writeNamedWriteableList(clauses);
        out.writeBoolean(collectPayloads);
        out.writeBoolean(inOrder);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(clauses, slop, collectPayloads, inOrder);
    }

    @Override
    protected boolean doEquals(SpanNearQueryBuilder other) {
        return Objects.equals(clauses, other.clauses) &&
               Objects.equals(slop, other.slop) &&
               Objects.equals(collectPayloads, other.collectPayloads) &&
               Objects.equals(inOrder, other.inOrder);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
