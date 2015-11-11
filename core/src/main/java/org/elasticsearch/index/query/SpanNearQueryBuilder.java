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

    private final List<SpanQueryBuilder> clauses = new ArrayList<>();

    private final int slop;

    private boolean inOrder = DEFAULT_IN_ORDER;

    private boolean collectPayloads = DEFAULT_COLLECT_PAYLOADS;

    static final SpanNearQueryBuilder PROTOTYPE = new SpanNearQueryBuilder(SpanTermQueryBuilder.PROTOTYPE, 0);

    /**
     * @param initialClause an initial span query clause
     * @param slop controls the maximum number of intervening unmatched positions permitted
     */
    public SpanNearQueryBuilder(SpanQueryBuilder initialClause, int slop) {
        if (initialClause == null) {
            throw new IllegalArgumentException("query must include at least one clause");
        }
        this.clauses.add(initialClause);
        this.slop = slop;
    }

    /**
     * @return the maximum number of intervening unmatched positions permitted
     */
    public int slop() {
        return this.slop;
    }

    public SpanNearQueryBuilder clause(SpanQueryBuilder clause) {
        if (clause == null) {
            throw new IllegalArgumentException("query clauses cannot be null");
        }
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
     * @see SpanNearQueryBuilder#inOrder(boolean)
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
     * @see SpanNearQueryBuilder#collectPayloads(boolean)
     */
    public boolean collectPayloads() {
        return this.collectPayloads;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray(SpanNearQueryParser.CLAUSES_FIELD.getPreferredName());
        for (SpanQueryBuilder clause : clauses) {
            clause.toXContent(builder, params);
        }
        builder.endArray();
        builder.field(SpanNearQueryParser.SLOP_FIELD.getPreferredName(), slop);
        builder.field(SpanNearQueryParser.IN_ORDER_FIELD.getPreferredName(), inOrder);
        builder.field(SpanNearQueryParser.COLLECT_PAYLOADS_FIELD.getPreferredName(), collectPayloads);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        SpanQuery[] spanQueries = new SpanQuery[clauses.size()];
        for (int i = 0; i < clauses.size(); i++) {
            Query query = clauses.get(i).toQuery(context);
            assert query instanceof SpanQuery;
            spanQueries[i] = (SpanQuery) query;
        }
        return new SpanNearQuery(spanQueries, slop, inOrder, collectPayloads);
    }

    @Override
    protected SpanNearQueryBuilder doReadFrom(StreamInput in) throws IOException {
        List<QueryBuilder> clauses = readQueries(in);
        SpanNearQueryBuilder queryBuilder = new SpanNearQueryBuilder((SpanQueryBuilder)clauses.get(0), in.readVInt());
        for (int i = 1; i < clauses.size(); i++) {
            queryBuilder.clauses.add((SpanQueryBuilder)clauses.get(i));
        }
        queryBuilder.collectPayloads = in.readBoolean();
        queryBuilder.inOrder = in.readBoolean();
        return queryBuilder;

    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        writeQueries(out, clauses);
        out.writeVInt(slop);
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
    public String getWriteableName() {
        return NAME;
    }
}
