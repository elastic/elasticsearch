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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Matches spans which are near one another. One can specify slop, the maximum number
 * of intervening unmatched positions, as well as whether matches are required to be in-order.
 * The span near query maps to Lucene {@link SpanNearQuery}.
 */
public class SpanNearQueryBuilder extends AbstractQueryBuilder<SpanNearQueryBuilder> implements SpanQueryBuilder {

    public static final String NAME = "span_near";
    public static final ParseField QUERY_NAME_FIELD = new ParseField(NAME);

    /** Default for flag controlling whether matches are required to be in-order */
    public static boolean DEFAULT_IN_ORDER = true;

    private static final ParseField SLOP_FIELD = new ParseField("slop");
    private static final ParseField COLLECT_PAYLOADS_FIELD = new ParseField("collect_payloads").withAllDeprecated("no longer supported");
    private static final ParseField CLAUSES_FIELD = new ParseField("clauses");
    private static final ParseField IN_ORDER_FIELD = new ParseField("in_order");

    private final List<SpanQueryBuilder> clauses = new ArrayList<>();

    private final int slop;

    private boolean inOrder = DEFAULT_IN_ORDER;

    /**
     * @param initialClause an initial span query clause
     * @param slop controls the maximum number of intervening unmatched positions permitted
     */
    public SpanNearQueryBuilder(SpanQueryBuilder initialClause, int slop) {
        if (initialClause == null) {
            throw new IllegalArgumentException("[" + NAME + "] must include at least one clause");
        }
        this.clauses.add(initialClause);
        this.slop = slop;
    }

    /**
     * Read from a stream.
     */
    public SpanNearQueryBuilder(StreamInput in) throws IOException {
        super(in);
        for (QueryBuilder clause : readQueries(in)) {
            this.clauses.add((SpanQueryBuilder) clause);
        }
        slop = in.readVInt();
        inOrder = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        writeQueries(out, clauses);
        out.writeVInt(slop);
        out.writeBoolean(inOrder);
    }

    /**
     * @return the maximum number of intervening unmatched positions permitted
     */
    public int slop() {
        return this.slop;
    }

    /**
     * Add a span clause to the current list of clauses
     */
    public SpanNearQueryBuilder addClause(SpanQueryBuilder clause) {
        if (clause == null) {
            throw new IllegalArgumentException("[" + NAME + "]  clauses cannot be null");
        }
        clauses.add(clause);
        return this;
    }

    /**
     * @return the {@link SpanQueryBuilder} clauses that were set for this query
     */
    public List<SpanQueryBuilder> clauses() {
        return Collections.unmodifiableList(this.clauses);
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

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray(CLAUSES_FIELD.getPreferredName());
        for (SpanQueryBuilder clause : clauses) {
            clause.toXContent(builder, params);
        }
        builder.endArray();
        builder.field(SLOP_FIELD.getPreferredName(), slop);
        builder.field(IN_ORDER_FIELD.getPreferredName(), inOrder);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static Optional<SpanNearQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        Integer slop = null;
        boolean inOrder = SpanNearQueryBuilder.DEFAULT_IN_ORDER;
        String queryName = null;

        List<SpanQueryBuilder> clauses = new ArrayList<>();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, CLAUSES_FIELD)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        Optional<QueryBuilder> query = parseContext.parseInnerQueryBuilder();
                        if (query.isPresent() == false || query.get() instanceof SpanQueryBuilder == false) {
                            throw new ParsingException(parser.getTokenLocation(), "spanNear [clauses] must be of type span query");
                        }
                        clauses.add((SpanQueryBuilder) query.get());
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_near] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, IN_ORDER_FIELD)) {
                    inOrder = parser.booleanValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, COLLECT_PAYLOADS_FIELD)) {
                    // Deprecated in 3.0.0
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, SLOP_FIELD)) {
                    slop = parser.intValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_near] query does not support [" + currentFieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[span_near] query does not support [" + currentFieldName + "]");
            }
        }

        if (clauses.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), "span_near must include [clauses]");
        }

        if (slop == null) {
            throw new ParsingException(parser.getTokenLocation(), "span_near must include [slop]");
        }

        SpanNearQueryBuilder queryBuilder = new SpanNearQueryBuilder(clauses.get(0), slop);
        for (int i = 1; i < clauses.size(); i++) {
            queryBuilder.addClause(clauses.get(i));
        }
        queryBuilder.inOrder(inOrder);
        queryBuilder.boost(boost);
        queryBuilder.queryName(queryName);
        return Optional.of(queryBuilder);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        SpanQuery[] spanQueries = new SpanQuery[clauses.size()];
        for (int i = 0; i < clauses.size(); i++) {
            Query query = clauses.get(i).toQuery(context);
            assert query instanceof SpanQuery;
            spanQueries[i] = (SpanQuery) query;
        }
        return new SpanNearQuery(spanQueries, slop, inOrder);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(clauses, slop, inOrder);
    }

    @Override
    protected boolean doEquals(SpanNearQueryBuilder other) {
        return Objects.equals(clauses, other.clauses) &&
               Objects.equals(slop, other.slop) &&
               Objects.equals(inOrder, other.inOrder);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
