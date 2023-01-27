/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.index.query.SpanQueryBuilder.SpanQueryBuilderUtil.checkNoBoost;

/**
 * Matches spans which are near one another. One can specify slop, the maximum number
 * of intervening unmatched positions, as well as whether matches are required to be in-order.
 * The span near query maps to Lucene {@link SpanNearQuery}.
 */
public class SpanNearQueryBuilder extends AbstractQueryBuilder<SpanNearQueryBuilder> implements SpanQueryBuilder {
    public static final String NAME = "span_near";

    /** Default for flag controlling whether matches are required to be in-order */
    public static boolean DEFAULT_IN_ORDER = true;
    /** Default slop value, this is the same that lucene {@link SpanNearQuery} uses if no slop is provided */
    public static int DEFAULT_SLOP = 0;

    private static final ParseField SLOP_FIELD = new ParseField("slop");
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
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    public static SpanNearQueryBuilder fromXContent(XContentParser parser) throws IOException {
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        int slop = DEFAULT_SLOP;
        boolean inOrder = DEFAULT_IN_ORDER;
        String queryName = null;

        List<SpanQueryBuilder> clauses = new ArrayList<>();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (CLAUSES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        QueryBuilder query = parseInnerQueryBuilder(parser);
                        if (query instanceof SpanQueryBuilder == false) {
                            throw new ParsingException(parser.getTokenLocation(), "span_near [clauses] must be of type span query");
                        }
                        final SpanQueryBuilder clause = (SpanQueryBuilder) query;
                        checkNoBoost(NAME, currentFieldName, parser, clause);
                        clauses.add(clause);
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_near] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (IN_ORDER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    inOrder = parser.booleanValue();
                } else if (SLOP_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    slop = parser.intValue();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
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

        SpanNearQueryBuilder queryBuilder = new SpanNearQueryBuilder(clauses.get(0), slop);
        for (int i = 1; i < clauses.size(); i++) {
            queryBuilder.addClause(clauses.get(i));
        }
        queryBuilder.inOrder(inOrder);
        queryBuilder.boost(boost);
        queryBuilder.queryName(queryName);
        return queryBuilder;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        SpanQueryBuilder queryBuilder = clauses.get(0);
        boolean isGap = queryBuilder instanceof SpanGapQueryBuilder;
        Query query = null;
        if (isGap == false) {
            query = queryBuilder.toQuery(context);
            assert query instanceof SpanQuery;
        }
        if (clauses.size() == 1) {
            assert isGap == false;
            return query;
        }
        String spanNearFieldName = null;
        if (isGap) {
            String fieldName = ((SpanGapQueryBuilder) queryBuilder).fieldName();
            spanNearFieldName = queryFieldName(context, fieldName);
        } else {
            spanNearFieldName = ((SpanQuery) query).getField();
        }

        SpanNearQuery.Builder builder = new SpanNearQuery.Builder(spanNearFieldName, inOrder);
        builder.setSlop(slop);
        /*
         * Lucene SpanNearQuery throws exceptions for certain use cases like adding gap to a
         * unordered SpanNearQuery. Should ES have the same checks or wrap those thrown exceptions?
         */
        if (isGap) {
            int gap = ((SpanGapQueryBuilder) queryBuilder).width();
            builder.addGap(gap);
        } else {
            builder.addClause((SpanQuery) query);
        }

        for (int i = 1; i < clauses.size(); i++) {
            queryBuilder = clauses.get(i);
            isGap = queryBuilder instanceof SpanGapQueryBuilder;
            if (isGap) {
                String fieldName = ((SpanGapQueryBuilder) queryBuilder).fieldName();
                String spanGapFieldName = queryFieldName(context, fieldName);

                if (spanNearFieldName.equals(spanGapFieldName) == false) {
                    throw new IllegalArgumentException("[span_near] clauses must have same field");
                }
                int gap = ((SpanGapQueryBuilder) queryBuilder).width();
                builder.addGap(gap);
            } else {
                query = clauses.get(i).toQuery(context);
                assert query instanceof SpanQuery;
                builder.addClause((SpanQuery) query);
            }
        }
        return builder.build();
    }

    private static String queryFieldName(SearchExecutionContext context, String fieldName) {
        MappedFieldType fieldType = context.getFieldType(fieldName);
        return fieldType != null ? fieldType.name() : fieldName;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(clauses, slop, inOrder);
    }

    @Override
    protected boolean doEquals(SpanNearQueryBuilder other) {
        return Objects.equals(clauses, other.clauses) && Objects.equals(slop, other.slop) && Objects.equals(inOrder, other.inOrder);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.ZERO;
    }

    /**
     * SpanGapQueryBuilder enables gaps in a SpanNearQuery.
     * Since, SpanGapQuery is private to SpanNearQuery, SpanGapQueryBuilder cannot
     * be used to generate a Query (SpanGapQuery) like another QueryBuilder.
     * Instead, it just identifies a span_gap clause so that SpanNearQuery.addGap(int)
     * can be invoked for it.
     * This QueryBuilder is only applicable as a clause in SpanGapQueryBuilder but
     * yet to enforce this restriction.
     */
    public static class SpanGapQueryBuilder implements SpanQueryBuilder {
        public static final String NAME = "span_gap";

        /** Name of field to match against. */
        private final String fieldName;

        /** Width of the gap introduced. */
        private final int width;

        /**
         * Constructs a new SpanGapQueryBuilder term query.
         *
         * @param fieldName  The name of the field
         * @param width The width of the gap introduced
         */
        public SpanGapQueryBuilder(String fieldName, int width) {
            if (Strings.isEmpty(fieldName)) {
                throw new IllegalArgumentException("[span_gap] field name is null or empty");
            }
            // lucene has not coded any restriction on value of width.
            // to-do : find if theoretically it makes sense to apply restrictions.
            this.fieldName = fieldName;
            this.width = width;
        }

        /**
         * Read from a stream.
         */
        public SpanGapQueryBuilder(StreamInput in) throws IOException {
            fieldName = in.readString();
            width = in.readInt();
        }

        /**
         * @return fieldName  The name of the field
         */
        public String fieldName() {
            return fieldName;
        }

        /**
         * @return width The width of the gap introduced
         */
        public int width() {
            return width;
        }

        @Override
        public Query toQuery(SearchExecutionContext context) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String queryName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public QueryBuilder queryName(String queryName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public float boost() {
            throw new UnsupportedOperationException();
        }

        @Override
        public QueryBuilder boost(float boost) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.ZERO;
        }

        @Override
        public final void writeTo(StreamOutput out) throws IOException {
            out.writeString(fieldName);
            out.writeInt(width);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject(getName());
            builder.field(fieldName, width);
            builder.endObject();
            builder.endObject();
            return builder;
        }

        public static SpanGapQueryBuilder fromXContent(XContentParser parser) throws IOException {
            String fieldName = null;
            int width = 0;
            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                    throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, currentFieldName);
                    fieldName = currentFieldName;
                } else if (token.isValue()) {
                    width = parser.intValue();
                }
            }
            SpanGapQueryBuilder result = new SpanGapQueryBuilder(fieldName, width);
            return result;
        }

        @Override
        public final boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            SpanGapQueryBuilder other = (SpanGapQueryBuilder) obj;
            return Objects.equals(fieldName, other.fieldName) && Objects.equals(width, other.width);
        }

        @Override
        public final int hashCode() {
            return Objects.hash(getClass(), fieldName, width);
        }

        @Override
        public final String toString() {
            return Strings.toString(this, true, true);
        }

        // copied from AbstractQueryBuilder
        protected static void throwParsingExceptionOnMultipleFields(
            String queryName,
            XContentLocation contentLocation,
            String processedFieldName,
            String currentFieldName
        ) {
            if (processedFieldName != null) {
                throw new ParsingException(
                    contentLocation,
                    "["
                        + queryName
                        + "] query doesn't support multiple fields, found ["
                        + processedFieldName
                        + "] and ["
                        + currentFieldName
                        + "]"
                );
            }
        }
    }
}
