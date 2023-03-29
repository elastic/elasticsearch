/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rescore;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.rescore.QueryRescorer.QueryRescoreContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseTopLevelQuery;

public class QueryRescorerBuilder extends RescorerBuilder<QueryRescorerBuilder> {
    public static final String NAME = "query";

    private static final ParseField RESCORE_QUERY_FIELD = new ParseField("rescore_query");
    private static final ParseField QUERY_WEIGHT_FIELD = new ParseField("query_weight");
    private static final ParseField RESCORE_QUERY_WEIGHT_FIELD = new ParseField("rescore_query_weight");
    private static final ParseField SCORE_MODE_FIELD = new ParseField("score_mode");

    private static final ObjectParser<InnerBuilder, Void> QUERY_RESCORE_PARSER = new ObjectParser<>(NAME);
    static {
        QUERY_RESCORE_PARSER.declareObject(InnerBuilder::setQueryBuilder, (p, c) -> {
            try {
                return parseTopLevelQuery(p);
            } catch (IOException e) {
                throw new ParsingException(p.getTokenLocation(), "Could not parse inner query", e);
            }
        }, RESCORE_QUERY_FIELD);
        QUERY_RESCORE_PARSER.declareFloat(InnerBuilder::setQueryWeight, QUERY_WEIGHT_FIELD);
        QUERY_RESCORE_PARSER.declareFloat(InnerBuilder::setRescoreQueryWeight, RESCORE_QUERY_WEIGHT_FIELD);
        QUERY_RESCORE_PARSER.declareString((struct, value) -> struct.setScoreMode(QueryRescoreMode.fromString(value)), SCORE_MODE_FIELD);
    }

    public static final float DEFAULT_RESCORE_QUERYWEIGHT = 1.0f;
    public static final float DEFAULT_QUERYWEIGHT = 1.0f;
    public static final QueryRescoreMode DEFAULT_SCORE_MODE = QueryRescoreMode.Total;
    private final QueryBuilder queryBuilder;
    private float rescoreQueryWeight = DEFAULT_RESCORE_QUERYWEIGHT;
    private float queryWeight = DEFAULT_QUERYWEIGHT;
    private QueryRescoreMode scoreMode = DEFAULT_SCORE_MODE;

    /**
     * Creates a new {@link QueryRescorerBuilder} instance
     * @param builder the query builder to build the rescore query from
     */
    public QueryRescorerBuilder(QueryBuilder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("rescore_query cannot be null");
        }
        this.queryBuilder = builder;
    }

    /**
     * Read from a stream.
     */
    public QueryRescorerBuilder(StreamInput in) throws IOException {
        super(in);
        queryBuilder = in.readNamedWriteable(QueryBuilder.class);
        scoreMode = QueryRescoreMode.readFromStream(in);
        rescoreQueryWeight = in.readFloat();
        queryWeight = in.readFloat();
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(queryBuilder);
        scoreMode.writeTo(out);
        out.writeFloat(rescoreQueryWeight);
        out.writeFloat(queryWeight);
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
     * @return the query used for this rescore query
     */
    public QueryBuilder getRescoreQuery() {
        return this.queryBuilder;
    }

    /**
     * Sets the original query weight for rescoring. The default is {@code 1.0}
     */
    public QueryRescorerBuilder setQueryWeight(float queryWeight) {
        this.queryWeight = queryWeight;
        return this;
    }

    /**
     * Gets the original query weight for rescoring. The default is {@code 1.0}
     */
    public float getQueryWeight() {
        return this.queryWeight;
    }

    /**
     * Sets the original query weight for rescoring. The default is {@code 1.0}
     */
    public QueryRescorerBuilder setRescoreQueryWeight(float rescoreQueryWeight) {
        this.rescoreQueryWeight = rescoreQueryWeight;
        return this;
    }

    /**
     * Gets the original query weight for rescoring. The default is {@code 1.0}
     */
    public float getRescoreQueryWeight() {
        return this.rescoreQueryWeight;
    }

    /**
     * Sets the original query score mode. The default is {@link QueryRescoreMode#Total}.
     */
    public QueryRescorerBuilder setScoreMode(QueryRescoreMode scoreMode) {
        this.scoreMode = scoreMode;
        return this;
    }

    /**
     * Gets the original query score mode. The default is {@code total}
     */
    public QueryRescoreMode getScoreMode() {
        return this.scoreMode;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(RESCORE_QUERY_FIELD.getPreferredName(), queryBuilder);
        builder.field(QUERY_WEIGHT_FIELD.getPreferredName(), queryWeight);
        builder.field(RESCORE_QUERY_WEIGHT_FIELD.getPreferredName(), rescoreQueryWeight);
        builder.field(SCORE_MODE_FIELD.getPreferredName(), scoreMode.name().toLowerCase(Locale.ROOT));
        builder.endObject();
    }

    public static QueryRescorerBuilder fromXContent(XContentParser parser) throws IOException {
        InnerBuilder innerBuilder = QUERY_RESCORE_PARSER.parse(parser, new InnerBuilder(), null);
        return innerBuilder.build();
    }

    @Override
    public QueryRescoreContext innerBuildContext(int windowSize, SearchExecutionContext context) throws IOException {
        QueryRescoreContext queryRescoreContext = new QueryRescoreContext(windowSize);
        // query is rewritten at this point already
        queryRescoreContext.setQuery(context.toQuery(queryBuilder));
        queryRescoreContext.setQueryWeight(this.queryWeight);
        queryRescoreContext.setRescoreQueryWeight(this.rescoreQueryWeight);
        queryRescoreContext.setScoreMode(this.scoreMode);
        return queryRescoreContext;
    }

    @Override
    public final int hashCode() {
        int result = super.hashCode();
        return 31 * result + Objects.hash(scoreMode, queryWeight, rescoreQueryWeight, queryBuilder);
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        QueryRescorerBuilder other = (QueryRescorerBuilder) obj;
        return super.equals(obj)
            && Objects.equals(scoreMode, other.scoreMode)
            && Objects.equals(queryWeight, other.queryWeight)
            && Objects.equals(rescoreQueryWeight, other.rescoreQueryWeight)
            && Objects.equals(queryBuilder, other.queryBuilder);
    }

    /**
     * Helper to be able to use {@link ObjectParser}, since we need the inner query builder
     * for the constructor of {@link QueryRescorerBuilder}, but {@link ObjectParser} only
     * allows filling properties of an already constructed value.
     */
    private static class InnerBuilder {

        private QueryBuilder queryBuilder;
        private float rescoreQueryWeight = DEFAULT_RESCORE_QUERYWEIGHT;
        private float queryWeight = DEFAULT_QUERYWEIGHT;
        private QueryRescoreMode scoreMode = DEFAULT_SCORE_MODE;

        void setQueryBuilder(QueryBuilder builder) {
            this.queryBuilder = builder;
        }

        QueryRescorerBuilder build() {
            QueryRescorerBuilder queryRescoreBuilder = new QueryRescorerBuilder(queryBuilder);
            queryRescoreBuilder.setQueryWeight(queryWeight);
            queryRescoreBuilder.setRescoreQueryWeight(rescoreQueryWeight);
            queryRescoreBuilder.setScoreMode(scoreMode);
            return queryRescoreBuilder;
        }

        void setQueryWeight(float queryWeight) {
            this.queryWeight = queryWeight;
        }

        void setRescoreQueryWeight(float rescoreQueryWeight) {
            this.rescoreQueryWeight = rescoreQueryWeight;
        }

        void setScoreMode(QueryRescoreMode scoreMode) {
            this.scoreMode = scoreMode;
        }
    }

    @Override
    public QueryRescorerBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        QueryBuilder rewrite = queryBuilder.rewrite(ctx);
        if (rewrite == queryBuilder) {
            return this;
        }
        QueryRescorerBuilder queryRescoreBuilder = new QueryRescorerBuilder(rewrite);
        queryRescoreBuilder.setQueryWeight(queryWeight);
        queryRescoreBuilder.setRescoreQueryWeight(rescoreQueryWeight);
        queryRescoreBuilder.setScoreMode(scoreMode);
        if (windowSize() != null) {
            queryRescoreBuilder.windowSize(windowSize());
        }
        return queryRescoreBuilder;
    }
}
