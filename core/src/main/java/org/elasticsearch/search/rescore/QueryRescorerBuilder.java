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

package org.elasticsearch.search.rescore;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.rescore.QueryRescorer.QueryRescoreContext;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class QueryRescorerBuilder extends RescoreBuilder<QueryRescorerBuilder> {

    public static final String NAME = "query";

    public static final float DEFAULT_RESCORE_QUERYWEIGHT = 1.0f;
    public static final float DEFAULT_QUERYWEIGHT = 1.0f;
    public static final QueryRescoreMode DEFAULT_SCORE_MODE = QueryRescoreMode.Total;
    private final QueryBuilder queryBuilder;
    private float rescoreQueryWeight = DEFAULT_RESCORE_QUERYWEIGHT;
    private float queryWeight = DEFAULT_QUERYWEIGHT;
    private QueryRescoreMode scoreMode = DEFAULT_SCORE_MODE;

    private static ParseField RESCORE_QUERY_FIELD = new ParseField("rescore_query");
    private static ParseField QUERY_WEIGHT_FIELD = new ParseField("query_weight");
    private static ParseField RESCORE_QUERY_WEIGHT_FIELD = new ParseField("rescore_query_weight");
    private static ParseField SCORE_MODE_FIELD = new ParseField("score_mode");

    private static final ObjectParser<InnerBuilder, QueryParseContext> QUERY_RESCORE_PARSER = new ObjectParser<>(NAME, null);

    static {
        QUERY_RESCORE_PARSER.declareObject(InnerBuilder::setQueryBuilder, (p, c) -> {
            try {
                return c.parseInnerQueryBuilder().orElse(QueryBuilders.matchAllQuery());
            } catch (IOException e) {
                throw new ParsingException(p.getTokenLocation(), "Could not parse inner query", e);
            }
        } , RESCORE_QUERY_FIELD);
        QUERY_RESCORE_PARSER.declareFloat(InnerBuilder::setQueryWeight, QUERY_WEIGHT_FIELD);
        QUERY_RESCORE_PARSER.declareFloat(InnerBuilder::setRescoreQueryWeight, RESCORE_QUERY_WEIGHT_FIELD);
        QUERY_RESCORE_PARSER.declareString((struct, value) ->  struct.setScoreMode(QueryRescoreMode.fromString(value)), SCORE_MODE_FIELD);
    }

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

    /**
     * @return the query used for this rescore query
     */
    public QueryBuilder getRescoreQuery() {
        return this.queryBuilder;
    }

    /**
     * Sets the original query weight for rescoring. The default is <tt>1.0</tt>
     */
    public QueryRescorerBuilder setQueryWeight(float queryWeight) {
        this.queryWeight = queryWeight;
        return this;
    }


    /**
     * Gets the original query weight for rescoring. The default is <tt>1.0</tt>
     */
    public float getQueryWeight() {
        return this.queryWeight;
    }

    /**
     * Sets the original query weight for rescoring. The default is <tt>1.0</tt>
     */
    public QueryRescorerBuilder setRescoreQueryWeight(float rescoreQueryWeight) {
        this.rescoreQueryWeight = rescoreQueryWeight;
        return this;
    }

    /**
     * Gets the original query weight for rescoring. The default is <tt>1.0</tt>
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
     * Gets the original query score mode. The default is <tt>total</tt>
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

    public static QueryRescorerBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        InnerBuilder innerBuilder = QUERY_RESCORE_PARSER.parse(parseContext.parser(), new InnerBuilder(), parseContext);
        return innerBuilder.build();
    }

    @Override
    public QueryRescoreContext build(QueryShardContext context) throws IOException {
        org.elasticsearch.search.rescore.QueryRescorer rescorer = new org.elasticsearch.search.rescore.QueryRescorer();
        QueryRescoreContext queryRescoreContext = new QueryRescoreContext(rescorer);
        queryRescoreContext.setQuery(QueryBuilder.rewriteQuery(this.queryBuilder, context).toQuery(context));
        queryRescoreContext.setQueryWeight(this.queryWeight);
        queryRescoreContext.setRescoreQueryWeight(this.rescoreQueryWeight);
        queryRescoreContext.setScoreMode(this.scoreMode);
        if (this.windowSize != null) {
            queryRescoreContext.setWindowSize(this.windowSize);
        }
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
        return super.equals(obj) &&
               Objects.equals(scoreMode, other.scoreMode) &&
               Objects.equals(queryWeight, other.queryWeight) &&
               Objects.equals(rescoreQueryWeight, other.rescoreQueryWeight) &&
               Objects.equals(queryBuilder, other.queryBuilder);
    }

    @Override
    public String getWriteableName() {
        return NAME;
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
}
