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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class RescoreBuilder implements ToXContent, Writeable<RescoreBuilder> {

    private Rescorer rescorer;
    private Integer windowSize;
    public static final RescoreBuilder PROTOYPE = new RescoreBuilder(new QueryRescorer(new MatchAllQueryBuilder()));

    public RescoreBuilder(Rescorer rescorer) {
        if (rescorer == null) {
            throw new IllegalArgumentException("rescorer cannot be null");
        }
        this.rescorer = rescorer;
    }

    public Rescorer rescorer() {
        return this.rescorer;
    }

    public RescoreBuilder windowSize(int windowSize) {
        this.windowSize = windowSize;
        return this;
    }

    public Integer windowSize() {
        return windowSize;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (windowSize != null) {
            builder.field("window_size", windowSize);
        }
        rescorer.toXContent(builder, params);
        return builder;
    }

    public static QueryRescorer queryRescorer(QueryBuilder<?> queryBuilder) {
        return new QueryRescorer(queryBuilder);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(windowSize, rescorer);
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RescoreBuilder other = (RescoreBuilder) obj;
        return Objects.equals(windowSize, other.windowSize) &&
               Objects.equals(rescorer, other.rescorer);
    }

    @Override
    public RescoreBuilder readFrom(StreamInput in) throws IOException {
        RescoreBuilder builder = new RescoreBuilder(in.readRescorer());
        Integer windowSize = in.readOptionalVInt();
        if (windowSize != null) {
            builder.windowSize(windowSize);
        }
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeRescorer(rescorer);
        out.writeOptionalVInt(this.windowSize);
    }

    @Override
    public final String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (Exception e) {
            return "{ \"error\" : \"" + ExceptionsHelper.detailedMessage(e) + "\"}";
        }
    }

    public static abstract class Rescorer implements ToXContent, NamedWriteable<Rescorer> {

        private String name;

        public Rescorer(String name) {
            this.name = name;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(name);
            builder = innerToXContent(builder, params);
            builder.endObject();
            return builder;
        }

        protected abstract XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException;

        @Override
        public abstract int hashCode();

        @Override
        public abstract boolean equals(Object obj);
    }

    public static class QueryRescorer extends Rescorer {

        private static final String NAME = "query";
        public static final QueryRescorer PROTOTYPE = new QueryRescorer(new MatchAllQueryBuilder());
        public static final float DEFAULT_RESCORE_QUERYWEIGHT = 1.0f;
        public static final float DEFAULT_QUERYWEIGHT = 1.0f;
        public static final QueryRescoreMode DEFAULT_SCORE_MODE = QueryRescoreMode.Total;
        private final QueryBuilder<?> queryBuilder;
        private float rescoreQueryWeight = DEFAULT_RESCORE_QUERYWEIGHT;
        private float queryWeight = DEFAULT_QUERYWEIGHT;
        private QueryRescoreMode scoreMode = DEFAULT_SCORE_MODE;

        /**
         * Creates a new {@link QueryRescorer} instance
         * @param builder the query builder to build the rescore query from
         */
        public QueryRescorer(QueryBuilder<?> builder) {
            super(NAME);
            this.queryBuilder = builder;
        }

        /**
         * @return the query used for this rescore query
         */
        public QueryBuilder<?> getRescoreQuery() {
            return this.queryBuilder;
        }

        /**
         * Sets the original query weight for rescoring. The default is <tt>1.0</tt>
         */
        public QueryRescorer setQueryWeight(float queryWeight) {
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
        public QueryRescorer setRescoreQueryWeight(float rescoreQueryWeight) {
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
        public QueryRescorer setScoreMode(QueryRescoreMode scoreMode) {
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
        protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("rescore_query", queryBuilder);
            builder.field("query_weight", queryWeight);
            builder.field("rescore_query_weight", rescoreQueryWeight);
            builder.field("score_mode", scoreMode.name().toLowerCase(Locale.ROOT));
            return builder;
        }

        @Override
        public final int hashCode() {
            return Objects.hash(getClass(), scoreMode, queryWeight, rescoreQueryWeight, queryBuilder);
        }

        @Override
        public final boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            QueryRescorer other = (QueryRescorer) obj;
            return Objects.equals(scoreMode, other.scoreMode) &&
                   Objects.equals(queryWeight, other.queryWeight) &&
                   Objects.equals(rescoreQueryWeight, other.rescoreQueryWeight) &&
                   Objects.equals(queryBuilder, other.queryBuilder);
        }

        @Override
        public QueryRescorer readFrom(StreamInput in) throws IOException {
            QueryRescorer rescorer = new QueryRescorer(in.readQuery());
            rescorer.setScoreMode(QueryRescoreMode.PROTOTYPE.readFrom(in));
            rescorer.setRescoreQueryWeight(in.readFloat());
            rescorer.setQueryWeight(in.readFloat());
            return rescorer;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeQuery(queryBuilder);
            scoreMode.writeTo(out);
            out.writeFloat(rescoreQueryWeight);
            out.writeFloat(queryWeight);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }
}
