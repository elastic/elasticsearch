/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.Evaluation;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.QueryProvider;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class EvaluateDataFrameAction extends ActionType<EvaluateDataFrameAction.Response> {

    public static final EvaluateDataFrameAction INSTANCE = new EvaluateDataFrameAction();
    public static final String NAME = "cluster:monitor/xpack/ml/data_frame/evaluate";

    private EvaluateDataFrameAction() {
        super(NAME, EvaluateDataFrameAction.Response::new);
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        private static final ParseField INDEX = new ParseField("index");
        private static final ParseField QUERY = new ParseField("query");
        private static final ParseField EVALUATION = new ParseField("evaluation");

        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            NAME,
            a -> new Request((List<String>) a[0], (QueryProvider) a[1], (Evaluation) a[2]));

        static {
            PARSER.declareStringArray(constructorArg(), INDEX);
            PARSER.declareObject(
                optionalConstructorArg(),
                (p, c) -> QueryProvider.fromXContent(p, true, Messages.DATA_FRAME_ANALYTICS_BAD_QUERY_FORMAT),
                QUERY);
            PARSER.declareObject(constructorArg(), (p, c) -> parseEvaluation(p), EVALUATION);
        }

        private static Evaluation parseEvaluation(XContentParser parser) throws IOException {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
            Evaluation evaluation = parser.namedObject(Evaluation.class, parser.currentName(), null);
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
            return evaluation;
        }

        public static Request parseRequest(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private String[] indices;
        private QueryProvider queryProvider;
        private Evaluation evaluation;

        private Request(List<String> indices, @Nullable QueryProvider queryProvider, Evaluation evaluation) {
            setIndices(indices);
            setQueryProvider(queryProvider);
            setEvaluation(evaluation);
        }

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            indices = in.readStringArray();
            if (in.getVersion().onOrAfter(Version.V_7_4_0)) {
                if (in.readBoolean()) {
                    queryProvider = QueryProvider.fromStream(in);
                }
            }
            evaluation = in.readNamedWriteable(Evaluation.class);
        }

        public String[] getIndices() {
            return indices;
        }

        public final Request setIndices(List<String> indices) {
            ExceptionsHelper.requireNonNull(indices, INDEX);
            if (indices.isEmpty()) {
                throw ExceptionsHelper.badRequestException("At least one index must be specified");
            }
            this.indices = indices.toArray(new String[indices.size()]);
            return this;
        }

        public QueryBuilder getParsedQuery() {
            return Optional.ofNullable(queryProvider).orElseGet(QueryProvider::defaultQuery).getParsedQuery();
        }

        public final Request setQueryProvider(QueryProvider queryProvider) {
            this.queryProvider = queryProvider;
            return this;
        }

        public Evaluation getEvaluation() {
            return evaluation;
        }

        public final Request setEvaluation(Evaluation evaluation) {
            this.evaluation = ExceptionsHelper.requireNonNull(evaluation, EVALUATION);
            return this;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(indices);
            if (out.getVersion().onOrAfter(Version.V_7_4_0)) {
                if (queryProvider != null) {
                    out.writeBoolean(true);
                    queryProvider.writeTo(out);
                } else {
                    out.writeBoolean(false);
                }
            }
            out.writeNamedWriteable(evaluation);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.array(INDEX.getPreferredName(), indices);
            if (queryProvider != null) {
                builder.field(QUERY.getPreferredName(), queryProvider.getQuery());
            }
            builder
                .startObject(EVALUATION.getPreferredName())
                    .field(evaluation.getName(), evaluation)
                .endObject();
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(indices), queryProvider, evaluation);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request that = (Request) o;
            return Arrays.equals(indices, that.indices)
                && Objects.equals(queryProvider, that.queryProvider)
                && Objects.equals(evaluation, that.evaluation);
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new Request());
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private String evaluationName;
        private List<EvaluationMetricResult> metrics;

        public Response(StreamInput in) throws IOException {
            super(in);
            this.evaluationName = in.readString();
            this.metrics = in.readNamedWriteableList(EvaluationMetricResult.class);
        }

        public Response(String evaluationName, List<EvaluationMetricResult> metrics) {
            this.evaluationName = Objects.requireNonNull(evaluationName);
            this.metrics = Objects.requireNonNull(metrics);
        }

        public String getEvaluationName() {
            return evaluationName;
        }

        public List<EvaluationMetricResult> getMetrics() {
            return metrics;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(evaluationName);
            out.writeNamedWriteableList(metrics);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject(evaluationName);
            for (EvaluationMetricResult metric : metrics) {
                builder.field(metric.getMetricName(), metric);
            }
            builder.endObject();
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(evaluationName, metrics);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(evaluationName, other.evaluationName) && Objects.equals(metrics, other.metrics);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }
}
