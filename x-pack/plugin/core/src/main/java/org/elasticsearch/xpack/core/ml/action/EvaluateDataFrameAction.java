/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.Evaluation;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class EvaluateDataFrameAction extends ActionType<EvaluateDataFrameAction.Response> {

    public static final EvaluateDataFrameAction INSTANCE = new EvaluateDataFrameAction();
    public static final String NAME = "cluster:monitor/xpack/ml/data_frame/evaluate";

    private EvaluateDataFrameAction() {
        super(NAME, EvaluateDataFrameAction.Response::new);
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        private static final ParseField INDEX = new ParseField("index");
        private static final ParseField EVALUATION = new ParseField("evaluation");

        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(NAME,
            a -> new Request((List<String>) a[0], (Evaluation) a[1]));

        static {
            PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), INDEX);
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> parseEvaluation(p), EVALUATION);
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
        private Evaluation evaluation;

        private Request(List<String> indices, Evaluation evaluation) {
            setIndices(indices);
            setEvaluation(evaluation);
        }

        public Request() {
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            indices = in.readStringArray();
            evaluation = in.readNamedWriteable(Evaluation.class);
        }

        public String[] getIndices() {
            return indices;
        }

        public final void setIndices(List<String> indices) {
            ExceptionsHelper.requireNonNull(indices, INDEX);
            if (indices.isEmpty()) {
                throw ExceptionsHelper.badRequestException("At least one index must be specified");
            }
            this.indices = indices.toArray(new String[indices.size()]);
        }

        public Evaluation getEvaluation() {
            return evaluation;
        }

        public final void setEvaluation(Evaluation evaluation) {
            this.evaluation = ExceptionsHelper.requireNonNull(evaluation, EVALUATION);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(indices);
            out.writeNamedWriteable(evaluation);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.array(INDEX.getPreferredName(), indices);
            builder.startObject(EVALUATION.getPreferredName());
            builder.field(evaluation.getName(), evaluation);
            builder.endObject();
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(indices), evaluation);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request that = (Request) o;
            return Arrays.equals(indices, that.indices) && Objects.equals(evaluation, that.evaluation);
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

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(evaluationName);
            out.writeList(metrics);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject(evaluationName);
            for (EvaluationMetricResult metric : metrics) {
                builder.field(metric.getName(), metric);
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
