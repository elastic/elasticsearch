/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class EstimateModelMemoryAction extends ActionType<EstimateModelMemoryAction.Response> {

    public static final EstimateModelMemoryAction INSTANCE = new EstimateModelMemoryAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/estimate_model_memory";

    private EstimateModelMemoryAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {

        public static final ParseField ANALYSIS_CONFIG = Job.ANALYSIS_CONFIG;
        public static final ParseField OVERALL_CARDINALITY = new ParseField("overall_cardinality");
        public static final ParseField MAX_BUCKET_CARDINALITY = new ParseField("max_bucket_cardinality");

        public static final ObjectParser<Request, Void> PARSER =
            new ObjectParser<>(NAME, EstimateModelMemoryAction.Request::new);

        static {
            PARSER.declareObject(Request::setAnalysisConfig, (p, c) -> AnalysisConfig.STRICT_PARSER.apply(p, c).build(), ANALYSIS_CONFIG);
            PARSER.declareObject(Request::setOverallCardinality,
                (p, c) -> p.map(HashMap::new, parser -> Request.parseNonNegativeLong(parser, OVERALL_CARDINALITY)),
                OVERALL_CARDINALITY);
            PARSER.declareObject(Request::setMaxBucketCardinality,
                (p, c) -> p.map(HashMap::new, parser -> Request.parseNonNegativeLong(parser, MAX_BUCKET_CARDINALITY)),
                MAX_BUCKET_CARDINALITY);
        }

        public static Request parseRequest(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private AnalysisConfig analysisConfig;
        private Map<String, Long> overallCardinality = Collections.emptyMap();
        private Map<String, Long> maxBucketCardinality = Collections.emptyMap();

        public Request() {
            super();
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.analysisConfig = in.readBoolean() ? new AnalysisConfig(in) : null;
            this.overallCardinality = in.readMap(StreamInput::readString, StreamInput::readVLong);
            this.maxBucketCardinality = in.readMap(StreamInput::readString, StreamInput::readVLong);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (analysisConfig != null) {
                out.writeBoolean(true);
                analysisConfig.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
            out.writeMap(overallCardinality, StreamOutput::writeString, StreamOutput::writeVLong);
            out.writeMap(maxBucketCardinality, StreamOutput::writeString, StreamOutput::writeVLong);
        }

        @Override
        public ActionRequestValidationException validate() {
            if (analysisConfig == null) {
                ActionRequestValidationException e = new ActionRequestValidationException();
                e.addValidationError("[" + ANALYSIS_CONFIG.getPreferredName() + "] was not specified");
                return e;
            }
            return null;
        }

        public AnalysisConfig getAnalysisConfig() {
            return analysisConfig;
        }

        public void setAnalysisConfig(AnalysisConfig analysisConfig) {
            this.analysisConfig = ExceptionsHelper.requireNonNull(analysisConfig, ANALYSIS_CONFIG);
        }

        public Map<String, Long> getOverallCardinality() {
            return overallCardinality;
        }

        public void setOverallCardinality(Map<String, Long> overallCardinality) {
            this.overallCardinality =
                Collections.unmodifiableMap(ExceptionsHelper.requireNonNull(overallCardinality, OVERALL_CARDINALITY));
        }

        public Map<String, Long> getMaxBucketCardinality() {
            return maxBucketCardinality;
        }

        public void setMaxBucketCardinality(Map<String, Long> maxBucketCardinality) {
            this.maxBucketCardinality =
                Collections.unmodifiableMap(ExceptionsHelper.requireNonNull(maxBucketCardinality, MAX_BUCKET_CARDINALITY));
        }

        private static long parseNonNegativeLong(XContentParser parser, ParseField enclosingField) throws IOException {
            long value = parser.longValue();
            if (value < 0) {
                throw ExceptionsHelper.badRequestException("[{}] contained negative cardinality [{}]",
                    enclosingField.getPreferredName(), value);
            }
            return value;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private static final ParseField MODEL_MEMORY_ESTIMATE = new ParseField("model_memory_estimate");

        private final ByteSizeValue modelMemoryEstimate;

        public Response(ByteSizeValue modelMemoryEstimate) {
            this.modelMemoryEstimate = Objects.requireNonNull(modelMemoryEstimate);
        }

        public Response(StreamInput in) throws IOException {
            modelMemoryEstimate = new ByteSizeValue(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            modelMemoryEstimate.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MODEL_MEMORY_ESTIMATE.getPreferredName(), modelMemoryEstimate.getStringRep());
            builder.endObject();
            return builder;
        }

        public ByteSizeValue getModelMemoryEstimate() {
            return modelMemoryEstimate;
        }
    }
}
