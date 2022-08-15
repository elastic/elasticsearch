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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

public class PreviewDataFrameAnalyticsAction extends ActionType<PreviewDataFrameAnalyticsAction.Response> {

    public static final PreviewDataFrameAnalyticsAction INSTANCE = new PreviewDataFrameAnalyticsAction();
    public static final String NAME = "cluster:admin/xpack/ml/data_frame/analytics/preview";

    private PreviewDataFrameAnalyticsAction() {
        super(NAME, PreviewDataFrameAnalyticsAction.Response::new);
    }

    public static class Request extends ActionRequest {

        public static final ParseField CONFIG = new ParseField("config");

        private final DataFrameAnalyticsConfig config;

        static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("preview_data_frame_analytics_response", Request.Builder::new);
        static {
            PARSER.declareObject(Request.Builder::setConfig, DataFrameAnalyticsConfig.STRICT_PARSER::apply, CONFIG);
        }

        public static Request.Builder fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        public Request(DataFrameAnalyticsConfig config) {
            this.config = ExceptionsHelper.requireNonNull(config, CONFIG);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.config = new DataFrameAnalyticsConfig(in);
        }

        public DataFrameAnalyticsConfig getConfig() {
            return config;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            config.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(config, request.config);
        }

        @Override
        public int hashCode() {
            return Objects.hash(config);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, format("preview_data_frame_analytics[%s]", config.getId()), parentTaskId, headers);
        }

        public static class Builder {
            private DataFrameAnalyticsConfig config;

            private Builder setConfig(DataFrameAnalyticsConfig.Builder config) {
                this.config = config.buildForExplain();
                return this;
            }

            public Builder setConfig(DataFrameAnalyticsConfig config) {
                this.config = config;
                return this;
            }

            public DataFrameAnalyticsConfig getConfig() {
                return config;
            }

            public Request build() {
                return new Request(config);
            }
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public static final ParseField TYPE = new ParseField("preview_data_frame_analytics_response");
        public static final ParseField FEATURE_VALUES = new ParseField("feature_values");

        @SuppressWarnings("unchecked")
        static final ConstructingObjectParser<Response, Void> PARSER = new ConstructingObjectParser<>(
            TYPE.getPreferredName(),
            args -> new Response((List<Map<String, Object>>) args[0])
        );

        static {
            PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> p.map(), FEATURE_VALUES);
        }

        private final List<Map<String, Object>> featureValues;

        public Response(List<Map<String, Object>> featureValues) {
            this.featureValues = Objects.requireNonNull(featureValues);
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.featureValues = in.readList(StreamInput::readMap);
        }

        public List<Map<String, Object>> getFeatureValues() {
            return featureValues;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(featureValues, StreamOutput::writeGenericMap);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(FEATURE_VALUES.getPreferredName(), featureValues);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (other == null || getClass() != other.getClass()) return false;

            Response that = (Response) other;
            return Objects.equals(featureValues, that.featureValues);
        }

        @Override
        public int hashCode() {
            return Objects.hash(featureValues);
        }
    }
}
