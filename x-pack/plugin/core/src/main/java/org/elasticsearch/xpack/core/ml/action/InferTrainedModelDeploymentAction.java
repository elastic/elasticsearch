/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class InferTrainedModelDeploymentAction extends ActionType<InferTrainedModelDeploymentAction.Response> {

    public static final InferTrainedModelDeploymentAction INSTANCE = new InferTrainedModelDeploymentAction();

    // TODO Review security level
    public static final String NAME = "cluster:monitor/xpack/ml/trained_models/deployment/infer";

    public InferTrainedModelDeploymentAction() {
        super(NAME, InferTrainedModelDeploymentAction.Response::new);
    }

    public static class Request extends BaseTasksRequest<Request> implements ToXContentObject {

        public static final ParseField DEPLOYMENT_ID = new ParseField("deployment_id");
        public static final ParseField DOCS = new ParseField("docs");
        public static final ParseField TIMEOUT = new ParseField("timeout");

        public static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(10);

        static final ObjectParser<Request.Builder, Void> PARSER = new ObjectParser<>(NAME, Request.Builder::new);
        static {
            PARSER.declareString(Request.Builder::setDeploymentId, DEPLOYMENT_ID);
            PARSER.declareObjectArray(Request.Builder::setDocs, (p, c) -> p.mapOrdered(), DOCS);
            PARSER.declareString(Request.Builder::setTimeout, TIMEOUT);
        }

        public static Request parseRequest(String deploymentId, XContentParser parser) {
            Request.Builder builder = PARSER.apply(parser, null);
            if (deploymentId != null) {
                builder.setDeploymentId(deploymentId);
            }
            return builder.build();
        }

        private final String deploymentId;
        private final List<Map<String, Object>> docs;

        public Request(String deploymentId, List<Map<String, Object>> docs) {
            this.deploymentId = ExceptionsHelper.requireNonNull(deploymentId, DEPLOYMENT_ID);
            this.docs = ExceptionsHelper.requireNonNull(Collections.unmodifiableList(docs), DOCS);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            deploymentId = in.readString();
            docs = Collections.unmodifiableList(in.readList(StreamInput::readMap));
        }

        public String getDeploymentId() {
            return deploymentId;
        }

        public List<Map<String, Object>> getDocs() {
            return docs;
        }

        @Override
        public TimeValue getTimeout() {
            TimeValue tv = super.getTimeout();
            if (tv == null) {
                return DEFAULT_TIMEOUT;
            }
            return tv;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = super.validate();
            if (docs == null) {
                validationException = addValidationError("[" + DOCS.getPreferredName() + "] must not be null",
                    validationException);
            } else {
                if (docs.isEmpty()) {
                    validationException = addValidationError("at least one document is required",
                        validationException);
                }
                if (docs.size() > 1) {
                    // TODO support multiple docs
                    validationException = addValidationError("multiple documents are not supported",
                        validationException);
                }
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(deploymentId);
            out.writeCollection(docs, StreamOutput::writeMap);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field(DEPLOYMENT_ID.getPreferredName(), deploymentId);
            builder.field(DOCS.getPreferredName(), docs);
            builder.field(TIMEOUT.getPreferredName(), getTimeout().getStringRep());
            builder.endObject();
            return builder;
        }

        @Override
        public boolean match(Task task) {
            return StartTrainedModelDeploymentAction.TaskMatcher.match(task, deploymentId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InferTrainedModelDeploymentAction.Request that = (InferTrainedModelDeploymentAction.Request) o;
            return Objects.equals(deploymentId, that.deploymentId)
                && Objects.equals(docs, that.docs)
                && Objects.equals(getTimeout(), that.getTimeout());
        }

        @Override
        public int hashCode() {
            return Objects.hash(deploymentId, docs, getTimeout());
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        public static class Builder {

            private String deploymentId;
            private List<Map<String, Object>> docs;
            private TimeValue timeout;

            private Builder() {}

            public Builder setDeploymentId(String deploymentId) {
                this.deploymentId = ExceptionsHelper.requireNonNull(deploymentId, DEPLOYMENT_ID);
                return this;
            }

            public Builder setDocs(List<Map<String, Object>> docs) {
                this.docs = ExceptionsHelper.requireNonNull(docs, DOCS);
                return this;
            }

            public Builder setTimeout(TimeValue timeout) {
                this.timeout = timeout;
                return this;
            }

            private Builder setTimeout(String timeout) {
                return setTimeout(TimeValue.parseTimeValue(timeout, TIMEOUT.getPreferredName()));
            }

            public Request build() {
                Request request = new Request(deploymentId, docs);
                if (timeout != null) {
                    request.setTimeout(timeout);
                }
                return request;
            }
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {

        private final InferenceResults results;

        public Response(InferenceResults result) {
            super(Collections.emptyList(), Collections.emptyList());
            this.results = Objects.requireNonNull(result);
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            results = in.readNamedWriteable(InferenceResults.class);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            results.toXContent(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeNamedWriteable(results);
        }

        public InferenceResults getResults() {
            return results;
        }
    }
}
