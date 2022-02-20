/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class StopTrainedModelDeploymentAction extends ActionType<StopTrainedModelDeploymentAction.Response> {

    public static final StopTrainedModelDeploymentAction INSTANCE = new StopTrainedModelDeploymentAction();
    public static final String NAME = "cluster:admin/xpack/ml/trained_models/deployment/stop";

    public StopTrainedModelDeploymentAction() {
        super(NAME, StopTrainedModelDeploymentAction.Response::new);
    }

    public static class Request extends BaseTasksRequest<Request> implements ToXContentObject {

        public static final ParseField ALLOW_NO_MATCH = new ParseField("allow_no_match");
        public static final ParseField FORCE = new ParseField("force");

        private String id;
        private boolean allowNoMatch = true;
        private boolean force;

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString(Request::setId, TrainedModelConfig.MODEL_ID);
            PARSER.declareBoolean(Request::setAllowNoMatch, ALLOW_NO_MATCH);
            PARSER.declareBoolean(Request::setForce, FORCE);
        }

        public static Request parseRequest(String id, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (request.getId() == null) {
                request.setId(id);
            } else if (Strings.isNullOrEmpty(id) == false && id.equals(request.getId()) == false) {
                throw new IllegalArgumentException(
                    Messages.getMessage(Messages.INCONSISTENT_ID, TrainedModelConfig.MODEL_ID, request.getId(), id)
                );
            }
            return request;
        }

        public Request(String id) {
            setId(id);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
            allowNoMatch = in.readBoolean();
            force = in.readBoolean();
        }

        private Request() {}

        public final void setId(String id) {
            this.id = ExceptionsHelper.requireNonNull(id, TrainedModelConfig.MODEL_ID);
        }

        public String getId() {
            return id;
        }

        public void setAllowNoMatch(boolean allowNoMatch) {
            this.allowNoMatch = allowNoMatch;
        }

        public boolean isAllowNoMatch() {
            return allowNoMatch;
        }

        public void setForce(boolean force) {
            this.force = force;
        }

        public boolean isForce() {
            return force;
        }

        @Override
        public boolean match(Task task) {
            return StartTrainedModelDeploymentAction.TaskMatcher.match(task, id);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            out.writeBoolean(allowNoMatch);
            out.writeBoolean(force);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TrainedModelConfig.MODEL_ID.getPreferredName(), id);
            builder.field(ALLOW_NO_MATCH.getPreferredName(), allowNoMatch);
            builder.field(FORCE.getPreferredName(), force);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, allowNoMatch, force);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Request that = (Request) o;
            return Objects.equals(id, that.id) && allowNoMatch == that.allowNoMatch && force == that.force;
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {

        private final boolean undeployed;

        public Response(boolean undeployed) {
            super(null, null);
            this.undeployed = undeployed;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            undeployed = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(undeployed);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            toXContentCommon(builder, params);
            builder.field("stopped", undeployed);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(undeployed);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response that = (Response) o;
            return undeployed == that.undeployed;
        }
    }
}
