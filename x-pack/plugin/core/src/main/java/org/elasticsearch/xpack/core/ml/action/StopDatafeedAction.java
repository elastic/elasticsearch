/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class StopDatafeedAction extends ActionType<StopDatafeedAction.Response> {

    public static final StopDatafeedAction INSTANCE = new StopDatafeedAction();
    public static final String NAME = "cluster:admin/xpack/ml/datafeed/stop";
    public static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueMinutes(5);

    private StopDatafeedAction() {
        super(NAME, StopDatafeedAction.Response::new);
    }

    public static class Request extends BaseTasksRequest<Request> implements ToXContentObject {

        public static final ParseField TIMEOUT = new ParseField("timeout");
        public static final ParseField FORCE = new ParseField("force");
        @Deprecated
        public static final String ALLOW_NO_DATAFEEDS = "allow_no_datafeeds";
        public static final ParseField ALLOW_NO_MATCH = new ParseField("allow_no_match", ALLOW_NO_DATAFEEDS);

        public static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);
        static {
            PARSER.declareString((request, datafeedId) -> request.datafeedId = datafeedId, DatafeedConfig.ID);
            PARSER.declareString((request, val) ->
                    request.setStopTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
            PARSER.declareBoolean(Request::setForce, FORCE);
            PARSER.declareBoolean(Request::setAllowNoMatch, ALLOW_NO_MATCH);
        }

        public static Request fromXContent(XContentParser parser) {
            return parseRequest(null, parser);
        }

        public static Request parseRequest(String datafeedId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (datafeedId != null) {
                request.datafeedId = datafeedId;
            }
            return request;
        }

        private String datafeedId;
        private String[] resolvedStartedDatafeedIds = new String[] {};
        private TimeValue stopTimeout = DEFAULT_TIMEOUT;
        private boolean force = false;
        private boolean allowNoMatch = true;

        public Request(String datafeedId) {
            this.datafeedId = ExceptionsHelper.requireNonNull(datafeedId, DatafeedConfig.ID.getPreferredName());
        }

        public Request() {
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            datafeedId = in.readString();
            resolvedStartedDatafeedIds = in.readStringArray();
            stopTimeout = in.readTimeValue();
            force = in.readBoolean();
            allowNoMatch = in.readBoolean();
        }

        public String getDatafeedId() {
            return datafeedId;
        }

        public String[] getResolvedStartedDatafeedIds() {
            return resolvedStartedDatafeedIds;
        }

        // This is used internally - the transport action sets it, not the user
        public void setResolvedStartedDatafeedIds(String[] resolvedStartedDatafeedIds) {
            assert resolvedStartedDatafeedIds != null;
            this.resolvedStartedDatafeedIds = resolvedStartedDatafeedIds;
        }

        public TimeValue getStopTimeout() {
            return stopTimeout;
        }

        public Request setStopTimeout(TimeValue stopTimeout) {
            this.stopTimeout = ExceptionsHelper.requireNonNull(stopTimeout, TIMEOUT.getPreferredName());
            return this;
        }

        public boolean isForce() {
            return force;
        }

        public Request setForce(boolean force) {
            this.force = force;
            return this;
        }

        public boolean allowNoMatch() {
            return allowNoMatch;
        }

        public Request setAllowNoMatch(boolean allowNoMatch) {
            this.allowNoMatch = allowNoMatch;
            return this;
        }

        @Override
        public boolean match(Task task) {
            for (String id : resolvedStartedDatafeedIds) {
                String expectedDescription = MlTasks.datafeedTaskId(id);
                if (task instanceof StartDatafeedAction.DatafeedTaskMatcher && expectedDescription.equals(task.getDescription())){
                    return true;
                }
            }
            return false;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(datafeedId);
            out.writeStringArray(resolvedStartedDatafeedIds);
            out.writeTimeValue(stopTimeout);
            out.writeBoolean(force);
            out.writeBoolean(allowNoMatch);
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeedId, stopTimeout, force, allowNoMatch);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(DatafeedConfig.ID.getPreferredName(), datafeedId);
            builder.field(TIMEOUT.getPreferredName(), stopTimeout.getStringRep());
            builder.field(FORCE.getPreferredName(), force);
            builder.field(ALLOW_NO_MATCH.getPreferredName(), allowNoMatch);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(datafeedId, other.datafeedId) &&
                    Objects.equals(stopTimeout, other.stopTimeout) &&
                    Objects.equals(force, other.force) &&
                    Objects.equals(allowNoMatch, other.allowNoMatch);
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable {

        private final boolean stopped;

        public Response(boolean stopped) {
            super(null, null);
            this.stopped = stopped;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            stopped = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(stopped);
        }

        public boolean isStopped() {
            return stopped;
        }

    }
}
