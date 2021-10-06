/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.rollup.RollupField;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class StopRollupJobAction extends ActionType<StopRollupJobAction.Response> {

    public static final StopRollupJobAction INSTANCE = new StopRollupJobAction();
    public static final String NAME = "cluster:admin/xpack/rollup/stop";
    public static final ParseField WAIT_FOR_COMPLETION = new ParseField("wait_for_completion");
    public static final ParseField TIMEOUT = new ParseField("timeout");
    public static final TimeValue DEFAULT_TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    private StopRollupJobAction() {
        super(NAME, StopRollupJobAction.Response::new);
    }

    public static class Request extends BaseTasksRequest<Request> implements ToXContentObject {
        private String id;
        private boolean waitForCompletion = false;
        private TimeValue timeout = null;

        public Request (String id) {
            this(id, false, null);
        }

        public Request(String id, boolean waitForCompletion, @Nullable TimeValue timeout) {
            this.id = ExceptionsHelper.requireNonNull(id, RollupField.ID.getPreferredName());
            this.timeout = timeout == null ? DEFAULT_TIMEOUT : timeout;
            this.waitForCompletion = waitForCompletion;
        }

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
            waitForCompletion = in.readBoolean();
            timeout = in.readTimeValue();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            out.writeBoolean(waitForCompletion);
            out.writeTimeValue(timeout);
        }

        public String getId() {
            return id;
        }

        public TimeValue timeout() {
            return timeout;
        }

        public boolean waitForCompletion() {
            return waitForCompletion;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(RollupField.ID.getPreferredName(), id);
            builder.field(WAIT_FOR_COMPLETION.getPreferredName(), waitForCompletion);
            if (timeout != null) {
                builder.field(TIMEOUT.getPreferredName(), timeout);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, waitForCompletion, timeout);
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
            return Objects.equals(id, other.id)
                && Objects.equals(waitForCompletion, other.waitForCompletion)
                && Objects.equals(timeout, other.timeout);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        protected RequestBuilder(ElasticsearchClient client, StopRollupJobAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {

        private final boolean stopped;

        public Response(boolean stopped) {
            super(Collections.emptyList(), Collections.emptyList());
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

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("stopped", stopped);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return stopped == response.stopped;
        }

        @Override
        public int hashCode() {
            return Objects.hash(stopped);
        }
    }
}
