/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class StopDataFrameTransformAction extends Action<StopDataFrameTransformAction.Response> {

    public static final StopDataFrameTransformAction INSTANCE = new StopDataFrameTransformAction();
    public static final String NAME = "cluster:admin/data_frame/stop";

    public static final TimeValue DEFAULT_TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    private StopDataFrameTransformAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends BaseTasksRequest<Request> {
        private String id;
        private final boolean waitForCompletion;
        private final boolean force;

        public Request(String id, boolean waitForCompletion, boolean force, @Nullable TimeValue timeout) {
            this.id = ExceptionsHelper.requireNonNull(id, DataFrameField.ID.getPreferredName());
            this.waitForCompletion = waitForCompletion;
            this.force = force;

            // use the timeout value already present in BaseTasksRequest
            this.setTimeout(timeout == null ? DEFAULT_TIMEOUT : timeout);
        }

        private Request() {
            this(null, false, false, null);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
            waitForCompletion = in.readBoolean();
            force = in.readBoolean();
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public boolean waitForCompletion() {
            return waitForCompletion;
        }

        public boolean isForce() {
            return force;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            out.writeBoolean(waitForCompletion);
            out.writeBoolean(force);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public int hashCode() {
            // the base class does not implement hashCode, therefore we need to hash timeout ourselves
            return Objects.hash(id, waitForCompletion, force, this.getTimeout());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;

            // the base class does not implement equals, therefore we need to compare timeout ourselves
            if (Objects.equals(this.getTimeout(), other.getTimeout()) == false) {
                return false;
            }

            return Objects.equals(id, other.id) &&
                Objects.equals(waitForCompletion, other.waitForCompletion) &&
                Objects.equals(force, other.force);
        }

        @Override
        public boolean match(Task task) {
            String expectedDescription = DataFrameField.PERSISTENT_TASK_DESCRIPTION_PREFIX + id;

            return task.getDescription().equals(expectedDescription);
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {

        private boolean stopped;

        public Response() {
            super(Collections.emptyList(), Collections.emptyList());
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            readFrom(in);
        }

        public Response(boolean stopped) {
            super(Collections.emptyList(), Collections.emptyList());
            this.stopped = stopped;
        }

        public boolean isStopped() {
            return stopped;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            stopped = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(stopped);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            toXContentCommon(builder, params);
            builder.field("stopped", stopped);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Response response = (Response) o;
            return stopped == response.stopped;
        }

        @Override
        public int hashCode() {
            return Objects.hash(stopped);
        }
    }
}
