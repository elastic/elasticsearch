/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

public class StartDataFrameTransformTaskAction extends ActionType<StartDataFrameTransformTaskAction.Response> {

    public static final StartDataFrameTransformTaskAction INSTANCE = new StartDataFrameTransformTaskAction();
    public static final String NAME = "cluster:admin/data_frame/start_task";

    private StartDataFrameTransformTaskAction() {
        super(NAME, StartDataFrameTransformTaskAction.Response::new);
    }

    public static class Request extends BaseTasksRequest<Request> {

        private final String id;
        private final boolean force;

        public Request(String id, boolean force) {
            this.id = ExceptionsHelper.requireNonNull(id, DataFrameField.ID.getPreferredName());
            this.force = force;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
            if (in.getVersion().onOrAfter(Version.V_7_4_0)) {
                force = in.readBoolean();
            } else {
                // The behavior before V_7_4_0 was that this flag did not exist,
                // assuming previous checks allowed this task to be started.
                force = true;
            }
        }

        public String getId() {
            return id;
        }

        public boolean isForce() {
            return force;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            if (out.getVersion().onOrAfter(Version.V_7_4_0)) {
                out.writeBoolean(force);
            }
        }

        @Override
        public boolean match(Task task) {
            return task.getDescription().equals(DataFrameField.PERSISTENT_TASK_DESCRIPTION_PREFIX + id);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, force);
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
            return Objects.equals(id, other.id) && force == other.force;
        }
    }

    public static class Response extends BaseTasksResponse implements ToXContentObject {
        private final boolean started;

        public Response(StreamInput in) throws IOException {
            super(in);
            started = in.readBoolean();
        }

        public Response(boolean started) {
            super(Collections.emptyList(), Collections.emptyList());
            this.started = started;
        }

        public boolean isStarted() {
            return started;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(started);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            toXContentCommon(builder, params);
            builder.field("started", started);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Response response = (Response) obj;
            return started == response.started;
        }

        @Override
        public int hashCode() {
            return Objects.hash(started);
        }
    }
}
