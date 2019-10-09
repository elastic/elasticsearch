/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.enrich.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Objects;

public class ExecuteEnrichPolicyAction extends ActionType<ExecuteEnrichPolicyAction.Response> {

    public static final ExecuteEnrichPolicyAction INSTANCE = new ExecuteEnrichPolicyAction();
    public static final String NAME = "cluster:admin/xpack/enrich/execute";

    private ExecuteEnrichPolicyAction() {
        super(NAME, ExecuteEnrichPolicyAction.Response::new);
    }

    public static class Request extends MasterNodeRequest<Request> {

        private final String name;

        public Request(String name) {
            this.name = Objects.requireNonNull(name, "name cannot be null");
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            name = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
        }

        public String getName() {
            return name;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        // This will be displayed in tasks api and allows stats api to figure out which policies are being executed.
        @Override
        public String getDescription() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return name.equals(request.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final TaskId taskId;
        private final ExecuteEnrichPolicyStatus status;

        public Response(ExecuteEnrichPolicyStatus status) {
            this.taskId = null;
            this.status = status;
        }

        public Response(TaskId taskId) {
            this.taskId = taskId;
            this.status = null;
        }

        public TaskId getTaskId() {
            return taskId;
        }

        public ExecuteEnrichPolicyStatus getStatus() {
            return status;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            if (in.readBoolean()) {
                this.status = new ExecuteEnrichPolicyStatus(in);
                this.taskId = null;
            } else {
                this.taskId = TaskId.readFromStream(in);
                this.status = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            boolean waitedForCompletion = status != null;
            out.writeBoolean(waitedForCompletion);
            if (waitedForCompletion) {
                status.writeTo(out);
            } else {
                taskId.writeTo(out);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                if (taskId != null) {
                    builder.field("task", taskId.getNodeId() + ":" + taskId.getId());
                } else {
                    builder.field("status", status);
                }
            }
            builder.endObject();
            return builder;
        }
    }
}
