/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.slm.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Action used to manually invoke a create snapshot request for a given
 * snapshot lifecycle policy regardless of schedule.
 */
public class ExecuteSnapshotLifecycleAction extends ActionType<ExecuteSnapshotLifecycleAction.Response> {
    public static final ExecuteSnapshotLifecycleAction INSTANCE = new ExecuteSnapshotLifecycleAction();
    public static final String NAME = "cluster:admin/slm/execute";

    protected ExecuteSnapshotLifecycleAction() {
        super(NAME, ExecuteSnapshotLifecycleAction.Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        private String lifecycleId;

        public Request(String lifecycleId) {
            this.lifecycleId = lifecycleId;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            lifecycleId = in.readString();
        }

        public Request() { }

        public String getLifecycleId() {
            return this.lifecycleId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(lifecycleId);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(lifecycleId);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return lifecycleId.equals(other.lifecycleId);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final String snapshotName;

        public Response(String snapshotName) {
            this.snapshotName = snapshotName;
        }

        public String getSnapshotName() {
            return this.snapshotName;
        }

        public Response(StreamInput in) throws IOException {
            this(in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.snapshotName);
        }

        @Override
        public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("snapshot_name", getSnapshotName());
            builder.endObject();
            return builder;
        }
    }
}
