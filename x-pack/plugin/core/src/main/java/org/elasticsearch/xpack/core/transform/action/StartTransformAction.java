/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.transform.transforms.TransformConfig.TRANSFORM_CLOUD_CREDENTIAL_ON_REQUEST;

public class StartTransformAction extends ActionType<StartTransformAction.Response> {

    public static final StartTransformAction INSTANCE = new StartTransformAction();
    public static final String NAME = "cluster:admin/transform/start";

    private StartTransformAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements Releasable {

        private final String id;
        private final Instant from;

        // Caller's UIAM cloud credential carried on the request so it survives coordinator -> master
        // transport, where the AUTHENTICATING_CLOUD_TOKEN_THREAD_CONTEXT transient is no longer present.
        @Nullable
        private CloudCredential cloudCredential;

        public Request(String id, Instant from, TimeValue timeout) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, timeout);
            this.id = ExceptionsHelper.requireNonNull(id, TransformField.ID.getPreferredName());
            this.from = from;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
            from = in.readOptionalInstant();
            if (in.getTransportVersion().supports(TRANSFORM_CLOUD_CREDENTIAL_ON_REQUEST)) {
                cloudCredential = in.readOptionalWriteable(CloudCredential::new);
            } else {
                cloudCredential = null;
            }
        }

        public String getId() {
            return id;
        }

        public Instant from() {
            return from;
        }

        @Nullable
        public CloudCredential getCloudCredential() {
            return cloudCredential;
        }

        /**
         * Sets the credential this request carries and hands ownership of the previously-held
         * credential back to the caller, which is responsible for closing it. Returns {@code null}
         * when the credential is unchanged.
         */
        @Nullable
        public CloudCredential setCloudCredential(@Nullable CloudCredential cloudCredential) {
            var previous = this.cloudCredential == cloudCredential ? null : this.cloudCredential;
            this.cloudCredential = cloudCredential;
            return previous;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            out.writeOptionalInstant(from);
            if (out.getTransportVersion().supports(TRANSFORM_CLOUD_CREDENTIAL_ON_REQUEST)) {
                out.writeOptionalWriteable(cloudCredential);
            }
        }

        @Override
        public void close() {
            IOUtils.closeWhileHandlingException(cloudCredential);
        }

        @Override
        public int hashCode() {
            // the base class does not implement hashCode, therefore we need to hash timeout ourselves
            // cloudCredential is intentionally excluded: request-scoped secret carrier, not logical identity.
            return Objects.hash(ackTimeout(), id, from);
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
            // the base class does not implement equals, therefore we need to check timeout ourselves
            // cloudCredential is intentionally excluded: request-scoped secret carrier, not logical identity.
            return Objects.equals(id, other.id) && Objects.equals(from, other.from) && ackTimeout().equals(other.ackTimeout());
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }
    }

    public static class Response extends BaseTasksResponse implements ToXContentObject {
        private final boolean acknowledged;

        public Response(StreamInput in) throws IOException {
            super(in);
            acknowledged = in.readBoolean();
        }

        public Response(boolean acknowledged) {
            super(Collections.emptyList(), Collections.emptyList());
            this.acknowledged = acknowledged;
        }

        public boolean isAcknowledged() {
            return acknowledged;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(acknowledged);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            toXContentCommon(builder, params);
            builder.field("acknowledged", acknowledged);
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
            return acknowledged == response.acknowledged;
        }

        @Override
        public int hashCode() {
            return Objects.hash(acknowledged);
        }
    }
}
