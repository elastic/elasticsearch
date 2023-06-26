/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Objects;

public class StartTransformAction extends ActionType<StartTransformAction.Response> {

    public static final StartTransformAction INSTANCE = new StartTransformAction();
    public static final String NAME = "cluster:admin/transform/start";

    private StartTransformAction() {
        super(NAME, StartTransformAction.Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final String id;
        private final Instant from;

        public Request(String id, Instant from, TimeValue timeout) {
            super(timeout);
            this.id = ExceptionsHelper.requireNonNull(id, TransformField.ID.getPreferredName());
            this.from = from;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
                from = in.readOptionalInstant();
            } else {
                from = null;
            }
        }

        public String getId() {
            return id;
        }

        public Instant from() {
            return from;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
                out.writeOptionalInstant(from);
            }
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public int hashCode() {
            // the base class does not implement hashCode, therefore we need to hash timeout ourselves
            return Objects.hash(timeout(), id, from);
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
            return Objects.equals(id, other.id) && Objects.equals(from, other.from) && timeout().equals(other.timeout());
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
