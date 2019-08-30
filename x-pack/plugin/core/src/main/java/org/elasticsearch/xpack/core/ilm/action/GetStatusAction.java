/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ilm.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ilm.OperationMode;

import java.io.IOException;
import java.util.Objects;

public class GetStatusAction extends ActionType<GetStatusAction.Response> {
    public static final GetStatusAction INSTANCE = new GetStatusAction();
    public static final String NAME = "cluster:admin/ilm/operation_mode/get";

    protected GetStatusAction() {
        super(NAME, GetStatusAction.Response::new);
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private OperationMode mode;

        public Response(StreamInput in) throws IOException {
            super(in);
            mode = in.readEnum(OperationMode.class);
        }

        public Response(OperationMode mode) {
            this.mode = mode;
        }

        public OperationMode getMode() {
            return mode;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("operation_mode", mode);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(mode);
        }

        @Override
        public int hashCode() {
            return Objects.hash(mode);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(mode, other.mode);
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }

    }

    public static class Request extends AcknowledgedRequest<Request> {

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        public Request() {
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }
}
