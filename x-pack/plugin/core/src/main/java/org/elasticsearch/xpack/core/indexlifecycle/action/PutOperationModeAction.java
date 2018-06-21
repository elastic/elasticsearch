/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.xpack.core.indexlifecycle.OperationMode;

import java.io.IOException;
import java.util.Objects;

public class PutOperationModeAction extends Action<PutOperationModeAction.Response> {
    public static final PutOperationModeAction INSTANCE = new PutOperationModeAction();
    public static final String NAME = "cluster:admin/xpack/index_lifecycle/operation_mode/set";

    protected PutOperationModeAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Response extends AcknowledgedResponse implements ToXContentObject {

        public Response() {
        }

        public Response(boolean acknowledged) {
            super(acknowledged);
        }
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private OperationMode mode;

        public Request(OperationMode mode) {
            this.mode = mode;
        }

        public Request() {
        }

        public OperationMode getMode() {
            return mode;
        }

        @Override
        public ActionRequestValidationException validate() {
            if (mode == OperationMode.STOPPED) {
                ActionRequestValidationException exception = new ActionRequestValidationException();
                exception.addValidationError("cannot directly stop index-lifecycle");
                return exception;
            }
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            mode = in.readEnum(OperationMode.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
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
            Request other = (Request) obj;
            return Objects.equals(mode, other.mode);
        }
    }

}
