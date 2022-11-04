/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class DeleteSnapshotLifecycleAction extends ActionType<AcknowledgedResponse> {
    public static final DeleteSnapshotLifecycleAction INSTANCE = new DeleteSnapshotLifecycleAction();
    public static final String NAME = "cluster:admin/slm/delete";

    protected DeleteSnapshotLifecycleAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private String lifecycleId;

        public Request(StreamInput in) throws IOException {
            super(in);
            lifecycleId = in.readString();
        }

        public Request() {}

        public Request(String lifecycleId) {
            this.lifecycleId = Objects.requireNonNull(lifecycleId, "id may not be null");
        }

        public String getLifecycleId() {
            return this.lifecycleId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(lifecycleId);
        }

        @Override
        public int hashCode() {
            return lifecycleId.hashCode();
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
            return Objects.equals(lifecycleId, other.lifecycleId);
        }
    }
}
