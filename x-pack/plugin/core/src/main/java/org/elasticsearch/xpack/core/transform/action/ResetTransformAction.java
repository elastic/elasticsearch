/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class ResetTransformAction extends ActionType<AcknowledgedResponse> {

    public static final String NAME = "cluster:admin/transform/reset";
    public static final ResetTransformAction INSTANCE = new ResetTransformAction();

    private ResetTransformAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final String id;
        private final boolean force;

        public Request(String id, boolean force, TimeValue timeout) {
            super(timeout);
            this.id = ExceptionsHelper.requireNonNull(id, TransformField.ID.getPreferredName());
            this.force = force;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
            force = in.readBoolean();
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
            out.writeBoolean(force);
        }

        @Override
        public int hashCode() {
            // the base class does not implement hashCode, therefore we need to hash timeout ourselves
            return Objects.hash(timeout(), id, force);
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
            // the base class does not implement equals, therefore we need to check timeout ourselves
            return Objects.equals(id, other.id) && force == other.force && timeout().equals(other.timeout());
        }
    }
}
