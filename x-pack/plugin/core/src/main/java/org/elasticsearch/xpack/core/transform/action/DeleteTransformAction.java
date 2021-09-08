/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class DeleteTransformAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteTransformAction INSTANCE = new DeleteTransformAction();
    public static final String NAME = "cluster:admin/transform/delete";

    private DeleteTransformAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends MasterNodeRequest<Request> {
        private final String id;
        private final boolean force;

        public Request(String id, boolean force) {
            this.id = ExceptionsHelper.requireNonNull(id, TransformField.ID.getPreferredName());
            this.force = force;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
            if (in.getVersion().onOrAfter(Version.V_7_4_0)) {
                force = in.readBoolean();
            } else {
                force = false;
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
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, force);
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
            return Objects.equals(id, other.id) && force == other.force;
        }
    }
}
