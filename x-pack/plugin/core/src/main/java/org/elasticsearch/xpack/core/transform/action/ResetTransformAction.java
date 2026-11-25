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
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.core.transform.transforms.TransformConfig.TRANSFORM_CLOUD_CREDENTIAL_ON_REQUEST;

public class ResetTransformAction extends ActionType<AcknowledgedResponse> {

    public static final String NAME = "cluster:admin/transform/reset";
    public static final ResetTransformAction INSTANCE = new ResetTransformAction();

    private ResetTransformAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements Releasable {

        private final String id;
        private final boolean force;

        // Caller's UIAM cloud credential carried on the request so it survives coordinator -> master
        // transport, where the AUTHENTICATING_CLOUD_TOKEN_THREAD_CONTEXT transient is no longer present.
        @Nullable
        private CloudCredential cloudCredential;

        public Request(String id, boolean force, TimeValue timeout) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, timeout);
            this.id = ExceptionsHelper.requireNonNull(id, TransformField.ID.getPreferredName());
            this.force = force;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
            force = in.readBoolean();
            if (in.getTransportVersion().supports(TRANSFORM_CLOUD_CREDENTIAL_ON_REQUEST)) {
                this.cloudCredential = in.readOptionalWriteable(CloudCredential::new);
            } else {
                this.cloudCredential = null;
            }
        }

        public String getId() {
            return id;
        }

        public boolean isForce() {
            return force;
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
            out.writeBoolean(force);
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
            return Objects.hash(ackTimeout(), id, force);
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
            // cloudCredential is intentionally excluded: request-scoped secret carrier, not logical identity.
            return Objects.equals(id, other.id) && force == other.force && ackTimeout().equals(other.ackTimeout());
        }
    }
}
