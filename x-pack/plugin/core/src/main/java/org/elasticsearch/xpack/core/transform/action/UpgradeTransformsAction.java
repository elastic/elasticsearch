/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.core.transform.transforms.TransformConfig.TRANSFORM_CLOUD_CREDENTIAL_ON_REQUEST;

public class UpgradeTransformsAction extends ActionType<UpgradeTransformsAction.Response> {

    public static final UpgradeTransformsAction INSTANCE = new UpgradeTransformsAction();
    public static final String NAME = "cluster:admin/transform/upgrade";

    private UpgradeTransformsAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements Releasable {

        private final boolean dryRun;

        // Caller's UIAM cloud credential carried on the request so it survives coordinator -> master
        // transport, where the AUTHENTICATING_CLOUD_TOKEN_THREAD_CONTEXT transient is no longer present.
        @Nullable
        private CloudCredential cloudCredential;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.dryRun = in.readBoolean();
            if (in.getTransportVersion().supports(TRANSFORM_CLOUD_CREDENTIAL_ON_REQUEST)) {
                this.cloudCredential = in.readOptionalWriteable(CloudCredential::new);
            } else {
                this.cloudCredential = null;
            }
        }

        public Request(boolean dryRun, TimeValue timeout) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, timeout);
            this.dryRun = dryRun;
        }

        public boolean isDryRun() {
            return dryRun;
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
            out.writeBoolean(dryRun);
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
            return Objects.hash(ackTimeout(), dryRun);
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
            return this.dryRun == other.dryRun && ackTimeout().equals(other.ackTimeout());
        }
    }

    public static class Response extends ActionResponse implements Writeable, ToXContentObject {

        private final long updated;
        private final long noAction;
        private final long needsUpdate;

        public Response(StreamInput in) throws IOException {
            updated = in.readVLong();
            noAction = in.readVLong();
            needsUpdate = in.readVLong();
        }

        public Response(long updated, long noAction, long needsUpdate) {
            if (updated < 0 || noAction < 0 || needsUpdate < 0) {
                throw new IllegalArgumentException("response counters must be > 0");
            }

            this.updated = updated;
            this.noAction = noAction;
            this.needsUpdate = needsUpdate;
        }

        public long getUpdated() {
            return updated;
        }

        public long getNoAction() {
            return noAction;
        }

        public long getNeedsUpdate() {
            return needsUpdate;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(updated);
            out.writeVLong(noAction);
            out.writeVLong(needsUpdate);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("updated", updated);
            builder.field("no_action", noAction);
            builder.field("needs_update", needsUpdate);
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
            Response other = (Response) obj;
            return this.updated == other.updated && this.noAction == other.noAction && this.needsUpdate == other.needsUpdate;
        }

        @Override
        public int hashCode() {
            return Objects.hash(updated, noAction, needsUpdate);
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }
    }
}
