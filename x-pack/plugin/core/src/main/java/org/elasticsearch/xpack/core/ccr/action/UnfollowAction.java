/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class UnfollowAction extends Action<UnfollowAction.Response> {

    public static final UnfollowAction INSTANCE = new UnfollowAction();
    public static final String NAME = "indices:admin/xpack/ccr/unfollow";

    private UnfollowAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest {

        private final String followerIndex;

        public Request(String followerIndex) {
            this.followerIndex = followerIndex;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            followerIndex = in.readString();
        }

        public String getFollowerIndex() {
            return followerIndex;
        }

        @Override
        public String[] indices() {
            return new String[] {followerIndex};
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException e = null;
            if (followerIndex == null) {
                e = addValidationError("follower index is missing", e);
            }
            return e;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(followerIndex);
        }
    }

    public static class Response extends AcknowledgedResponse implements ToXContentObject {

        private boolean retentionLeasesRemoved;
        @Nullable private Exception retentionLeasesRemovalFailureCause;

        public Response() {

        }

        public Response(
                final boolean acknowledged,
                final boolean retentionLeasesRemoved,
                final Exception retentionLeasesRemovalFailureCause) {
            super(acknowledged);
            this.retentionLeasesRemoved = retentionLeasesRemoved;
            if (retentionLeasesRemoved && retentionLeasesRemovalFailureCause != null) {
                throw new IllegalArgumentException(
                        "there should not be a failure cause when retention leases are removed",
                        retentionLeasesRemovalFailureCause);
            } else if (retentionLeasesRemoved == false && retentionLeasesRemovalFailureCause == null) {
                throw new IllegalArgumentException("there should be a failure cause when retention leases are not removed");
            }
            this.retentionLeasesRemovalFailureCause = retentionLeasesRemovalFailureCause;
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
                retentionLeasesRemoved = in.readBoolean();
                if (retentionLeasesRemoved) {
                    retentionLeasesRemovalFailureCause = null;
                } else {
                    retentionLeasesRemovalFailureCause = in.readException();
                }
            } else {
                // the response is from an old version that did not attempt to remove retention leases, treat as success
                retentionLeasesRemoved = true;
                retentionLeasesRemovalFailureCause = null;
            }
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
                if (retentionLeasesRemoved) {
                    out.writeBoolean(true);
                } else {
                    out.writeBoolean(false);
                    assert retentionLeasesRemovalFailureCause != null;
                    out.writeException(retentionLeasesRemovalFailureCause);
                }
            }
        }

        @Override
        protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
            builder.field("acknowledged", acknowledged);
            builder.field("retention_leases_removed", retentionLeasesRemoved);
            if (retentionLeasesRemovalFailureCause != null) {
                builder.field("retention_leases_removal_failure_cause", retentionLeasesRemovalFailureCause);
            }
        }

        @Override
        public boolean equals(final Object that) {
            if (this == that) return true;
            if (that == null || getClass() != that.getClass()) return false;
            final UnfollowAction.Response response = (UnfollowAction.Response) that;
            return acknowledged == response.acknowledged &&
                    retentionLeasesRemoved == response.retentionLeasesRemoved &&
                    Objects.equals(retentionLeasesRemovalFailureCause, response.retentionLeasesRemovalFailureCause);
        }

        @Override
        public int hashCode() {
            return Objects.hash(acknowledged, retentionLeasesRemoved, retentionLeasesRemovalFailureCause);
        }

        @Override
        public String toString() {
            return "Response{" +
                    "acknowledged=" + acknowledged +
                    ", retentionLeasesRemoved=" + retentionLeasesRemoved +
                    ", retentionLeasesRemovalFailureCause=" + retentionLeasesRemovalFailureCause +
                    '}';
        }

    }

}
