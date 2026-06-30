/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * Internal action that triggers an Elastic Inference Service authorization refresh. This action encapsulates the decision
 * logic (should we send a request?) and the work (call the Elastic Inference Service, delete removed endpoints, persist
 * new/changed endpoints).
 */
public class RefreshAuthorizedEndpointsAction extends ActionType<RefreshAuthorizedEndpointsAction.Response> {

    public static final RefreshAuthorizedEndpointsAction INSTANCE = new RefreshAuthorizedEndpointsAction();
    public static final RefreshAuthorizedEndpointsAction.Response REFRESHED_RESPONSE = new RefreshAuthorizedEndpointsAction.Response(
        RefreshAuthorizedEndpointsAction.Response.Status.REFRESHED
    );

    public static final String NAME = "cluster:internal/xpack/inference/refresh_authorized_endpoints";

    public RefreshAuthorizedEndpointsAction() {
        super(NAME);
    }

    public static class Request extends ActionRequest {

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return true;
        }

        @Override
        public int hashCode() {
            // The class doesn't have any members at the moment so return the same hash code
            return Objects.hash(NAME);
        }
    }

    public static class Response extends ActionResponse {

        public enum Status {
            /**
             * The authorization refresh ran normally (either endpoints were stored, or the request
             * was skipped because preconditions were not met). The poller should keep polling.
             */
            REFRESHED,
            /**
             * CCM is supported in this environment but is currently disabled. The poller should
             * permanently complete the persistent task rather than keep polling.
             */
            CCM_DISABLED
        }

        private final Status status;

        public Response(Status status) {
            this.status = Objects.requireNonNull(status);
        }

        public Response(StreamInput in) throws IOException {
            this.status = in.readEnum(Status.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(status);
        }

        public Status status() {
            return status;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            var other = (Response) o;
            return status == other.status;
        }

        @Override
        public int hashCode() {
            return Objects.hash(status);
        }
    }
}
