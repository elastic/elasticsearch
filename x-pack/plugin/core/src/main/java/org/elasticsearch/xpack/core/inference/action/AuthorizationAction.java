/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Internal action that triggers an EIS authorization refresh and returns the set of inference endpoints
 * that were stored as a result. This action encapsulates the decision logic (should we send a request?)
 * and the work (call EIS, delete removed endpoints, persist new/changed endpoints). It is invoked
 * node-locally by {@code AuthorizationPoller} via the node client.
 */
public class AuthorizationAction extends ActionType<StoreInferenceEndpointsAction.Response> {

    public static final AuthorizationAction INSTANCE = new AuthorizationAction();
    public static final String NAME = "cluster:internal/xpack/inference/refresh_authorized_endpoints";

    public AuthorizationAction() {
        super(NAME);
    }

    public static class Request extends ActionRequest {

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }
}
