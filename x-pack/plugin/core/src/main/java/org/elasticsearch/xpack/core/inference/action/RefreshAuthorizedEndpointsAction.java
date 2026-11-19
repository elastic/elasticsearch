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

import java.io.IOException;
import java.util.Objects;

/**
 * Internal action that triggers an Elastic Inference Service authorization refresh. This action
 * performs the work (call the Elastic Inference Service, delete removed endpoints, persist
 * new/changed endpoints).
 */
public class RefreshAuthorizedEndpointsAction extends ActionType<ActionResponse.Empty> {

    public static final RefreshAuthorizedEndpointsAction INSTANCE = new RefreshAuthorizedEndpointsAction();

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
}
