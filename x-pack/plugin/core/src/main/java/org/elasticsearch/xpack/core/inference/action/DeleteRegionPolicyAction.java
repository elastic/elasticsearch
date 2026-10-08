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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Objects;

public class DeleteRegionPolicyAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteRegionPolicyAction INSTANCE = new DeleteRegionPolicyAction();
    public static final String NAME = "cluster:admin/xpack/inference/region_policy/delete";

    public DeleteRegionPolicyAction() {
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
            return Objects.hash(NAME);
        }
    }
}
