/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class DeleteAutoscalingPolicyAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteAutoscalingPolicyAction INSTANCE = new DeleteAutoscalingPolicyAction();
    public static final String NAME = "cluster:admin/autoscaling/delete_autoscaling_policy";

    private DeleteAutoscalingPolicyAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    public static class Request extends AcknowledgedRequest<DeleteAutoscalingPolicyAction.Request> {

        private final String name;

        public String name() {
            return name;
        }

        public Request(final String name) {
            this.name = Objects.requireNonNull(name);
        }

        public Request(final StreamInput in) throws IOException {
            super(in);
            name = in.readString();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Request request = (Request) o;
            return name.equals(request.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

    }

}
