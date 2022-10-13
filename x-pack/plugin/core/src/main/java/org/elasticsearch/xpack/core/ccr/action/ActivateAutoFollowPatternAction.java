/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class ActivateAutoFollowPatternAction extends ActionType<AcknowledgedResponse> {

    public static final String NAME = "cluster:admin/xpack/ccr/auto_follow_pattern/activate";
    public static final ActivateAutoFollowPatternAction INSTANCE = new ActivateAutoFollowPatternAction();

    private ActivateAutoFollowPatternAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final String name;
        private final boolean active;

        public Request(final String name, final boolean active) {
            this.name = name;
            this.active = active;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (name == null) {
                validationException = addValidationError("[name] is missing", validationException);
            }
            return validationException;
        }

        public String getName() {
            return name;
        }

        public boolean isActive() {
            return active;
        }

        public Request(final StreamInput in) throws IOException {
            super(in);
            name = in.readString();
            active = in.readBoolean();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            out.writeBoolean(active);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return active == request.active && Objects.equals(name, request.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, active);
        }
    }
}
