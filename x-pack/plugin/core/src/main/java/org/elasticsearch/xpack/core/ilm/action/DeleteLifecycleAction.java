/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class DeleteLifecycleAction extends ActionType<AcknowledgedResponse> {
    public static final DeleteLifecycleAction INSTANCE = new DeleteLifecycleAction();
    public static final String NAME = "cluster:admin/ilm/delete";

    protected DeleteLifecycleAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        public static final ParseField POLICY_FIELD = new ParseField("policy");

        private String policyName;

        public Request(String policyName) {
            this.policyName = policyName;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            policyName = in.readString();
        }

        public Request() {
        }

        public String getPolicyName() {
            return policyName;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(policyName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(policyName);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(policyName, other.policyName);
        }

    }

}
