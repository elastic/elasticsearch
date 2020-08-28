/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ilm.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;

import java.io.IOException;
import java.util.Objects;

public class DeleteLifecycleAction extends ActionType<DeleteLifecycleAction.Response> {
    public static final DeleteLifecycleAction INSTANCE = new DeleteLifecycleAction();
    public static final String NAME = "cluster:admin/ilm/delete";

    protected DeleteLifecycleAction() {
        super(NAME, DeleteLifecycleAction.Response::new);
    }

    public static class Response extends AcknowledgedResponse implements ToXContentObject {

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(boolean acknowledged) {
            super(acknowledged);
        }
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
