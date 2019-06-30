/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.enrich.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class ExecuteEnrichPolicyAction extends Action<AcknowledgedResponse> {

    public static final ExecuteEnrichPolicyAction INSTANCE = new ExecuteEnrichPolicyAction();
    public static final String NAME = "cluster:admin/xpack/enrich/execute";

    private ExecuteEnrichPolicyAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        private final String name;

        public Request(String name) {
            this.name = Objects.requireNonNull(name, "name cannot be null");
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            name = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
        }

        public String getName() {
            return name;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return name.equals(request.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }
    }
}
