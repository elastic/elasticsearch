/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Objects;

public class DeleteViewAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteViewAction INSTANCE = new DeleteViewAction();
    public static final String NAME = "cluster:admin/xpack/esql/view/delete";

    private DeleteViewAction() {
        super(NAME);
    }

    public static class Request extends MasterNodeRequest<DeleteViewAction.Request> {
        private final String name;

        public Request(TimeValue masterNodeTimeout, String name) {
            super(masterNodeTimeout);
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

        public String name() {
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
