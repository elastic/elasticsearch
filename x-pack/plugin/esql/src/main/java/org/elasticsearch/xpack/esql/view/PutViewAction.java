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

public class PutViewAction extends ActionType<AcknowledgedResponse> {

    public static final PutViewAction INSTANCE = new PutViewAction();
    public static final String NAME = "cluster:admin/xpack/esql/view/put";

    private PutViewAction() {
        super(NAME);
    }

    public static class Request extends MasterNodeRequest<PutViewAction.Request> {
        private final String name;
        private final View view;

        public Request(TimeValue masterNodeTimeout, String name, View view) {
            super(masterNodeTimeout);
            this.name = Objects.requireNonNull(name, "name cannot be null");
            this.view = view;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            name = in.readString();
            view = new View(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            view.writeTo(out);
        }

        public String name() {
            return name;
        }

        public View view() {
            return view;
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
            return name.equals(request.name) && view.equals(request.view);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, view);
        }
    }
}
