/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.View;
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

    public static class Request extends AcknowledgedRequest<Request> {
        private final View view;

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, View view) {
            super(masterNodeTimeout, ackTimeout);
            this.view = view;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            view = new View(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            view.writeTo(out);
        }

        public View view() {
            return view;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return view.equals(request.view);
        }

        @Override
        public int hashCode() {
            return Objects.hash(view);
        }
    }
}
