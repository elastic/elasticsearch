/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.eql.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

public class DeleteAsyncEqlSearchAction extends ActionType<AcknowledgedResponse> {
    public static final DeleteAsyncEqlSearchAction INSTANCE = new DeleteAsyncEqlSearchAction();
    public static final String NAME = "indices:data/read/eql/async_search/delete";

    private DeleteAsyncEqlSearchAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    @Override
    public Writeable.Reader<AcknowledgedResponse> getResponseReader() {
        return AcknowledgedResponse::new;
    }

    public static class Request extends ActionRequest {
        private final String id;

        public Request(String id) {
            this.id = id;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.id = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public String getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return id.equals(request.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }
}
