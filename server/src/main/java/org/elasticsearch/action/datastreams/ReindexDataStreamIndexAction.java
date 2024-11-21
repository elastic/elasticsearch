/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.datastreams;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class ReindexDataStreamIndexAction extends ActionType<ReindexDataStreamIndexAction.Response> {

    public static final String NAME = "indices:admin/data_stream/index/reindex";

    public static final ActionType<Response> INSTANCE = new ReindexDataStreamIndexAction();

    private ReindexDataStreamIndexAction() {
        super(NAME);
    }

    public static class Request extends ActionRequest {

        private final String sourceIndex;

        public Request(String sourceIndex) {
            this.sourceIndex = sourceIndex;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.sourceIndex = in.readString();
        }

        public static Request readFrom(StreamInput in) throws IOException {
            return new Request(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sourceIndex);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public String getSourceIndex() {
            return sourceIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(sourceIndex, request.sourceIndex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceIndex);
        }
    }

    public static class Response extends ActionResponse {
        private final String destIndex;

        public Response(String destIndex) {
            this.destIndex = destIndex;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.destIndex = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(destIndex);
        }

        public String getDestIndex() {
            return destIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(destIndex, response.destIndex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(destIndex);
        }
    }
}
