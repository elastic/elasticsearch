/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class ReindexDataStreamIndexAction extends ActionType<ReindexDataStreamIndexAction.Response> {

    public static final String NAME = "indices:admin/migration/data_stream/index/reindex";

    public static final ActionType<Response> INSTANCE = new ReindexDataStreamIndexAction();

    private ReindexDataStreamIndexAction() {
        super(NAME);
    }

    public static class Request extends ActionRequest implements IndicesRequest {

        private final String sourceIndex;
        private final boolean deleteDestIfExists;

        public Request(String sourceIndex) {
            this(sourceIndex, true);
        }

        public Request(String sourceIndex, boolean deleteDestIfExists) {
            this.sourceIndex = sourceIndex;
            this.deleteDestIfExists = deleteDestIfExists;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.sourceIndex = in.readString();
            this.deleteDestIfExists = in.readBoolean();
        }

        public static Request readFrom(StreamInput in) throws IOException {
            return new Request(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sourceIndex);
            out.writeBoolean(deleteDestIfExists);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public String getSourceIndex() {
            return sourceIndex;
        }

        public boolean getDeleteDestIfExists() {
            return deleteDestIfExists;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(sourceIndex, request.sourceIndex) && deleteDestIfExists == request.deleteDestIfExists;
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceIndex, deleteDestIfExists);
        }

        @Override
        public String[] indices() {
            return new String[] { sourceIndex };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }
    }

    public static class Response extends ActionResponse {
        private final String destIndex;
        private final long numCreated;

        public Response(String destIndex, long numCreated) {
            this.destIndex = destIndex;
            this.numCreated = numCreated;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.destIndex = in.readString();
            this.numCreated = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(destIndex);
            out.writeLong(numCreated);
        }

        public String getDestIndex() {
            return destIndex;
        }

        public long getNumCreated() {
            return numCreated;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(destIndex, response.destIndex) && numCreated == response.numCreated;
        }

        @Override
        public int hashCode() {
            return Objects.hash(destIndex, numCreated);
        }
    }
}
