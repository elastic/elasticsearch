/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Objects;

public class SwapDataStreamIndexAction extends ActionType<SwapDataStreamIndexAction.Response> {

    public static final String NAME = "indices:admin/data_stream/index/swap";

    public static final ActionType<Response> INSTANCE = new SwapDataStreamIndexAction();

    private SwapDataStreamIndexAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest {

        private final String dataStream;
        private final String oldIndex;
        private final String newIndex;

        public Request(String dataStream, String oldIndex, String newIndex) {
            super(TimeValue.MAX_VALUE, TimeValue.MAX_VALUE);
            this.dataStream = dataStream;
            this.oldIndex = oldIndex;
            this.newIndex = newIndex;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.dataStream = in.readString();
            this.oldIndex = in.readString();
            this.newIndex = in.readString();
        }

        public static Request readFrom(StreamInput in) throws IOException {
            return new Request(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(dataStream);
            out.writeString(oldIndex);
            out.writeString(newIndex);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public String getDataStream() {
            return dataStream;
        }

        public String getOldIndex() {
            return oldIndex;
        }

        public String getNewIndex() {
            return newIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(dataStream, request.dataStream);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataStream);
        }

        @Override
        public String[] indices() {
            return new String[] { dataStream };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
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
