/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class GetRerankerWindowSizeAction extends ActionType<GetRerankerWindowSizeAction.Response> {

    public static final GetRerankerWindowSizeAction INSTANCE = new GetRerankerWindowSizeAction();
    public static final String NAME = "cluster:internal/xpack/inference/rerankwindowsize/get";

    public GetRerankerWindowSizeAction() {
        super(NAME);
    }

    public static class Request extends ActionRequest {

        private final String inferenceEntityId;

        public Request(String inferenceEntityId) {
            this.inferenceEntityId = inferenceEntityId;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.inferenceEntityId = in.readString();
        }

        public String getInferenceEntityId() {
            return inferenceEntityId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(inferenceEntityId);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(inferenceEntityId, request.inferenceEntityId);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(inferenceEntityId);
        }
    }

    public static class Response extends ActionResponse {

        private final int windowSize;

        public Response(int windowSize) {
            this.windowSize = windowSize;
        }

        public Response(StreamInput in) throws IOException {
            this.windowSize = in.readVInt();
        }

        public int getWindowSize() {
            return windowSize;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(windowSize);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return windowSize == response.windowSize;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(windowSize);
        }
    }
}
