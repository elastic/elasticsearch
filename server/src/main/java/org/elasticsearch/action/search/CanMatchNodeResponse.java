/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.CanMatchShardResponse;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.List;

public class CanMatchNodeResponse extends TransportResponse {

    private final List<ResponseOrFailure> responses;

    public CanMatchNodeResponse(StreamInput in) throws IOException {
        responses = in.readCollectionAsList(ResponseOrFailure::new);
    }

    public CanMatchNodeResponse(List<ResponseOrFailure> responses) {
        this.responses = responses;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(responses);
    }

    public List<ResponseOrFailure> getResponses() {
        return responses;
    }

    public static class ResponseOrFailure implements Writeable {

        public ResponseOrFailure(CanMatchShardResponse response) {
            this.response = response;
            this.exception = null;
        }

        public ResponseOrFailure(Exception exception) {
            this.exception = exception;
            this.response = null;
        }

        @Nullable
        public CanMatchShardResponse getResponse() {
            return response;
        }

        @Nullable
        public Exception getException() {
            return exception;
        }

        private final CanMatchShardResponse response;
        private final Exception exception;

        public ResponseOrFailure(StreamInput in) throws IOException {
            if (in.readBoolean()) {
                response = new CanMatchShardResponse(in);
                exception = null;
            } else {
                exception = in.readException();
                response = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            final boolean hasResponse = response != null;
            out.writeBoolean(hasResponse);
            if (hasResponse) {
                response.writeTo(out);
            } else {
                out.writeException(exception);
            }
        }
    }
}
