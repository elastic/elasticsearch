/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

/**
 * Response sent from server to client indicating batch exchange completion status.
 * A {@code null} failure means success; a non-null failure means the server encountered an error.
 */
public final class BatchExchangeStatusResponse extends TransportResponse {
    @Nullable
    private final Exception failure;

    /**
     * Create a success response.
     */
    public BatchExchangeStatusResponse() {
        this.failure = null;
    }

    /**
     * Create a failure response.
     */
    public BatchExchangeStatusResponse(Exception failure) {
        this.failure = failure;
    }

    public BatchExchangeStatusResponse(StreamInput in) throws IOException {
        this.failure = in.readOptionalException();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalException(failure);
    }

    public boolean isSuccess() {
        return failure == null;
    }

    public Exception getFailure() {
        return failure;
    }

}
