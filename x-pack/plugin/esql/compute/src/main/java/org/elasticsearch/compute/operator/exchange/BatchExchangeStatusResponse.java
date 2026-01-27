/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Objects;

/**
 * Response sent from server to client indicating batch exchange completion status.
 * Contains success/failure information that the client uses to trigger onSuccess/onFailure.
 */
public final class BatchExchangeStatusResponse extends TransportResponse {
    private final boolean success;
    private final Exception failure;

    public BatchExchangeStatusResponse(boolean success, Exception failure) {
        this.success = success;
        this.failure = failure;
    }

    public BatchExchangeStatusResponse(StreamInput in) throws IOException {
        this.success = in.readBoolean();
        if (success) {
            this.failure = null;
        } else {
            this.failure = in.readException();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(success);
        if (success == false) {
            out.writeException(failure);
        }
    }

    public boolean isSuccess() {
        return success;
    }

    public Exception getFailure() {
        return failure;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BatchExchangeStatusResponse that = (BatchExchangeStatusResponse) o;
        return success == that.success && Objects.equals(failure, that.failure);
    }

    @Override
    public int hashCode() {
        return Objects.hash(success, failure);
    }
}
