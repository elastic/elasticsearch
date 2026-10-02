/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;
import java.util.Objects;

/**
 * Request sent from client to server to track batch exchange status.
 * Client sends this before page streaming starts, and server replies after streaming ends.
 * Though the order on the receiving end is not guaranteed
 * as streaming and BatchExchangeStatusRequest use different connections.
 */
public final class BatchExchangeStatusRequest extends AbstractTransportRequest {
    private final String exchangeId;

    public BatchExchangeStatusRequest(String exchangeId) {
        this.exchangeId = exchangeId;
    }

    public BatchExchangeStatusRequest(StreamInput in) throws IOException {
        super(in);
        this.exchangeId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(exchangeId);
    }

    public String exchangeId() {
        return exchangeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BatchExchangeStatusRequest that = (BatchExchangeStatusRequest) o;
        return exchangeId.equals(that.exchangeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(exchangeId);
    }
}
