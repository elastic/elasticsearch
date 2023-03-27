/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Objects;

public final class ExchangeRequest extends TransportRequest {
    private final boolean sourcesFinished;

    public ExchangeRequest(boolean sourcesFinished) {
        this.sourcesFinished = sourcesFinished;
    }

    public ExchangeRequest(StreamInput in) throws IOException {
        super(in);
        this.sourcesFinished = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(sourcesFinished);
    }

    /**
     * True if the {@link ExchangeSourceHandler} has enough input.
     * The corresponding {@link ExchangeSinkHandler} can drain pages and finish itself.
     */
    public boolean sourcesFinished() {
        return sourcesFinished;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExchangeRequest that = (ExchangeRequest) o;
        return sourcesFinished == that.sourcesFinished;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourcesFinished);
    }
}
