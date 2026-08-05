/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.List;

/**
 * Response sent from server to client indicating batch exchange completion status.
 * A {@code null} failure means success; a non-null failure means the server encountered an error.
 */
public final class BatchExchangeStatusResponse extends TransportResponse {
    private static final TransportVersion ESQL_LOOKUP_BYTES_READ = TransportVersion.fromName("esql_lookup_bytes_read");
    // Warnings ship as part of the same per-driver warnings feature as the DriverCompletionInfo warnings field.
    private static final TransportVersion ESQL_DRIVER_WARNINGS = TransportVersion.fromName("esql_driver_warnings");

    @Nullable
    private final Exception failure;
    private final long bytesRead;
    private final List<String> warnings;

    /**
     * Create a success response.
     */
    public BatchExchangeStatusResponse(long bytesRead, List<String> warnings) {
        this.failure = null;
        this.bytesRead = bytesRead;
        this.warnings = warnings == null ? List.of() : warnings;
    }

    /**
     * Create a failure response.
     */
    public BatchExchangeStatusResponse(Exception failure) {
        this.failure = failure;
        this.bytesRead = 0L;
        this.warnings = List.of();
    }

    public BatchExchangeStatusResponse(StreamInput in) throws IOException {
        this.failure = in.readOptionalException();
        this.bytesRead = in.getTransportVersion().supports(ESQL_LOOKUP_BYTES_READ) ? in.readVLong() : 0L;
        this.warnings = in.getTransportVersion().supports(ESQL_DRIVER_WARNINGS) ? in.readStringCollectionAsList() : List.of();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalException(failure);
        if (out.getTransportVersion().supports(ESQL_LOOKUP_BYTES_READ)) {
            out.writeVLong(bytesRead);
        }
        if (out.getTransportVersion().supports(ESQL_DRIVER_WARNINGS)) {
            out.writeStringCollection(warnings);
        }
    }

    public boolean isSuccess() {
        return failure == null;
    }

    public Exception getFailure() {
        return failure;
    }

    public long bytesRead() {
        return bytesRead;
    }

    /**
     * Warnings accumulated by the server-side lookup driver, replayed into the client driver's per-driver sink so
     * they reach the response chokepoint. Never {@code null}.
     */
    public List<String> warnings() {
        return warnings;
    }

}
