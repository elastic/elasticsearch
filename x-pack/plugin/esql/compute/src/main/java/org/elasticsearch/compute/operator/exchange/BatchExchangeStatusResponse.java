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
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Collection;
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
    public BatchExchangeStatusResponse(long bytesRead, Collection<String> warnings) {
        this.failure = null;
        this.bytesRead = bytesRead;
        this.warnings = List.copyOf(warnings);
    }

    /**
     * Create a failure response.
     */
    public BatchExchangeStatusResponse(Exception failure) {
        this.failure = failure;
        this.bytesRead = 0L;
        this.warnings = List.of();
    }

    public BatchExchangeStatusResponse(StreamInput in, ThreadContext threadContext) throws IOException {
        this.failure = in.readOptionalException();
        this.bytesRead = in.getTransportVersion().supports(ESQL_LOOKUP_BYTES_READ) ? in.readVLong() : 0L;
        if (in.getTransportVersion().supports(ESQL_DRIVER_WARNINGS)) {
            this.warnings = in.readStringCollectionAsList();
        } else {
            // Old nodes send warnings as transport response headers; the transport layer has already deposited
            // them into the current thread's context before this constructor is called.
            // Parse the RFC 7234 warning format (e.g. "299 Elasticsearch-9.5.0 \"message\"") to extract the
            // plain warning text, so the strings are in the same format as those sent by new nodes.
            this.warnings = threadContext.takeResponseHeaders("Warning")
                .stream()
                .map(s -> HeaderWarning.decodeAndUnescape(HeaderWarning.extractWarningValueFromWarningHeader(s, false)))
                .toList();
        }
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
     * Warnings accumulated by the lookup-side lookup driver. Never {@code null}.
     */
    public List<String> warnings() {
        return warnings;
    }

}
