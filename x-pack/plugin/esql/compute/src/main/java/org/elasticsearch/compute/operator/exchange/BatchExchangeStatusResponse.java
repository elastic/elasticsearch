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
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorStatus;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
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
    /** Adds the optional server-driver profile summary to batch exchange responses and remote fetch operator status. */
    public static final TransportVersion ESQL_BATCH_EXCHANGE_PROFILE = TransportVersion.fromName("esql_batch_exchange_profile");

    @Nullable
    private final Exception failure;
    private final long bytesRead;
    private final List<String> warnings;
    @Nullable
    private final Profile profile;

    /**
     * Create a success response.
     */
    public BatchExchangeStatusResponse(long bytesRead, Collection<String> warnings) {
        this(bytesRead, warnings, null);
    }

    /**
     * Create a successful response, optionally including the server driver's profile summary.
     */
    public BatchExchangeStatusResponse(long bytesRead, Collection<String> warnings, @Nullable Profile profile) {
        this.failure = null;
        this.bytesRead = bytesRead;
        this.warnings = List.copyOf(warnings);
        this.profile = profile;
    }

    /**
     * Create a failure response.
     */
    public BatchExchangeStatusResponse(Exception failure) {
        this.failure = failure;
        this.bytesRead = 0L;
        this.warnings = List.of();
        this.profile = null;
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
        this.profile = in.getTransportVersion().supports(ESQL_BATCH_EXCHANGE_PROFILE) ? in.readOptionalWriteable(Profile::new) : null;
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
        if (out.getTransportVersion().supports(ESQL_BATCH_EXCHANGE_PROFILE)) {
            out.writeOptionalWriteable(profile);
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

    /**
     * Server-side driver profile summary, present only when profiling was requested.
     */
    @Nullable
    public Profile profile() {
        return profile;
    }

    /**
     * Compact profile of a batch exchange server driver and its source loading work.
     * <p>
     * {@code valuesLoaded} sums {@link OperatorStatus#valuesLoaded()} across every operator in the driver.
     * {@code fieldLoadNanos} and the {@code source*} fields cover only
     * {@link ValuesSourceReaderOperatorStatus} instances.
     */
    public record Profile(
        long driverTookNanos,
        long driverCpuNanos,
        long valuesLoaded,
        long fieldLoadNanos,
        long sourceDocsLoaded,
        long sourceFieldReads,
        long sourceBytesLoaded
    ) implements org.elasticsearch.common.io.stream.Writeable {

        public Profile(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
        }

        /**
         * Summarizes the driver and values-reader profiles needed to diagnose fetch latency.
         *
         * @param driverProfile completed driver profile
         * @param driverTookNanos elapsed time measured from client-ready driver dispatch rather than driver construction
         */
        public static Profile from(DriverProfile driverProfile, long driverTookNanos) {
            long valuesLoaded = 0L;
            long fieldLoadNanos = 0L;
            long sourceDocsLoaded = 0L;
            long sourceFieldReads = 0L;
            long sourceBytesLoaded = 0L;
            for (OperatorStatus operator : driverProfile.operators()) {
                valuesLoaded += operator.valuesLoaded();
                if (operator.status() instanceof ValuesSourceReaderOperatorStatus sourceReader) {
                    fieldLoadNanos += sourceReader.processNanos();
                    sourceDocsLoaded += sourceReader.sourceDocsLoaded();
                    sourceFieldReads += sourceReader.sourceFieldReads();
                    sourceBytesLoaded += sourceReader.sourceBytesLoaded();
                }
            }
            return new Profile(
                driverTookNanos,
                driverProfile.cpuNanos(),
                valuesLoaded,
                fieldLoadNanos,
                sourceDocsLoaded,
                sourceFieldReads,
                sourceBytesLoaded
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(driverTookNanos);
            out.writeVLong(driverCpuNanos);
            out.writeVLong(valuesLoaded);
            out.writeVLong(fieldLoadNanos);
            out.writeVLong(sourceDocsLoaded);
            out.writeVLong(sourceFieldReads);
            out.writeVLong(sourceBytesLoaded);
        }
    }

}
