/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStats;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Source operator that retrieves data from external sources (Iceberg tables, Parquet files, etc.).
 * Modeled after {@link org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator}.
 *
 * This operator uses an async pattern:
 * - Background thread reads from external source and fills a buffer
 * - Operator polls from buffer without blocking the Driver
 * - {@link #isBlocked()} signals when waiting for data
 */
public class AsyncExternalSourceOperator extends SourceOperator {

    private static final TransportVersion ESQL_CAPTURED_SOURCE_METADATA = TransportVersion.fromName("esql_captured_source_metadata");

    private final AsyncExternalSourceBuffer buffer;
    /** Node telemetry sink; {@link ExternalSourceMetrics#NOOP} when none is wired (tests, connector factory path). */
    private final ExternalSourceMetrics externalSourceMetrics;
    /** Low-cardinality storage scheme dimension for this scan's histograms; {@code null} when unknown (connector path). */
    private final String scheme;
    /**
     * Reference point for the time-to-first-row measurement, captured when this SCAN OPERATOR is constructed
     * (per driver, after planning and discovery) — NOT at query start. The measurement is therefore a per-scan
     * proxy: a query with several external-source scans records one observation per scan.
     */
    private final long operatorStartNanos = System.nanoTime();
    private IsBlockedResult isBlocked = NOT_BLOCKED;
    private int pagesEmitted;
    private long rowsEmitted;
    private long processNanos;

    public AsyncExternalSourceOperator(AsyncExternalSourceBuffer buffer) {
        this(buffer, ExternalSourceMetrics.NOOP, null);
    }

    public AsyncExternalSourceOperator(AsyncExternalSourceBuffer buffer, ExternalSourceMetrics externalSourceMetrics, String scheme) {
        this.buffer = buffer;
        this.externalSourceMetrics = externalSourceMetrics == null ? ExternalSourceMetrics.NOOP : externalSourceMetrics;
        this.scheme = scheme;
    }

    @Override
    public Page getOutput() {
        long startNanos = System.nanoTime();
        try {
            final var page = buffer.pollPage();
            if (page != null) {
                if (pagesEmitted == 0) {
                    // First page delivered: record time-to-first-row once. The record method self-guards (best-effort).
                    externalSourceMetrics.recordTimeToFirstRow((startNanos - operatorStartNanos) / 1_000_000, scheme);
                }
                pagesEmitted++;
                rowsEmitted += page.getPositionCount();
                return page;
            }
            if (buffer.failure() != null) {
                throw propagateFailure(buffer.failure());
            }
            return null;
        } finally {
            processNanos += System.nanoTime() - startNanos;
        }
    }

    private static RuntimeException propagateFailure(Throwable t) {
        // Classify the read failure so it surfaces with the right HTTP status (client/server/retryable)
        // instead of the previous blanket wrap into a bare RuntimeException, which always became a 500.
        // Classification must run co-located with the throw, before any serialization (see ExternalException
        // and ExternalFailures): a NotSerializableExceptionWrapper arriving here would mean the failure has
        // already crossed a node boundary, so the concrete type — and the chance to classify it — is lost.
        assert t instanceof NotSerializableExceptionWrapper == false
            : "external read failure reached classification already serialized: " + t.getClass().getName();
        return ExternalFailures.classify(t);
    }

    @Override
    public boolean isFinished() {
        // Keep "not finished" while a failure is pending so the driver calls getOutput() and the
        // exception propagates instead of treating the source as a clean EOF.
        return buffer.isFinished() && buffer.failure() == null;
    }

    @Override
    public void finish() {
        buffer.finish(true);
    }

    @Override
    public IsBlockedResult isBlocked() {
        if (isBlocked.listener().isDone()) {
            isBlocked = buffer.waitForReading();
            if (isBlocked.listener().isDone()) {
                isBlocked = NOT_BLOCKED;
            }
        }
        return isBlocked;
    }

    @Override
    public void close() {
        emitPendingWarnings();
        recordParseAndSplits();
        finish();
    }

    /**
     * Publishes the operator's final parse counters to node telemetry. Called once from {@link #close()} (the
     * driver guarantees a single close). Best-effort: an instrumentation failure must never break teardown.
     */
    private void recordParseAndSplits() {
        // Per-operator split count: this operator's own processed-split total, NOT buffer.splitsTotal() (which on
        // the slice-queue path is the GLOBAL sliceQueue.totalSlices() shared across every parallel operator — using
        // it would make each of N parallel operators record the whole-query total, inflating parse.splits_scanned).
        // The single-file / multi-file paths run one operator instance, so splitsProcessed converges to the same
        // value splitsTotal held there.
        long splitsProcessed = buffer.splitsProcessed();
        // Skip a scan that did no work (failed/empty open): recording parse/splits with all-zero values would
        // seed the parse/splits/duration histograms with zero observations. Mirrors the read-stall millis<=0
        // guard rationale — an empty scan is not a data point about parse throughput or split fan-out.
        if (rowsEmitted == 0 && splitsProcessed == 0) {
            return;
        }
        FormatReaderStatus formatReaderStatus = buffer.formatReaderStatus();
        long readNanos = formatReaderStatus == null ? 0L : formatReaderStatus.readNanos();
        // Both record methods self-guard (best-effort): an instrumentation failure cannot break teardown.
        externalSourceMetrics.recordParse(rowsEmitted, TimeUnit.NANOSECONDS.toMillis(readNanos), scheme);
        externalSourceMetrics.recordSplitsScanned(splitsProcessed, scheme);
    }

    /**
     * Drains the buffer's recorded partial-results warnings and re-emits them via {@link HeaderWarning}.
     * The driver invokes {@link #close()} on its own thread during teardown — the same thread whose
     * response headers {@code DriverRunner} collects into the client response. The producer records these
     * off a forked reader / parse-worker thread whose own response headers are never merged back, so the
     * re-emission must happen here, on the driver thread, for the warning to reach the client (see #835).
     * This mirrors how {@link org.elasticsearch.compute.operator.AsyncOperator} flushes a
     * {@code ResponseHeadersCollector} from its {@code close()}.
     */
    private void emitPendingWarnings() {
        String warning;
        while ((warning = buffer.pollWarning()) != null) {
            HeaderWarning.addWarning(warning);
        }
    }

    @Override
    public String toString() {
        // Profile display name. The class keeps the implementation-detail `Async` prefix to mirror
        // the AsyncOperator inheritance, but the public-facing profile output drops it so the
        // observable operator name reads cleanly to users.
        return "ExternalDataSourceOperator";
    }

    @Override
    public Status status() {
        FormatReaderStatus formatReaderStatus = buffer.formatReaderStatus();
        // Lift format-reader read_nanos to the operator top level for rollup.
        long readNanos = formatReaderStatus == null ? 0L : formatReaderStatus.readNanos();
        return new Status(
            buffer.size(),
            pagesEmitted,
            rowsEmitted,
            buffer.bytesInBuffer(),
            buffer.failure(),
            processNanos,
            buffer.splitsProcessed(),
            buffer.splitsTotal(),
            buffer.currentSplit(),
            buffer.bytesRead(),
            readNanos,
            formatReaderStatus,
            buffer.capturedSourceMetadataSnapshot(),
            buffer.isPartial()
        );
    }

    public static class Status implements Operator.Status, org.elasticsearch.compute.operator.CapturingExternalSourceStatus {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "async_external_source",
            Status::new
        );

        private static final TransportVersion ESQL_ASYNC_SOURCE_BYTES_BUFFERED = TransportVersion.fromName(
            "esql_async_source_bytes_buffered"
        );

        private static final TransportVersion ESQL_EXTERNAL_SOURCE_PROFILE = TransportVersion.fromName("esql_external_source_profile");

        private static final TransportVersion ESQL_EXTERNAL_PARTIAL_RESULTS = TransportVersion.fromName("esql_external_partial_results");

        private final int pagesWaiting;
        private final int pagesEmitted;
        private final long rowsEmitted;
        private final long bytesBuffered;
        private final Throwable failure;
        private final long processNanos;
        private final int splitsProcessed;
        private final int splitsTotal;
        private final int currentSplit;
        private final long bytesRead;
        private final long readNanos;
        private final FormatReaderStatus formatReader;
        private final Map<String, List<Map<String, Object>>> capturedSourceMetadata;
        private final boolean partial;

        Status(
            int pagesWaiting,
            int pagesEmitted,
            long rowsEmitted,
            long bytesBuffered,
            Throwable failure,
            long processNanos,
            int splitsProcessed,
            int splitsTotal,
            int currentSplit,
            long bytesRead,
            long readNanos,
            FormatReaderStatus formatReader,
            Map<String, List<Map<String, Object>>> capturedSourceMetadata,
            boolean partial
        ) {
            this.pagesWaiting = pagesWaiting;
            this.pagesEmitted = pagesEmitted;
            this.rowsEmitted = rowsEmitted;
            this.bytesBuffered = bytesBuffered;
            this.failure = failure;
            this.processNanos = processNanos;
            this.splitsProcessed = splitsProcessed;
            this.splitsTotal = splitsTotal;
            this.currentSplit = currentSplit;
            this.bytesRead = bytesRead;
            this.readNanos = readNanos;
            this.formatReader = formatReader;
            this.capturedSourceMetadata = capturedSourceMetadata == null ? Map.of() : capturedSourceMetadata;
            this.partial = partial;
        }

        Status(StreamInput in) throws IOException {
            pagesWaiting = in.readVInt();
            pagesEmitted = in.readVInt();
            rowsEmitted = in.readVLong();
            bytesBuffered = in.getTransportVersion().supports(ESQL_ASYNC_SOURCE_BYTES_BUFFERED) ? in.readVLong() : 0;
            failure = in.readException();
            if (in.getTransportVersion().supports(ESQL_EXTERNAL_SOURCE_PROFILE)) {
                processNanos = in.readVLong();
                splitsProcessed = in.readVInt();
                splitsTotal = in.readVInt();
                currentSplit = in.readVInt();
                bytesRead = in.readVLong();
                readNanos = in.readVLong();
                formatReader = in.readOptionalNamedWriteable(FormatReaderStatus.class);
            } else {
                processNanos = 0L;
                splitsProcessed = 0;
                splitsTotal = 0;
                currentSplit = 0;
                bytesRead = 0L;
                readNanos = 0L;
                formatReader = null;
            }
            if (in.getTransportVersion().supports(ESQL_CAPTURED_SOURCE_METADATA)) {
                int n = in.readVInt();
                if (n == 0) {
                    capturedSourceMetadata = Map.of();
                } else {
                    Map<String, List<Map<String, Object>>> tmp = new HashMap<>(n);
                    for (int i = 0; i < n; i++) {
                        String path = in.readString();
                        int contributionCount = in.readVInt();
                        List<Map<String, Object>> contributions = new ArrayList<>(contributionCount);
                        for (int j = 0; j < contributionCount; j++) {
                            contributions.add(in.readGenericMap());
                        }
                        tmp.put(path, contributions);
                    }
                    capturedSourceMetadata = tmp;
                }
            } else {
                capturedSourceMetadata = Map.of();
            }
            partial = in.getTransportVersion().supports(ESQL_EXTERNAL_PARTIAL_RESULTS) && in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(pagesWaiting);
            out.writeVInt(pagesEmitted);
            out.writeVLong(rowsEmitted);
            if (out.getTransportVersion().supports(ESQL_ASYNC_SOURCE_BYTES_BUFFERED)) {
                out.writeVLong(bytesBuffered);
            }
            out.writeException(failure);
            if (out.getTransportVersion().supports(ESQL_EXTERNAL_SOURCE_PROFILE)) {
                out.writeVLong(processNanos);
                out.writeVInt(splitsProcessed);
                out.writeVInt(splitsTotal);
                out.writeVInt(currentSplit);
                out.writeVLong(bytesRead);
                out.writeVLong(readNanos);
                out.writeOptionalNamedWriteable(formatReader);
            }
            if (out.getTransportVersion().supports(ESQL_CAPTURED_SOURCE_METADATA)) {
                out.writeVInt(capturedSourceMetadata.size());
                for (Map.Entry<String, List<Map<String, Object>>> e : capturedSourceMetadata.entrySet()) {
                    out.writeString(e.getKey());
                    List<Map<String, Object>> contributions = e.getValue();
                    out.writeVInt(contributions.size());
                    for (Map<String, Object> contribution : contributions) {
                        out.writeGenericMap(contribution);
                    }
                }
            }
            if (out.getTransportVersion().supports(ESQL_EXTERNAL_PARTIAL_RESULTS)) {
                out.writeBoolean(partial);
            }
        }

        @Override
        public Map<String, List<Map<String, Object>>> capturedSourceMetadata() {
            return capturedSourceMetadata;
        }

        /**
         * Number of canonical-stripe contributions this scan harvested across all files — the
         * cold-harvest vs miss signal. A scan that committed stripes ({@code > 0}) has populated the
         * coordinator's stats so a future identical query can short-circuit warm; a scan that committed
         * none ({@code == 0}, e.g. a safe-miss / harvest-scope-none re-scan) leaves the next query cold.
         * <p>
         * Derived from {@link #capturedSourceMetadata} rather than carried as a separate wire field: a
         * stripe-addressed contribution is exactly one carrying {@link ExternalStats#STRIPE_ORDINAL_KEY}
         * (with {@link ExternalStats#STRIPE_SIZE_KEY}, which marks it cacheable). Counts every such
         * fragment, including sibling fragments of the same stripe, since each is a committed unit of
         * harvest work; this is a "did harvest happen, and how much" signal, not the deduped final
         * stripe count.
         */
        public long stripesCommitted() {
            long count = 0;
            for (List<Map<String, Object>> contributions : capturedSourceMetadata.values()) {
                for (Map<String, Object> contribution : contributions) {
                    if (contribution.containsKey(ExternalStats.STRIPE_ORDINAL_KEY)
                        && contribution.containsKey(ExternalStats.STRIPE_SIZE_KEY)) {
                        count++;
                    }
                }
            }
            return count;
        }

        @Override
        public boolean partial() {
            return partial;
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public int pagesWaiting() {
            return pagesWaiting;
        }

        public int pagesEmitted() {
            return pagesEmitted;
        }

        @Override
        public long rowsEmitted() {
            return rowsEmitted;
        }

        public long bytesBuffered() {
            return bytesBuffered;
        }

        public Throwable failure() {
            return failure;
        }

        /**
         * Wall time spent inside {@link AsyncExternalSourceOperator#getOutput()}'s read loop.
         * Producer-thread time (format-reader open, decode, decompression) lives in
         * {@code format_reader.read_nanos} on this same Status, not in this counter.
         */
        public long processNanos() {
            return processNanos;
        }

        public int splitsProcessed() {
            return splitsProcessed;
        }

        public int splitsTotal() {
            return splitsTotal;
        }

        public int currentSplit() {
            return currentSplit;
        }

        @Override
        public long bytesRead() {
            return bytesRead;
        }

        @Override
        public long readNanos() {
            return readNanos;
        }

        public FormatReaderStatus formatReader() {
            return formatReader;
        }

        /**
         * Projects the operator's existing {@code rowsEmitted} counter into the
         * {@link Operator.Status#documentsFound()} contract so external-source-emitted
         * rows aggregate into the top-level {@code documents_found} of the ES|QL response
         * alongside Lucene-sourced operators, without introducing a new wire field.
         */
        @Override
        public long documentsFound() {
            return rowsEmitted;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("pages_waiting", pagesWaiting);
            builder.field("pages_emitted", pagesEmitted);
            builder.field("rows_emitted", rowsEmitted);
            builder.field("bytes_buffered", bytesBuffered);
            builder.field("process_nanos", processNanos);
            builder.field("splits_processed", splitsProcessed);
            builder.field("splits_total", splitsTotal);
            builder.field("current_split", currentSplit);
            builder.field("bytes_read", bytesRead);
            builder.field("read_nanos", readNanos);
            builder.field("stripes_committed", stripesCommitted());
            builder.field("partial", partial);
            builder.startObject("format_reader");
            if (formatReader != null) {
                formatReader.toXContent(builder, params);
            }
            builder.endObject();
            if (failure != null) {
                builder.field("failure", failure.getMessage());
            }
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Status status = (Status) o;
            String thisFailureMsg = failure != null ? failure.getMessage() : null;
            String otherFailureMsg = status.failure != null ? status.failure.getMessage() : null;
            return pagesWaiting == status.pagesWaiting
                && pagesEmitted == status.pagesEmitted
                && rowsEmitted == status.rowsEmitted
                && bytesBuffered == status.bytesBuffered
                && processNanos == status.processNanos
                && splitsProcessed == status.splitsProcessed
                && splitsTotal == status.splitsTotal
                && currentSplit == status.currentSplit
                && bytesRead == status.bytesRead
                && readNanos == status.readNanos
                && partial == status.partial
                && Objects.equals(formatReader, status.formatReader)
                && Objects.equals(thisFailureMsg, otherFailureMsg)
                && Objects.equals(capturedSourceMetadata, status.capturedSourceMetadata);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                pagesWaiting,
                pagesEmitted,
                rowsEmitted,
                bytesBuffered,
                failure != null ? failure.getMessage() : null,
                processNanos,
                splitsProcessed,
                splitsTotal,
                currentSplit,
                bytesRead,
                readNanos,
                formatReader,
                capturedSourceMetadata,
                partial
            );
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.minimumCompatible();
        }
    }
}
