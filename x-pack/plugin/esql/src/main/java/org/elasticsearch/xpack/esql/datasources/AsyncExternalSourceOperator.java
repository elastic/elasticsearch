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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

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

    private final AsyncExternalSourceBuffer buffer;
    private IsBlockedResult isBlocked = NOT_BLOCKED;
    private int pagesEmitted;
    private long rowsEmitted;
    private long processNanos;

    public AsyncExternalSourceOperator(AsyncExternalSourceBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public Page getOutput() {
        long startNanos = System.nanoTime();
        try {
            final var page = buffer.pollPage();
            if (page != null) {
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
        if (t instanceof RuntimeException re) {
            return re;
        }
        if (t instanceof Error e) {
            throw e;
        }
        return new RuntimeException(t);
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
        finish();
    }

    @Override
    public String toString() {
        return "AsyncExternalSourceOperator";
    }

    @Override
    public Status status() {
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
            buffer.formatReaderStatus()
        );
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "async_external_source",
            Status::new
        );

        private static final TransportVersion ESQL_ASYNC_SOURCE_BYTES_BUFFERED = TransportVersion.fromName(
            "esql_async_source_bytes_buffered"
        );

        private static final TransportVersion ESQL_EXTERNAL_SOURCE_TELEMETRY = TransportVersion.fromName("esql_external_source_telemetry");

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
        private final Map<String, Object> formatReader;

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
            Map<String, Object> formatReader
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
            // Null-default lives at AsyncExternalSourceBuffer (field initialiser + setter normalisation);
            // the StreamInput ctor below also defends the wire path independently.
            this.formatReader = formatReader;
        }

        Status(StreamInput in) throws IOException {
            pagesWaiting = in.readVInt();
            pagesEmitted = in.readVInt();
            rowsEmitted = in.readVLong();
            bytesBuffered = in.getTransportVersion().supports(ESQL_ASYNC_SOURCE_BYTES_BUFFERED) ? in.readVLong() : 0;
            failure = in.readException();
            if (in.getTransportVersion().supports(ESQL_EXTERNAL_SOURCE_TELEMETRY)) {
                processNanos = in.readVLong();
                splitsProcessed = in.readVInt();
                splitsTotal = in.readVInt();
                currentSplit = in.readVInt();
                bytesRead = in.readVLong();
                Map<String, Object> read = in.readGenericMap();
                formatReader = read == null ? Map.of() : read;
            } else {
                processNanos = 0L;
                splitsProcessed = 0;
                splitsTotal = 0;
                currentSplit = 0;
                bytesRead = 0L;
                formatReader = Map.of();
            }
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
            if (out.getTransportVersion().supports(ESQL_EXTERNAL_SOURCE_TELEMETRY)) {
                out.writeVLong(processNanos);
                out.writeVInt(splitsProcessed);
                out.writeVInt(splitsTotal);
                out.writeVInt(currentSplit);
                out.writeVLong(bytesRead);
                out.writeGenericMap(formatReader);
            }
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
         * {@code format_reader.total_read_nanos} on this same Status, not in this counter.
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

        public long bytesRead() {
            return bytesRead;
        }

        public Map<String, Object> formatReader() {
            return formatReader;
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
            builder.field("format_reader", formatReader);
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
                && formatReader.equals(status.formatReader)
                && Objects.equals(thisFailureMsg, otherFailureMsg);
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
                formatReader
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
