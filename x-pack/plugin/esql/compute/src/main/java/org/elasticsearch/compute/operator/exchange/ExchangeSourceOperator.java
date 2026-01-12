/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Source operator implementation that retrieves data from an {@link ExchangeSource}
 */
public class ExchangeSourceOperator extends SourceOperator {
    private static final Logger logger = LogManager.getLogger(ExchangeSourceOperator.class);

    private final ExchangeSource source;
    private final String driverName; // Identifies the driver for logging
    private IsBlockedResult isBlocked = NOT_BLOCKED;
    private int pagesEmitted;
    private long rowsEmitted;

    public record ExchangeSourceOperatorFactory(Supplier<ExchangeSource> exchangeSources, String driverName)
        implements
            SourceOperatorFactory {

        public ExchangeSourceOperatorFactory(Supplier<ExchangeSource> exchangeSources) {
            this(exchangeSources, "unknown-driver");
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new ExchangeSourceOperator(exchangeSources.get(), driverName);
        }

        @Override
        public String describe() {
            return "ExchangeSourceOperator[]";
        }
    }

    public ExchangeSourceOperator(ExchangeSource source) {
        this(source, "unknown-driver");
    }

    public ExchangeSourceOperator(ExchangeSource source, String driverName) {
        this.source = source;
        this.driverName = driverName;
    }

    @Override
    public Page getOutput() {
        final var page = source.pollPage();
        // Only log for BatchPages (marker pages) which are only used by client driver
        // Regular pages are used by both client and server drivers, so skip logging to reduce noise
        if (page != null) {
            pagesEmitted++;
            rowsEmitted += page.getPositionCount();
        }
        return page;
    }

    @Override
    public boolean isFinished() {
        boolean finished = source.isFinished();
        // Only log when there are pages in buffer but source is finished (critical for deadlock detection)
        // This scenario is only relevant for client driver with BatchPages
        int bufferSize = source.bufferSize();
        if (finished && bufferSize > 0) {
            logger.info(
                "[{}] ExchangeSourceOperator.isFinished() returning true BUT bufferSize={} (potential deadlock)",
                driverName,
                bufferSize
            );
        }
        return finished;
    }

    @Override
    public void finish() {
        source.finish();
    }

    @Override
    public IsBlockedResult isBlocked() {
        if (isBlocked.listener().isDone()) {
            isBlocked = source.waitForReading();
            // Only log when blocked and source is finished but buffer has pages (critical for deadlock)
            boolean isBlockedNow = isBlocked.listener().isDone() == false;
            if (isBlockedNow) {
                int bufferSize = source.bufferSize();
                boolean sourceFinished = source.isFinished();
                if (sourceFinished && bufferSize > 0) {
                    logger.info(
                        "[{}] ExchangeSourceOperator.isBlocked() returning BLOCKED but bufferSize={} and sourceFinished={} "
                            + "(potential deadlock)",
                        driverName,
                        bufferSize,
                        sourceFinished
                    );
                }
            }
            if (isBlocked.listener().isDone()) {
                isBlocked = NOT_BLOCKED;
            }
        }
        return isBlocked;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        // Source operators should return false - their data production is gated by nextOp.needsInput().
        // Even if there are buffered pages, if downstream doesn't need them (e.g., blocked on async),
        // we should wait, not busy-spin.
        return false;
    }

    /**
     * Get the current buffer size (number of pages waiting to be consumed).
     * Used for debugging and deadlock diagnosis.
     */
    public int bufferSize() {
        return source.bufferSize();
    }

    @Override
    public void close() {
        finish();
    }

    @Override
    public String toString() {
        return "ExchangeSourceOperator";
    }

    @Override
    public Status status() {
        return new Status(source.bufferSize(), pagesEmitted, rowsEmitted);
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "exchange_source",
            Status::new
        );

        private final int pagesWaiting;
        private final int pagesEmitted;
        private final long rowsEmitted;

        Status(int pagesWaiting, int pagesEmitted, long rowsEmitted) {
            this.pagesWaiting = pagesWaiting;
            this.pagesEmitted = pagesEmitted;
            this.rowsEmitted = rowsEmitted;
        }

        Status(StreamInput in) throws IOException {
            pagesWaiting = in.readVInt();
            pagesEmitted = in.readVInt();
            rowsEmitted = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(pagesWaiting);
            out.writeVInt(pagesEmitted);
            out.writeVLong(rowsEmitted);
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

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("pages_waiting", pagesWaiting);
            builder.field("pages_emitted", pagesEmitted);
            builder.field("rows_emitted", rowsEmitted);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return pagesWaiting == status.pagesWaiting && pagesEmitted == status.pagesEmitted && rowsEmitted == status.rowsEmitted;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pagesWaiting, pagesEmitted, rowsEmitted);
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
