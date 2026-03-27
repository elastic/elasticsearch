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

    public AsyncExternalSourceOperator(AsyncExternalSourceBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public Page getOutput() {
        final var page = buffer.pollPage();
        if (page != null) {
            pagesEmitted++;
            rowsEmitted += page.getPositionCount();
        }
        return page;
    }

    @Override
    public boolean isFinished() {
        return buffer.isFinished();
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
        return new Status(buffer.size(), pagesEmitted, rowsEmitted, buffer.failure());
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "async_external_source",
            Status::new
        );

        private final int pagesWaiting;
        private final int pagesEmitted;
        private final long rowsEmitted;
        private final Throwable failure;

        Status(int pagesWaiting, int pagesEmitted, long rowsEmitted, Throwable failure) {
            this.pagesWaiting = pagesWaiting;
            this.pagesEmitted = pagesEmitted;
            this.rowsEmitted = rowsEmitted;
            this.failure = failure;
        }

        Status(StreamInput in) throws IOException {
            pagesWaiting = in.readVInt();
            pagesEmitted = in.readVInt();
            rowsEmitted = in.readVLong();
            failure = in.readException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(pagesWaiting);
            out.writeVInt(pagesEmitted);
            out.writeVLong(rowsEmitted);
            out.writeException(failure);
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

        public Throwable failure() {
            return failure;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("pages_waiting", pagesWaiting);
            builder.field("pages_emitted", pagesEmitted);
            builder.field("rows_emitted", rowsEmitted);
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
                && Objects.equals(thisFailureMsg, otherFailureMsg);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pagesWaiting, pagesEmitted, rowsEmitted, failure != null ? failure.getMessage() : null);
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
