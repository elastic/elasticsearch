/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

/**
 * A {@link SinkOperator} that pushes each incoming {@link Page} to a {@link PageStreamPublisher},
 * applying an optional column-alignment transform before forwarding.
 * Backpressure is provided by {@link PageStreamPublisher#waitForWriting()}: the driver stalls
 * until the REST subscriber signals demand via {@code request(1)}.
 */
public class StreamingPageOperator extends SinkOperator {

    public static final TransportVersion ESQL_STREAMING = TransportVersion.fromName("esql_streaming");

    private static final String NAME = "StreamingPageOperator";

    private final PageStreamPublisher stream;
    private final PageStreamPublisher.Producer producer;
    private final Function<Page, Page> alignment;
    private boolean finishCalled;
    private int pagesEmitted;
    private long rowsEmitted;

    public StreamingPageOperator(PageStreamPublisher stream, PageStreamPublisher.Producer producer, Function<Page, Page> alignment) {
        this.stream = stream;
        this.producer = producer;
        this.alignment = alignment;
    }

    @Override
    protected void doAddInput(Page page) {
        Page aligned = alignment.apply(page);
        pagesEmitted++;
        rowsEmitted += aligned.getPositionCount();
        if (producer.addPage(aligned) == false) {
            // The publisher has been cancelled (e.g. the HTTP client disconnected) or terminated.
            // Treat this like a closed exchange sink: throw DriverEarlyTerminationException so the
            // driver tears the operator chain down cleanly, exactly as it does for "Exchange sink
            // is closed" (Driver.java). The page was already released inside addPage.
            throw new DriverEarlyTerminationException("Streaming subscriber cancelled");
        }
    }

    public void addCompletionListener(ActionListener<Void> listener) {
        stream.addCloseListener(listener);
    }

    @Override
    public boolean needsInput() {
        return isFinished() == false && isBlocked().listener().isDone();
    }

    @Override
    public IsBlockedResult isBlocked() {
        return stream.waitForWriting();
    }

    @Override
    public void finish() {
        finishCalled = true;
        producer.finish();
    }

    @Override
    public boolean isFinished() {
        return finishCalled || stream.isClosed();
    }

    @Override
    public void close() {}

    @Override
    public String toString() {
        return NAME;
    }

    @Override
    public Status status() {
        return new Status(pagesEmitted, rowsEmitted);
    }

    public record Factory(PageStreamPublisher stream, Function<Page, Page> alignment) implements SinkOperatorFactory {

        @Override
        public SinkOperator get(DriverContext driverContext) {
            return new StreamingPageOperator(stream, stream.registerProducer(), alignment);
        }

        @Override
        public String describe() {
            return NAME;
        }
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "streaming_page",
            Status::new
        );

        private final int pagesEmitted;
        private final long rowsEmitted;

        Status(int pagesEmitted, long rowsEmitted) {
            this.pagesEmitted = pagesEmitted;
            this.rowsEmitted = rowsEmitted;
        }

        Status(StreamInput in) throws IOException {
            pagesEmitted = in.readVInt();
            rowsEmitted = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(pagesEmitted);
            out.writeVLong(rowsEmitted);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
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
            builder.field("pages_emitted", pagesEmitted);
            builder.field("rows_emitted", rowsEmitted);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return pagesEmitted == status.pagesEmitted && rowsEmitted == status.rowsEmitted;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pagesEmitted, rowsEmitted);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return ESQL_STREAMING;
        }
    }
}
