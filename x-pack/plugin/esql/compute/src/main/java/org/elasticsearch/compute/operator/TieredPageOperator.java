/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersion;
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
public class TieredPageOperator extends SinkOperator {

    private final PageStreamPublisher stream;
    private final Function<Page, Page> alignment;
    private boolean finishCalled;
    private int pagesEmitted;
    private long rowsEmitted;

    public TieredPageOperator(PageStreamPublisher stream, Function<Page, Page> alignment) {
        this.stream = stream;
        this.alignment = alignment;
    }

    @Override
    protected void doAddInput(Page page) {
        Page aligned = alignment.apply(page);
        pagesEmitted++;
        rowsEmitted += aligned.getPositionCount();
        stream.addPage(aligned);
    }

    @Override
    public boolean needsInput() {
        return finishCalled == false && isBlocked().listener().isDone();
    }

    @Override
    public IsBlockedResult isBlocked() {
        return stream.waitForWriting();
    }

    @Override
    public void finish() {
        finishCalled = true;
        stream.pagesFinished();
    }

    @Override
    public boolean isFinished() {
        return finishCalled;
    }

    @Override
    public void close() {}

    @Override
    public Status status() {
        return new Status(pagesEmitted, rowsEmitted);
    }

    /**
     * Factory for {@link TieredPageOperator}.
     */
    public record Factory(PageStreamPublisher stream, Function<Page, Page> alignment) implements SinkOperatorFactory {

        @Override
        public SinkOperator get(DriverContext driverContext) {
            return new TieredPageOperator(stream, alignment);
        }

        @Override
        public String describe() {
            return "TieredPageOperator";
        }
    }

    /**
     * Operator status reported during driver profiling.
     */
    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "tiered_page",
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

        // TODO: Update this to ESQL_STREAM transport version
        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.minimumCompatible();
        }
    }
}
