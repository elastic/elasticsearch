/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Abstract superclass for operators that accept a single page, modify it, and then return it.
 */
public abstract class AbstractPageMappingOperator implements Operator {
    private Page prev;
    private boolean finished = false;

    /**
     * Number of milliseconds this operation has run.
     */
    private long processNanos;

    /**
     * Count of pages that have been processed by this operator.
     */
    private int pagesProcessed;
    /**
     * Count of rows this operator has received.
     */
    private long rowsReceived;
    /**
     * Count of rows this operator has emitted.
     */
    private long rowsEmitted;

    protected abstract Page process(Page page);

    @Override
    public abstract String toString();

    @Override
    public final boolean needsInput() {
        return prev == null && finished == false;
    }

    @Override
    public final void addInput(Page page) {
        assert prev == null : "has pending input page";
        prev = page;
        rowsReceived += page.getPositionCount();
    }

    @Override
    public final void finish() {
        finished = true;
    }

    @Override
    public final boolean isFinished() {
        return finished && prev == null;
    }

    @Override
    public final Page getOutput() {
        if (prev == null) {
            return null;
        }
        if (prev.getPositionCount() == 0) {
            return prev;
        }
        long start = System.nanoTime();
        Page p = process(prev);
        pagesProcessed++;
        if (p != null) {
            rowsEmitted += p.getPositionCount();
        }
        processNanos += System.nanoTime() - start;
        prev = null;
        return p;
    }

    @Override
    public final Status status() {
        return status(processNanos, pagesProcessed, rowsReceived, rowsEmitted);
    }

    protected Status status(long processNanos, int pagesProcessed, long rowsReceived, long rowsEmitted) {
        return new Status(processNanos, pagesProcessed, rowsReceived, rowsEmitted);
    }

    @Override
    public void close() {
        if (prev != null) {
            Releasables.closeExpectNoException(() -> prev.releaseBlocks());
        }
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "page_mapping",
            Status::new
        );

        private final long processNanos;
        private final int pagesProcessed;
        private final long rowsReceived;
        private final long rowsEmitted;

        public Status(long processNanos, int pagesProcessed, long rowsReceived, long rowsEmitted) {
            this.processNanos = processNanos;
            this.pagesProcessed = pagesProcessed;
            this.rowsReceived = rowsReceived;
            this.rowsEmitted = rowsEmitted;
        }

        public Status(StreamInput in) throws IOException {
            processNanos = in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0) ? in.readVLong() : 0;
            pagesProcessed = in.readVInt();
            if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_PROFILE_ROWS_PROCESSED)) {
                rowsReceived = in.readVLong();
                rowsEmitted = in.readVLong();
            } else {
                rowsReceived = 0;
                rowsEmitted = 0;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
                out.writeVLong(processNanos);
            }
            out.writeVInt(pagesProcessed);
            if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_PROFILE_ROWS_PROCESSED)) {
                out.writeVLong(rowsReceived);
                out.writeVLong(rowsEmitted);
            }
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public int pagesProcessed() {
            return pagesProcessed;
        }

        public long rowsReceived() {
            return rowsReceived;
        }

        public long rowsEmitted() {
            return rowsEmitted;
        }

        public long processNanos() {
            return processNanos;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            innerToXContent(builder);
            return builder.endObject();
        }

        /**
         * Render the body of the object for this status. Protected so subclasses
         * can call it to render the "default" body.
         */
        protected final XContentBuilder innerToXContent(XContentBuilder builder) throws IOException {
            builder.field("process_nanos", processNanos);
            if (builder.humanReadable()) {
                builder.field("process_time", TimeValue.timeValueNanos(processNanos));
            }
            builder.field("pages_processed", pagesProcessed);
            builder.field("rows_received", rowsReceived);
            builder.field("rows_emitted", rowsEmitted);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return processNanos == status.processNanos
                && pagesProcessed == status.pagesProcessed
                && rowsReceived == status.rowsReceived
                && rowsEmitted == status.rowsEmitted;
        }

        @Override
        public int hashCode() {
            return Objects.hash(processNanos, pagesProcessed, rowsReceived, rowsEmitted);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.V_8_11_X;
        }
    }
}
