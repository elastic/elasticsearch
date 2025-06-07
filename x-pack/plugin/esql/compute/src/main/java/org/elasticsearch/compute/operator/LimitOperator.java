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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class LimitOperator implements Operator {

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

    private Page lastInput;

    private final Limiter limiter;
    private boolean finished;

    public LimitOperator(Limiter limiter) {
        this.limiter = limiter;
    }

    public static final class Factory implements OperatorFactory {
        private final Limiter limiter;

        public Factory(int limit) {
            this.limiter = new Limiter(limit);
        }

        @Override
        public LimitOperator get(DriverContext driverContext) {
            return new LimitOperator(limiter);
        }

        @Override
        public String describe() {
            return "LimitOperator[limit = " + limiter.limit() + "]";
        }
    }

    @Override
    public boolean needsInput() {
        return finished == false && lastInput == null && limiter.remaining() > 0;
    }

    @Override
    public void addInput(Page page) {
        assert lastInput == null : "has pending input page";
        final int acceptedRows = limiter.tryAccumulateHits(page.getPositionCount());
        if (acceptedRows == 0) {
            page.releaseBlocks();
            assert isFinished();
        } else if (acceptedRows < page.getPositionCount()) {
            lastInput = truncatePage(page, acceptedRows);
        } else {
            lastInput = page;
        }
        rowsReceived += acceptedRows;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return lastInput == null && (finished || limiter.remaining() == 0);
    }

    @Override
    public Page getOutput() {
        if (lastInput == null) {
            return null;
        }
        final Page result = lastInput;
        lastInput = null;
        pagesProcessed++;
        rowsEmitted += result.getPositionCount();
        return result;
    }

    private static Page truncatePage(Page page, int upTo) {
        int[] filter = new int[upTo];
        for (int i = 0; i < upTo; i++) {
            filter[i] = i;
        }
        final Block[] blocks = new Block[page.getBlockCount()];
        Page result = null;
        try {
            for (int b = 0; b < blocks.length; b++) {
                blocks[b] = page.getBlock(b).filter(filter);
            }
            result = new Page(blocks);
        } finally {
            if (result == null) {
                Releasables.closeExpectNoException(page::releaseBlocks, Releasables.wrap(blocks));
            } else {
                page.releaseBlocks();
            }
        }
        return result;
    }

    @Override
    public Status status() {
        return new Status(limiter.limit(), limiter.remaining(), pagesProcessed, rowsReceived, rowsEmitted);
    }

    @Override
    public void close() {
        if (lastInput != null) {
            lastInput.releaseBlocks();
        }
    }

    @Override
    public String toString() {
        final int limitRemaining = limiter.remaining();
        final int limit = limiter.limit();
        return "LimitOperator[limit = " + limitRemaining + "/" + limit + "]";
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "limit",
            Status::new
        );

        /**
         * Total number of position that are emitted by this operator.
         */
        private final int limit;

        /**
         * Remaining number of positions that will be emitted by this operator.
         */
        private final int limitRemaining;

        /**
         * Count of pages that have been processed by this operator.
         */
        private final int pagesProcessed;

        /**
         * Count of rows this operator has received.
         */
        private final long rowsReceived;

        /**
         * Count of rows this operator has emitted.
         */
        private final long rowsEmitted;

        protected Status(int limit, int limitRemaining, int pagesProcessed, long rowsReceived, long rowsEmitted) {
            this.limit = limit;
            this.limitRemaining = limitRemaining;
            this.pagesProcessed = pagesProcessed;
            this.rowsReceived = rowsReceived;
            this.rowsEmitted = rowsEmitted;
        }

        protected Status(StreamInput in) throws IOException {
            limit = in.readVInt();
            limitRemaining = in.readVInt();
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
            out.writeVInt(limit);
            out.writeVInt(limitRemaining);
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

        /**
         * Total number of position that are emitted by this operator.
         */
        public int limit() {
            return limit;
        }

        /**
         * Count of pages that have been processed by this operator.
         */
        public int limitRemaining() {
            return limitRemaining;
        }

        /**
         * Count of pages that have been processed by this operator.
         */
        public int pagesProcessed() {
            return pagesProcessed;
        }

        /**
         * Count of rows this operator has received.
         */
        public long rowsReceived() {
            return rowsReceived;
        }

        /**
         * Count of rows this operator has emitted.
         */
        public long rowsEmitted() {
            return rowsEmitted;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("limit", limit);
            builder.field("limit_remaining", limitRemaining);
            builder.field("pages_processed", pagesProcessed);
            builder.field("rows_received", rowsReceived);
            builder.field("rows_emitted", rowsEmitted);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return limit == status.limit
                && limitRemaining == status.limitRemaining
                && pagesProcessed == status.pagesProcessed
                && rowsReceived == status.rowsReceived
                && rowsEmitted == status.rowsEmitted;
        }

        @Override
        public int hashCode() {
            return Objects.hash(limit, limitRemaining, pagesProcessed, rowsReceived, rowsEmitted);
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
