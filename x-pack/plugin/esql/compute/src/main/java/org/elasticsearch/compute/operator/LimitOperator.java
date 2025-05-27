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
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

    private final Limiter limiter;
    private boolean finished;

    private final int pageSize;
    private int pendingRows;
    private final List<Page> queue = new ArrayList<>();
    private final BlockFactory blockFactory;

    public LimitOperator(Limiter limiter, BlockFactory blockFactory, int pageSize) {
        this.limiter = limiter;
        this.blockFactory = blockFactory;
        this.pageSize = pageSize;
    }

    public static final class Factory implements OperatorFactory {
        private final Limiter limiter;
        private final int pageSize;

        public Factory(int limit, int pageSize) {
            this.limiter = new Limiter(limit);
            this.pageSize = pageSize;
        }

        @Override
        public LimitOperator get(DriverContext driverContext) {
            return new LimitOperator(limiter, driverContext.blockFactory(), pageSize);
        }

        @Override
        public String describe() {
            return "LimitOperator[limit = " + limiter.limit() + ", pageSize = " + pageSize + "]";
        }
    }

    @Override
    public boolean needsInput() {
        return readyToEmit() == false;
    }

    @Override
    public void addInput(Page page) {
        pagesProcessed++;
        rowsReceived += page.getPositionCount();
        final int acceptedRows = limiter.tryAccumulateHits(page.getPositionCount());
        if (acceptedRows == 0) {
            page.releaseBlocks();
        } else {
            queue.add(page);
            pendingRows += acceptedRows;
        }
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return pendingRows == 0 && (finished || limiter.remaining() == 0);
    }

    private boolean readyToEmit() {
        return finished || pendingRows >= pageSize || limiter.remaining() == 0;
    }

    @Override
    public Page getOutput() {
        if (pendingRows > 0 && readyToEmit()) {
            final Page result = combinePages(queue, blockFactory, pendingRows);
            pendingRows = 0;
            rowsEmitted += result.getPositionCount();
            return result;
        } else {
            return null;
        }
    }

    private static ElementType[] elementTypes(int blockCount, List<Page> pages) {
        ElementType[] elementTypes = new ElementType[blockCount];
        for (Page page : pages) {
            for (int b = 0; b < blockCount; b++) {
                ElementType newType = page.getBlock(b).elementType();
                ElementType currType = elementTypes[b];
                if (currType == null || currType == ElementType.NULL) {
                    elementTypes[b] = newType;
                } else {
                    assert newType == ElementType.NULL || currType == newType : "element type mismatch: " + currType + " != " + newType;
                }
            }
        }
        return elementTypes;
    }

    private static Page combinePages(List<Page> pages, BlockFactory blockFactory, int upTo) {
        assert pages.isEmpty() == false : "no pages to combine";
        if (pages.size() == 1 && pages.getFirst().getPositionCount() == upTo) {
            return pages.removeFirst();
        }
        int blockCount = pages.getFirst().getBlockCount();
        Block.Builder[] builders = new Block.Builder[blockCount];
        try {
            ElementType[] elementTypes = elementTypes(blockCount, pages);
            for (int b = 0; b < blockCount; b++) {
                builders[b] = elementTypes[b].newBlockBuilder(upTo, blockFactory);
            }
            int accumulated = 0;
            for (Page page : pages) {
                int size = Math.min(page.getPositionCount(), upTo - accumulated);
                for (int b = 0; b < blockCount; b++) {
                    Block block = page.getBlock(b);
                    builders[b].copyFrom(block, 0, size);
                }
                accumulated += size;
            }
            Block[] blocks = Block.Builder.buildAll(builders);
            return new Page(blocks);
        } finally {
            Releasables.close(Releasables.wrap(pages), pages::clear, Releasables.wrap(builders));
        }
    }

    @Override
    public Status status() {
        return new Status(limiter.limit(), limiter.remaining(), pagesProcessed, rowsReceived, rowsEmitted);
    }

    @Override
    public void close() {
        Releasables.close(queue);
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
