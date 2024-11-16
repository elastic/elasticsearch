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
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * Maps a single {@link Page} into zero or more resulting pages.
 */
public abstract class AbstractPageMappingToIteratorOperator implements Operator {
    private ReleasableIterator<Page> next;

    private boolean finished = false;

    /**
     * Number of milliseconds this operation has spent receiving pages.
     */
    private long processNanos;

    /**
     * Count of pages that have been received by this operator.
     */
    private int pagesReceived;

    /**
     * Count of pages that have been emitted by this operator.
     */
    private int pagesEmitted;

    /**
     * Build and Iterator of results for a new page.
     */
    protected abstract ReleasableIterator<Page> receive(Page page);

    /**
     * Append an {@link Iterator} of {@link Block}s to a {@link Page}, one
     * after the other. It's required that the iterator emit as many
     * <strong>positions</strong> as there were in the page.
     */
    public static ReleasableIterator<Page> appendBlocks(Page page, ReleasableIterator<? extends Block> toAdd) {
        return new AppendBlocksIterator(page, toAdd);
    }

    @Override
    public abstract String toString();

    @Override
    public final boolean needsInput() {
        return finished == false && (next == null || next.hasNext() == false);
    }

    @Override
    public final void addInput(Page page) {
        if (next != null) {
            assert next.hasNext() == false : "has pending input page";
            next.close();
        }
        if (page.getPositionCount() == 0) {
            return;
        }
        next = new RuntimeTrackingIterator(receive(page));
        pagesReceived++;
    }

    @Override
    public final void finish() {
        finished = true;
    }

    @Override
    public final boolean isFinished() {
        return finished && (next == null || next.hasNext() == false);
    }

    @Override
    public final Page getOutput() {
        if (next == null || next.hasNext() == false) {
            return null;
        }
        Page ret = next.next();
        pagesEmitted++;
        return ret;
    }

    @Override
    public final AbstractPageMappingToIteratorOperator.Status status() {
        return status(processNanos, pagesReceived, pagesEmitted);
    }

    protected AbstractPageMappingToIteratorOperator.Status status(long processNanos, int pagesReceived, int pagesEmitted) {
        return new AbstractPageMappingToIteratorOperator.Status(processNanos, pagesReceived, pagesEmitted);
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(next);
    }

    private class RuntimeTrackingIterator implements ReleasableIterator<Page> {
        private final ReleasableIterator<Page> next;

        private RuntimeTrackingIterator(ReleasableIterator<Page> next) {
            this.next = next;
        }

        @Override
        public boolean hasNext() {
            return next.hasNext();
        }

        @Override
        public Page next() {
            long start = System.nanoTime();
            Page out = next.next();
            processNanos += System.nanoTime() - start;
            return out;
        }

        @Override
        public void close() {
            next.close();
        }
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "page_mapping_to_iterator",
            AbstractPageMappingOperator.Status::new
        );

        private final long processNanos;
        private final int pagesReceived;
        private final int pagesEmitted;

        public Status(long processNanos, int pagesProcessed, int pagesEmitted) {
            this.processNanos = processNanos;
            this.pagesReceived = pagesProcessed;
            this.pagesEmitted = pagesEmitted;
        }

        protected Status(StreamInput in) throws IOException {
            processNanos = in.readVLong();
            pagesReceived = in.readVInt();
            pagesEmitted = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(processNanos);
            out.writeVInt(pagesReceived);
            out.writeVInt(pagesEmitted);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public int pagesReceived() {
            return pagesReceived;
        }

        public int pagesEmitted() {
            return pagesEmitted;
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
            builder.field("pages_received", pagesReceived);
            return builder.field("pages_emitted", pagesEmitted);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AbstractPageMappingToIteratorOperator.Status status = (AbstractPageMappingToIteratorOperator.Status) o;
            return processNanos == status.processNanos && pagesReceived == status.pagesReceived && pagesEmitted == status.pagesEmitted;
        }

        @Override
        public int hashCode() {
            return Objects.hash(processNanos, pagesReceived, pagesEmitted);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.V_8_15_0;
        }
    }

    private static class AppendBlocksIterator implements ReleasableIterator<Page> {
        private final Page page;
        private final ReleasableIterator<? extends Block> next;

        private int positionOffset;

        protected AppendBlocksIterator(Page page, ReleasableIterator<? extends Block> next) {
            this.page = page;
            this.next = next;
        }

        @Override
        public final boolean hasNext() {
            if (next.hasNext()) {
                assert positionOffset < page.getPositionCount();
                return true;
            }
            assert positionOffset == page.getPositionCount();
            return false;
        }

        @Override
        public final Page next() {
            Block read = next.next();
            int start = positionOffset;
            positionOffset += read.getPositionCount();
            if (start == 0 && read.getPositionCount() == page.getPositionCount()) {
                for (int b = 0; b < page.getBlockCount(); b++) {
                    page.getBlock(b).incRef();
                }
                return page.appendBlock(read);
            }
            Block[] newBlocks = new Block[page.getBlockCount() + 1];
            newBlocks[page.getBlockCount()] = read;
            try {
                // TODO a way to filter with a range please.
                int[] positions = IntStream.range(start, positionOffset).toArray();
                for (int b = 0; b < page.getBlockCount(); b++) {
                    newBlocks[b] = page.getBlock(b).filter(positions);
                }
                Page result = new Page(newBlocks);
                Arrays.fill(newBlocks, null);
                return result;
            } finally {
                Releasables.closeExpectNoException(newBlocks);
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(page::releaseBlocks, next);
        }
    }
}
