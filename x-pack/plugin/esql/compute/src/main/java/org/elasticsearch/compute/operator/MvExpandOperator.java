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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * "Expands" multivalued blocks by duplicating all the other columns for each value.
 * <pre>
 *     [0, 1, 2] | 2 | "foo"
 * </pre>
 * becomes
 * <pre>
 *     0 | 2 | "foo"
 *     1 | 2 | "foo"
 *     2 | 2 | "foo"
 * </pre>
 */
public class MvExpandOperator implements Operator {
    private static final Logger logger = LogManager.getLogger(MvExpandOperator.class);

    public record Factory(int channel, int blockSize) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new MvExpandOperator(channel, blockSize);
        }

        @Override
        public String describe() {
            return "MvExpandOperator[channel=" + channel + "]";
        }
    }

    private final int channel;

    private final int pageSize;

    private int noops;

    private Page prev;
    private boolean prevCompleted;
    private boolean finished = false;

    private Block expandingBlock;
    private Block expandedBlock;

    private int nextPositionToProcess = 0;
    private int nextMvToProcess = 0;
    private int nextItemOnExpanded = 0;

    /**
     * Count of pages that have been processed by this operator.
     */
    private int pagesIn;
    private int pagesOut;

    public MvExpandOperator(int channel, int pageSize) {
        this.channel = channel;
        this.pageSize = pageSize;
        assert pageSize > 0;
    }

    @Override
    public final Page getOutput() {
        if (prev == null) {
            return null;
        }
        pagesOut++;

        if (expandedBlock == null) {
            /*
             * If we're emitting the first block from this page
             * then we have to expand it.
             */
            logger.trace("starting {}", prev);
            expandingBlock = prev.getBlock(channel);
            expandedBlock = expandingBlock.expand();

            if (expandedBlock == expandingBlock) {
                // The expand was a noop - just return the previous page and clear state.
                logger.trace("expanded to same");
                noops++;
                Page result = prev;
                prev = null;

                releaseAndClearState();
                return result;
            }
            if (prev.getBlockCount() == 1) {
                /*
                 * The expand wasn't a noop, but there's only a single block in the result
                 * so the expansion didn't really make it take more memory. It should be safe
                 * to return it directly.
                 */
                logger.trace("single block output");
                assert channel == 0;
                Page result = new Page(expandedBlock);
                expandedBlock = null;

                releaseAndClearState();
                return result;
            }
        }
        logger.trace("slicing");
        return sliceExpandedIntoPages();
    }

    private Page sliceExpandedIntoPages() {
        prevCompleted = false;
        int[] duplicateFilter = nextDuplicateExpandingFilter();

        Block[] result = new Block[prev.getBlockCount()];
        boolean success = false;
        try {
            int[] expandedMask = new int[duplicateFilter.length];
            for (int i = 0; i < expandedMask.length; i++) {
                expandedMask[i] = i + nextItemOnExpanded;
            }
            nextItemOnExpanded += expandedMask.length;
            for (int b = 0; b < result.length; b++) {
                result[b] = b == channel ? expandedBlock.filter(expandedMask) : prev.getBlock(b).filter(duplicateFilter);
            }
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(result);
            }
        }
        if (nextItemOnExpanded == expandedBlock.getPositionCount()) {
            nextItemOnExpanded = 0;
        }
        if (prevCompleted) {
            releaseAndClearState();
        }
        return new Page(result);
    }

    private int[] nextDuplicateExpandingFilter() {
        int[] duplicateFilter = new int[Math.min(pageSize, expandedBlock.getPositionCount() - nextPositionToProcess)];
        int n = 0;
        while (true) {
            int count = expandingBlock.getValueCount(nextPositionToProcess);
            int positions = count == 0 ? 1 : count;
            int toAdd = Math.min(pageSize - n, positions - nextMvToProcess);
            Arrays.fill(duplicateFilter, n, n + toAdd, nextPositionToProcess);
            n += toAdd;

            if (n == pageSize) {
                if (nextMvToProcess + toAdd == positions) {
                    // finished expanding this position, let's move on to next position (that will be expanded with next call)
                    nextMvToProcess = 0;
                    nextPositionToProcess++;
                    if (nextPositionToProcess == expandingBlock.getPositionCount()) {
                        nextPositionToProcess = 0;
                        prevCompleted = true;
                    }
                } else {
                    // there are still items to expand in current position, but the duplicate filter is full, so we'll deal with them at
                    // next call
                    nextMvToProcess = nextMvToProcess + toAdd;
                }
                return duplicateFilter;
            }

            nextMvToProcess = 0;
            nextPositionToProcess++;
            if (nextPositionToProcess == expandingBlock.getPositionCount()) {
                nextPositionToProcess = 0;
                nextMvToProcess = 0;
                prevCompleted = true;
                return n < pageSize ? Arrays.copyOfRange(duplicateFilter, 0, n) : duplicateFilter;
            }
        }
    }

    private void releaseAndClearState() {
        Releasables.closeExpectNoException(() -> {
            if (prev != null) {
                prev.releaseBlocks();
                prev = null;
            }
        }, expandedBlock);
        expandingBlock = null;
        expandedBlock = null;
    }

    @Override
    public final boolean needsInput() {
        return prev == null && finished == false;
    }

    @Override
    public final void addInput(Page page) {
        assert prev == null : "has pending input page";
        prev = page;
        this.expandingBlock = prev.getBlock(channel);
        pagesIn++;
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
    public final Status status() {
        return new Status(pagesIn, pagesOut, noops);
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(() -> {
            if (prev != null) {
                prev.releaseBlocks();
            }
        }, expandedBlock);
    }

    @Override
    public String toString() {
        return "MvExpandOperator[channel=" + channel + "]";
    }

    public static final class Status implements Operator.Status {

        private final int pagesIn;
        private final int pagesOut;
        private final int noops;

        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "mv_expand",
            Status::new
        );

        Status(int pagesIn, int pagesOut, int noops) {
            this.pagesIn = pagesIn;
            this.pagesOut = pagesOut;
            this.noops = noops;
        }

        Status(StreamInput in) throws IOException {
            pagesIn = in.readVInt();
            pagesOut = in.readVInt();
            noops = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(pagesIn);
            out.writeVInt(pagesOut);
            out.writeVInt(noops);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("pages_in", pagesIn);
            builder.field("pages_out", pagesOut);
            builder.field("noops", noops);
            return builder.endObject();
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public int noops() {
            return noops;
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
            return noops == status.noops && pagesIn == status.pagesIn && pagesOut == status.pagesOut;
        }

        public int pagesIn() {
            return pagesIn;
        }

        public int pagesOut() {
            return pagesOut;
        }

        @Override
        public int hashCode() {
            return Objects.hash(noops, pagesIn, pagesOut);
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
