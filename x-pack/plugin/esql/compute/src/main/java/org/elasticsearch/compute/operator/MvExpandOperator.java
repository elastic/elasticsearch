/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
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
    private boolean prevCompleted = false;
    private boolean finished = false;

    private int nextPositionToProcess = 0;
    private int nextMvToProcess = 0;
    private int nextItemOnExpanded = 0;

    /**
     * Count of pages that have been processed by this operator.
     */
    private int pagesProcessed;

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
        if (prev.getPositionCount() == 0) {
            Page result = prev;
            prev = null;
            pagesProcessed++;
            return result;
        }

        Block expandingBlock = prev.getBlock(channel);
        if (expandingBlock.mayHaveMultivaluedFields() == false) {
            noops++;
            Page result = prev;
            prev = null;
            pagesProcessed++;
            return result;
        }

        try {
            return process();
        } finally {
            if (prevCompleted && prev != null) {
                pagesProcessed++;
                prev.releaseBlocks();
                prev = null;
            }
        }
    }

    protected Page process() {
        Block expandingBlock = prev.getBlock(channel);
        Block expandedBlock = expandingBlock.expand();
        if (expandedBlock == expandingBlock) {
            noops++;
            prevCompleted = true;
            return prev;
        }
        if (prev.getBlockCount() == 1) {
            assert channel == 0;
            prevCompleted = true;
            return new Page(expandedBlock);
        }

        int[] duplicateFilter = nextDuplicateExpandingFilter(expandingBlock, pageSize);

        Block[] result = new Block[prev.getBlockCount()];
        int[] expandedMask = new int[duplicateFilter.length];
        for (int i = 0; i < expandedMask.length; i++) {
            expandedMask[i] = i + nextItemOnExpanded;
        }
        nextItemOnExpanded += expandedMask.length;
        for (int b = 0; b < result.length; b++) {
            result[b] = b == channel ? expandedBlock.filter(expandedMask) : prev.getBlock(b).filter(duplicateFilter);
        }
        if (nextItemOnExpanded == expandedBlock.getPositionCount()) {
            nextItemOnExpanded = 0;
        }
        return new Page(result);
    }

    private int[] nextDuplicateExpandingFilter(Block expandingBlock, int size) {
        int[] duplicateFilter = new int[size];
        int n = 0;
        while (nextPositionToProcess < expandingBlock.getPositionCount()) {
            int count = expandingBlock.getValueCount(nextPositionToProcess);
            int positions = count == 0 ? 1 : count;
            int toAdd = Math.min(size - n, positions - nextMvToProcess);
            Arrays.fill(duplicateFilter, n, n + toAdd, nextPositionToProcess);
            n += toAdd;

            if (n == size) {
                if (nextMvToProcess + toAdd == positions) {
                    nextMvToProcess = 0;
                    nextPositionToProcess++;
                    if (nextPositionToProcess == expandingBlock.getPositionCount()) {
                        nextPositionToProcess = 0;
                        prevCompleted = true;
                    }
                } else {
                    nextMvToProcess = nextMvToProcess + toAdd;
                }
                return duplicateFilter;
            }

            nextMvToProcess = 0;
            nextPositionToProcess++;
        }

        if (nextPositionToProcess == expandingBlock.getPositionCount()) {
            nextPositionToProcess = 0;
            nextMvToProcess = 0;
            prevCompleted = true;
        }
        return n < size ? Arrays.copyOfRange(duplicateFilter, 0, n) : duplicateFilter;
    }

    @Override
    public final boolean needsInput() {
        return prev == null && finished == false;
    }

    @Override
    public final void addInput(Page page) {
        assert prev == null : "has pending input page";
        prev = page;
        prevCompleted = false;
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
        return new Status(pagesProcessed, noops);
    }

    @Override
    public void close() {
        if (prev != null) {
            Releasables.closeExpectNoException(() -> prev.releaseBlocks());
        }
    }

    @Override
    public String toString() {
        return "MvExpandOperator[channel=" + channel + "]";
    }

    public static final class Status implements Operator.Status {

        private final int pagesProcessed;

        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "mv_expand",
            Status::new
        );

        private final int noops;

        Status(int pagesProcessed, int noops) {
            this.pagesProcessed = pagesProcessed;
            this.noops = noops;
        }

        Status(StreamInput in) throws IOException {
            pagesProcessed = in.readVInt();
            noops = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(pagesProcessed);
            out.writeVInt(noops);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("pages_processed", pagesProcessed);
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
            return noops == status.noops && pagesProcessed == status.pagesProcessed;
        }

        public int pagesProcessed() {
            return pagesProcessed;
        }

        @Override
        public int hashCode() {
            return Objects.hash(noops, pagesProcessed);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
