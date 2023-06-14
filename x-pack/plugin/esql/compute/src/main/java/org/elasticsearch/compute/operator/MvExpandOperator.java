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
public class MvExpandOperator extends AbstractPageMappingOperator {
    public record Factory(int channel) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new MvExpandOperator(channel);
        }

        @Override
        public String describe() {
            return "MvExpandOperator[channel=" + channel + "]";
        }
    }

    private final int channel;

    private int noops;

    public MvExpandOperator(int channel) {
        this.channel = channel;
    }

    @Override
    protected Page process(Page page) {
        Block expandingBlock = page.getBlock(channel);
        Block expandedBlock = expandingBlock.expand();
        if (expandedBlock == expandingBlock) {
            noops++;
            return page;
        }
        if (page.getBlockCount() == 1) {
            assert channel == 0;
            return new Page(expandedBlock);
        }

        int[] duplicateFilter = buildDuplicateExpandingFilter(expandingBlock, expandedBlock.getPositionCount());

        Block[] result = new Block[page.getBlockCount()];
        for (int b = 0; b < result.length; b++) {
            result[b] = b == channel ? expandedBlock : page.getBlock(b).filter(duplicateFilter);
        }
        return new Page(result);
    }

    private int[] buildDuplicateExpandingFilter(Block expandingBlock, int newPositions) {
        int[] duplicateFilter = new int[newPositions];
        int n = 0;
        for (int p = 0; p < expandingBlock.getPositionCount(); p++) {
            int count = expandingBlock.getValueCount(p);
            int positions = count == 0 ? 1 : count;
            Arrays.fill(duplicateFilter, n, n + positions, p);
            n += positions;
        }
        return duplicateFilter;
    }

    @Override
    protected AbstractPageMappingOperator.Status status(int pagesProcessed) {
        return new Status(pagesProcessed, noops);
    }

    @Override
    public String toString() {
        return "MvExpandOperator[channel=" + channel + "]";
    }

    public static final class Status extends AbstractPageMappingOperator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "mv_expand",
            Status::new
        );

        private final int noops;

        Status(int pagesProcessed, int noops) {
            super(pagesProcessed);
            this.noops = noops;
        }

        Status(StreamInput in) throws IOException {
            super(in);
            noops = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(noops);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("pages_processed", pagesProcessed());
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
            return noops == status.noops && pagesProcessed() == status.pagesProcessed();
        }

        @Override
        public int hashCode() {
            return Objects.hash(noops, pagesProcessed());
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
