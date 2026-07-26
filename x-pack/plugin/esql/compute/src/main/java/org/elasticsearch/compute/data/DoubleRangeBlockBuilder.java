/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.GenericNamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;

public final class DoubleRangeBlockBuilder extends AbstractBlockBuilder implements DoubleRangeBlock.Builder {

    private DoubleBlockBuilder fromBuilder;
    private DoubleBlockBuilder toBuilder;

    public DoubleRangeBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        fromBuilder = null;
        toBuilder = null;
        try {
            fromBuilder = new DoubleBlockBuilder(estimatedSize, blockFactory);
            toBuilder = new DoubleBlockBuilder(estimatedSize, blockFactory);
        } finally {
            if (toBuilder == null) {
                Releasables.closeWhileHandlingException(fromBuilder);
            }
        }
    }

    @Override
    protected int valuesLength() {
        throw new UnsupportedOperationException("Not available on double_range");
    }

    @Override
    protected void growValuesArray(int newSize) {
        throw new UnsupportedOperationException("Not available on double_range");
    }

    @Override
    protected int elementSize() {
        throw new UnsupportedOperationException("Not available on double_range");
    }

    @Override
    public long estimatedBytes() {
        return fromBuilder.estimatedBytes() + toBuilder.estimatedBytes();
    }

    @Override
    public DoubleRangeBlockBuilder copyFrom(Block b, int beginInclusive, int endExclusive) {
        Block fromBlock;
        Block toBlock;
        if (b.areAllValuesNull()) {
            fromBlock = b;
            toBlock = b;
        } else {
            var block = (DoubleRangeArrayBlock) b;
            fromBlock = block.getDoubleFromBlock();
            toBlock = block.getDoubleToBlock();
        }
        fromBuilder.copyFrom(fromBlock, beginInclusive, endExclusive);
        toBuilder.copyFrom(toBlock, beginInclusive, endExclusive);
        return this;
    }

    @Override
    public DoubleRangeBlockBuilder copyFrom(DoubleRangeBlock block, int pos) {
        if (block.isNull(pos)) {
            appendNull();
            return this;
        }
        fromBuilder.copyFrom(block.getDoubleFromBlock(), pos);
        toBuilder.copyFrom(block.getDoubleToBlock(), pos);
        return this;
    }

    @Override
    public DoubleRangeBlockBuilder appendNull() {
        fromBuilder.appendNull();
        toBuilder.appendNull();
        return this;
    }

    @Override
    public DoubleRangeBlockBuilder beginPositionEntry() {
        fromBuilder.beginPositionEntry();
        toBuilder.beginPositionEntry();
        return this;
    }

    @Override
    public DoubleRangeBlockBuilder endPositionEntry() {
        fromBuilder.endPositionEntry();
        toBuilder.endPositionEntry();
        return this;
    }

    @Override
    public AbstractBlockBuilder cancelPositionEntry() {
        throw new UnsupportedOperationException("cancelPositionEntry is not supported by DoubleRangeBlockBuilder");
    }

    public DoubleRangeBlockBuilder appendDoubleRange(double from, double to) {
        fromBuilder.appendDouble(from);
        toBuilder.appendDouble(to);
        return this;
    }

    @Override
    public DoubleRangeBlockBuilder appendDoubleRange(@Nullable DoubleRange lit) {
        if (lit == null) {
            appendNull();
            return this;
        }
        return appendDoubleRange(lit.from(), lit.to());
    }

    @Override
    public DoubleRangeBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        fromBuilder.mvOrdering(mvOrdering);
        toBuilder.mvOrdering(mvOrdering);
        return this;
    }

    @Override
    public DoubleRangeBlock build() {
        DoubleBlock fromBlock = null;
        DoubleBlock toBlock = null;
        boolean success = false;
        try {
            finish();
            fromBlock = fromBuilder.build();
            toBlock = toBuilder.build();
            var block = new DoubleRangeArrayBlock(fromBlock, toBlock);
            success = true;
            return block;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(fromBlock, toBlock);
            }
        }
    }

    @Override
    protected void extraClose() {
        Releasables.closeExpectNoException(fromBuilder, toBuilder);
    }

    @Override
    public BlockLoader.DoubleBuilder from() {
        return fromBuilder;
    }

    @Override
    public BlockLoader.DoubleBuilder to() {
        return toBuilder;
    }

    /**
     * A mutable container for a half-open {@code [from, to)} double range.
     * <p>
     * Instances act both as a value type (used for literals and serialization) and as a reusable
     * scratch passed to {@link DoubleRangeBlock#getDoubleRange(int, DoubleRange)}.
     * The accessor mutates the scratch in place and returns it,
     * so any reference held by the caller is only valid until the next call.
     */
    public static final class DoubleRange implements GenericNamedWriteable, Comparable<DoubleRange> {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            GenericNamedWriteable.class,
            "DoubleRange",
            DoubleRange::new
        );

        private static final TransportVersion INTRODUCED_TRANSPORT_VERSION = TransportVersion.fromName("esql_double_range_value_holder");

        private double from;
        private double to;

        public DoubleRange() {}

        public DoubleRange(double from, double to) {
            this.from = from;
            this.to = to;
        }

        public DoubleRange(StreamInput in) throws IOException {
            this.from = in.readDouble();
            this.to = in.readDouble();
        }

        public double from() {
            return from;
        }

        public double to() {
            return to;
        }

        public DoubleRange reset(double from, double to) {
            this.from = from;
            this.to = to;
            return this;
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return INTRODUCED_TRANSPORT_VERSION;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(from);
            out.writeDouble(to);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof DoubleRange that) {
                return Double.compare(from, that.from) == 0 && Double.compare(to, that.to) == 0;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Double.hashCode(from) * 31 + Double.hashCode(to);
        }

        @Override
        public String toString() {
            return "DoubleRange[from=" + from + ", to=" + to + "]";
        }

        @Override
        public int compareTo(DoubleRange other) {
            int cmp = Double.compare(from, other.from);
            return cmp != 0 ? cmp : Double.compare(to, other.to);
        }
    }
}
