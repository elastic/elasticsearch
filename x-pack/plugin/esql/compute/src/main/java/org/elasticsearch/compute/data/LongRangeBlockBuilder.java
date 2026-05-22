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

import static org.elasticsearch.index.mapper.RangeFieldMapper.ESQL_LONG_RANGES;

public final class LongRangeBlockBuilder extends AbstractBlockBuilder implements LongRangeBlock.Builder {

    private LongBlockBuilder fromBuilder;
    private LongBlockBuilder toBuilder;

    public LongRangeBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        fromBuilder = null;
        toBuilder = null;
        try {
            fromBuilder = new LongBlockBuilder(estimatedSize, blockFactory);
            toBuilder = new LongBlockBuilder(estimatedSize, blockFactory);
        } finally {
            if (toBuilder == null) {
                Releasables.closeWhileHandlingException(fromBuilder);
            }
        }
    }

    @Override
    protected int valuesLength() {
        throw new UnsupportedOperationException("Not available on long_range");
    }

    @Override
    protected void growValuesArray(int newSize) {
        throw new UnsupportedOperationException("Not available on long_range");
    }

    @Override
    protected int elementSize() {
        throw new UnsupportedOperationException("Not available on long_range");
    }

    @Override
    public long estimatedBytes() {
        return fromBuilder.estimatedBytes() + toBuilder.estimatedBytes();
    }

    @Override
    public LongRangeBlockBuilder copyFrom(Block b, int beginInclusive, int endExclusive) {
        Block fromBlock;
        Block toBlock;
        if (b.areAllValuesNull()) {
            fromBlock = b;
            toBlock = b;
        } else {
            var block = (LongRangeArrayBlock) b;
            fromBlock = block.getFromBlock();
            toBlock = block.getToBlock();
        }
        fromBuilder.copyFrom(fromBlock, beginInclusive, endExclusive);
        toBuilder.copyFrom(toBlock, beginInclusive, endExclusive);
        return this;
    }

    @Override
    public LongRangeBlockBuilder copyFrom(LongRangeBlock block, int pos) {
        if (block.isNull(pos)) {
            appendNull();
            return this;
        }
        fromBuilder.copyFrom(block.getFromBlock(), pos);
        toBuilder.copyFrom(block.getToBlock(), pos);
        return this;
    }

    @Override
    public LongRangeBlockBuilder appendNull() {
        fromBuilder.appendNull();
        toBuilder.appendNull();
        return this;
    }

    @Override
    public LongRangeBlockBuilder beginPositionEntry() {
        fromBuilder.beginPositionEntry();
        toBuilder.beginPositionEntry();
        return this;
    }

    @Override
    public LongRangeBlockBuilder endPositionEntry() {
        fromBuilder.endPositionEntry();
        toBuilder.endPositionEntry();
        return this;
    }

    public LongRangeBlockBuilder appendLongRange(long from, long to) {
        fromBuilder.appendLong(from);
        toBuilder.appendLong(to);
        return this;
    }

    @Override
    public LongRangeBlockBuilder appendLongRange(@Nullable LongRange lit) {
        if (lit == null) {
            appendNull();
            return this;
        }
        fromBuilder.appendLong(lit.from());
        toBuilder.appendLong(lit.to());
        return this;
    }

    @Override
    public LongRangeBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        fromBuilder.mvOrdering(mvOrdering);
        toBuilder.mvOrdering(mvOrdering);
        return this;
    }

    @Override
    public LongRangeBlock build() {
        LongBlock fromBlock = null;
        LongBlock toBlock = null;
        boolean success = false;
        try {
            finish();
            fromBlock = fromBuilder.build();
            toBlock = toBuilder.build();
            var block = new LongRangeArrayBlock(fromBlock, toBlock);
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
    public BlockLoader.LongBuilder from() {
        return fromBuilder;
    }

    @Override
    public BlockLoader.LongBuilder to() {
        return toBuilder;
    }

    /**
     * A mutable container for a half-open {@code [from, to)} long range.
     * <p>
     * Instances act both as a value type (used for literals and serialization) and as a reusable
     * scratch passed to {@link LongRangeBlock#getLongRange(int, LongRange)}.
     * The accessor mutates the scratch in place and returns it,
     * so any reference held by the caller is only valid until the next call.
     */
    public static final class LongRange implements GenericNamedWriteable, Comparable<LongRange> {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            GenericNamedWriteable.class,
            "LongRange",
            LongRange::new
        );

        private long from;
        private long to;

        public LongRange() {}

        public LongRange(long from, long to) {
            this.from = from;
            this.to = to;
        }

        public LongRange(StreamInput in) throws IOException {
            this.from = in.readLong();
            this.to = in.readLong();
        }

        public long from() {
            return from;
        }

        public long to() {
            return to;
        }

        public LongRange reset(long from, long to) {
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
            return ESQL_LONG_RANGES;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(from);
            out.writeLong(to);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof LongRange that) {
                return from == that.from && to == that.to;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(from) * 31 + Long.hashCode(to);
        }

        @Override
        public String toString() {
            return "LongRange[from=" + from + ", to=" + to + "]";
        }

        @Override
        public int compareTo(LongRange other) {
            int cmp = Long.compare(from, other.from);
            return cmp != 0 ? cmp : Long.compare(to, other.to);
        }
    }
}
