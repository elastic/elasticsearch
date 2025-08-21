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
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;

import static org.elasticsearch.index.mapper.RangeFieldMapper.ESQL_DATE_RANGE_CREATED_VERSION;

public class DateRangeBlockBuilder extends AbstractBlockBuilder implements BlockLoader.DateRangeBuilder {

    private LongBlockBuilder fromBuilder;
    private LongBlockBuilder toBuilder;

    public DateRangeBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
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
        throw new UnsupportedOperationException("Not available on date_range");
    }

    @Override
    protected void growValuesArray(int newSize) {
        throw new UnsupportedOperationException("Not available on date_range");
    }

    @Override
    protected int elementSize() {
        throw new UnsupportedOperationException("Not available on date_range");
    }

    @Override
    public long estimatedBytes() {
        return fromBuilder.estimatedBytes() + toBuilder.estimatedBytes();
    }

    @Override
    public DateRangeBlockBuilder copyFrom(Block b, int beginInclusive, int endExclusive) {
        Block fromBlock;
        Block toBlock;
        if (b.areAllValuesNull()) {
            fromBlock = b;
            toBlock = b;
        } else {
            var block = (DateRangeArrayBlock) b;
            fromBlock = block.getFromBlock();
            toBlock = block.getToBlock();
        }
        fromBuilder.copyFrom(fromBlock, beginInclusive, endExclusive);
        toBuilder.copyFrom(toBlock, beginInclusive, endExclusive);
        return this;
    }

    public DateRangeBlockBuilder copyFrom(DateRangeBlock block, int pos) {
        if (block.isNull(pos)) {
            appendNull();
            return this;
        }

        if (block.getFromBlock().isNull(pos)) {
            from().appendNull();
        } else {
            from().appendLong(block.getFromBlock().getLong(pos));
        }

        if (block.getToBlock().isNull(pos)) {
            to().appendNull();
        } else {
            to().appendLong(block.getToBlock().getLong(pos));
        }
        return this;
    }

    @Override
    public DateRangeBlockBuilder appendNull() {
        fromBuilder.appendNull();
        toBuilder.appendNull();
        return this;
    }

    public DateRangeBlockBuilder appendDateRange(DateRangeLiteral lit) {
        fromBuilder.appendLong(lit.from);
        toBuilder.appendLong(lit.to);
        return this;
    }

    @Override
    public DateRangeBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        fromBuilder.mvOrdering(mvOrdering);
        toBuilder.mvOrdering(mvOrdering);
        return this;
    }

    @Override
    public DateRangeBlock build() {
        LongBlock fromBlock = null;
        LongBlock toBlock = null;
        boolean success = false;
        try {
            finish();
            fromBlock = fromBuilder.build();
            toBlock = toBuilder.build();
            var block = new DateRangeArrayBlock(fromBlock, toBlock);
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

    public record DateRangeLiteral(Long from, Long to) implements GenericNamedWriteable {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            GenericNamedWriteable.class,
            "DateRangeLiteral",
            DateRangeLiteral::new
        );

        public DateRangeLiteral(StreamInput in) throws IOException {
            this(in.readOptionalLong(), in.readOptionalLong());
        }

        @Override
        public String getWriteableName() {
            return "DateRangeLiteral";
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return ESQL_DATE_RANGE_CREATED_VERSION;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalLong(from);
            out.writeOptionalLong(to);
        }
    }
}
