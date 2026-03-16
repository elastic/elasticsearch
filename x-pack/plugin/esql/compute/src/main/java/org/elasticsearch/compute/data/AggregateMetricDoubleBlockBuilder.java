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

public class AggregateMetricDoubleBlockBuilder extends AbstractBlockBuilder implements BlockLoader.AggregateMetricDoubleBuilder {

    private DoubleBlockBuilder minBuilder;
    private DoubleBlockBuilder maxBuilder;
    private DoubleBlockBuilder sumBuilder;
    private IntBlockBuilder countBuilder;

    public AggregateMetricDoubleBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        minBuilder = null;
        maxBuilder = null;
        sumBuilder = null;
        countBuilder = null;
        try {
            minBuilder = new DoubleBlockBuilder(estimatedSize, blockFactory);
            maxBuilder = new DoubleBlockBuilder(estimatedSize, blockFactory);
            sumBuilder = new DoubleBlockBuilder(estimatedSize, blockFactory);
            countBuilder = new IntBlockBuilder(estimatedSize, blockFactory);
        } finally {
            if (countBuilder == null) {
                Releasables.closeWhileHandlingException(minBuilder, maxBuilder, sumBuilder, countBuilder);
            }
        }
    }

    @Override
    protected int valuesLength() {
        throw new UnsupportedOperationException("Not available on aggregate_metric_double");
    }

    @Override
    protected void growValuesArray(int newSize) {
        throw new UnsupportedOperationException("Not available on aggregate_metric_double");
    }

    @Override
    protected int elementSize() {
        throw new UnsupportedOperationException("Not available on aggregate_metric_double");
    }

    @Override
    public long estimatedBytes() {
        return minBuilder.estimatedBytes() + maxBuilder.estimatedBytes() + sumBuilder.estimatedBytes() + countBuilder.estimatedBytes();
    }

    @Override
    public AggregateMetricDoubleBlockBuilder copyFrom(Block b, int beginInclusive, int endExclusive) {
        Block minBlock;
        Block maxBlock;
        Block sumBlock;
        Block countBlock;
        if (b.areAllValuesNull()) {
            minBlock = b;
            maxBlock = b;
            sumBlock = b;
            countBlock = b;
        } else {
            AggregateMetricDoubleBlock block = (AggregateMetricDoubleBlock) b;
            minBlock = block.minBlock();
            maxBlock = block.maxBlock();
            sumBlock = block.sumBlock();
            countBlock = block.countBlock();
        }
        minBuilder.copyFrom(minBlock, beginInclusive, endExclusive);
        maxBuilder.copyFrom(maxBlock, beginInclusive, endExclusive);
        sumBuilder.copyFrom(sumBlock, beginInclusive, endExclusive);
        countBuilder.copyFrom(countBlock, beginInclusive, endExclusive);
        return this;
    }

    public AggregateMetricDoubleBlockBuilder copyFrom(AggregateMetricDoubleBlock block, int position) {
        if (block.isNull(position)) {
            appendNull();
            return this;
        }

        if (block.minBlock().isNull(position)) {
            min().appendNull();
        } else {
            min().appendDouble(block.minBlock().getDouble(position));
        }
        if (block.maxBlock().isNull(position)) {
            max().appendNull();
        } else {
            max().appendDouble(block.maxBlock().getDouble(position));
        }
        if (block.sumBlock().isNull(position)) {
            sum().appendNull();
        } else {
            sum().appendDouble(block.sumBlock().getDouble(position));
        }
        if (block.countBlock().isNull(position)) {
            count().appendNull();
        } else {
            count().appendInt(block.countBlock().getInt(position));
        }
        return this;
    }

    @Override
    public AggregateMetricDoubleBlockBuilder appendNull() {
        minBuilder.appendNull();
        maxBuilder.appendNull();
        sumBuilder.appendNull();
        countBuilder.appendNull();
        return this;
    }

    @Override
    public AggregateMetricDoubleBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        minBuilder.mvOrdering(mvOrdering);
        maxBuilder.mvOrdering(mvOrdering);
        sumBuilder.mvOrdering(mvOrdering);
        countBuilder.mvOrdering(mvOrdering);
        return this;
    }

    @Override
    public AggregateMetricDoubleBlock build() {
        DoubleBlock minBlock = null;
        DoubleBlock maxBlock = null;
        DoubleBlock sumBlock = null;
        IntBlock countBlock = null;
        boolean success = false;
        try {
            finish();
            minBlock = minBuilder.build();
            maxBlock = maxBuilder.build();
            sumBlock = sumBuilder.build();
            countBlock = countBuilder.build();
            AggregateMetricDoubleBlock block = new AggregateMetricDoubleArrayBlock(minBlock, maxBlock, sumBlock, countBlock);
            success = true;
            return block;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(minBlock, maxBlock, sumBlock, countBlock);
            }
        }
    }

    @Override
    protected void extraClose() {
        Releasables.closeExpectNoException(minBuilder, maxBuilder, sumBuilder, countBuilder);
    }

    @Override
    public BlockLoader.DoubleBuilder min() {
        return minBuilder;
    }

    @Override
    public BlockLoader.DoubleBuilder max() {
        return maxBuilder;
    }

    @Override
    public BlockLoader.DoubleBuilder sum() {
        return sumBuilder;
    }

    @Override
    public BlockLoader.IntBuilder count() {
        return countBuilder;
    }

    public enum Metric {
        MIN(0, "min"),
        MAX(1, "max"),
        SUM(2, "sum"),
        COUNT(3, "value_count"),
        DEFAULT(4, "default");

        private final int index;
        private final String label;

        Metric(int index, String label) {
            this.index = index;
            this.label = label;
        }

        public int getIndex() {
            return index;
        }

        public String getLabel() {
            return label;
        }

        public static Metric indexToMetric(int i) {
            return switch (i) {
                case 0 -> MIN;
                case 1 -> MAX;
                case 2 -> SUM;
                case 3 -> COUNT;
                case 4 -> DEFAULT;
                default -> null;
            };
        }
    }

    /**
     * Literal to represent AggregateMetricDouble and primarily used for testing and during folding.
     * For all other purposes it is preferred to use the individual builders over the literal for generating blocks when possible.
     */
    public record AggregateMetricDoubleLiteral(Double min, Double max, Double sum, Integer count)
        implements
            GenericNamedWriteable,
            Comparable<AggregateMetricDoubleLiteral> {

        private static final TransportVersion ESQL_AGGREGATE_METRIC_DOUBLE_LITERAL = TransportVersion.fromName(
            "esql_aggregate_metric_double_literal"
        );

        public AggregateMetricDoubleLiteral {
            min = (min == null || min.isNaN()) ? null : min;
            max = (max == null || max.isNaN()) ? null : max;
            sum = (sum == null || sum.isNaN()) ? null : sum;
        }

        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            GenericNamedWriteable.class,
            "AggregateMetricDoubleLiteral",
            AggregateMetricDoubleLiteral::new
        );

        @Override
        public String getWriteableName() {
            return "AggregateMetricDoubleLiteral";
        }

        public AggregateMetricDoubleLiteral(StreamInput input) throws IOException {
            this(input.readOptionalDouble(), input.readOptionalDouble(), input.readOptionalDouble(), input.readOptionalInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalDouble(min);
            out.writeOptionalDouble(max);
            out.writeOptionalDouble(sum);
            out.writeOptionalInt(count);
        }

        @Override
        public boolean supportsVersion(TransportVersion version) {
            return version.supports(ESQL_AGGREGATE_METRIC_DOUBLE_LITERAL);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            assert false : "must not be called when overriding supportsVersion";
            throw new UnsupportedOperationException("must not be called when overriding supportsVersion");
        }

        @Override
        public int compareTo(AggregateMetricDoubleLiteral other) {
            int c = Double.compare(min, other.min);
            if (c != 0) {
                return c;
            }
            c = Double.compare(max, other.max);
            if (c != 0) {
                return c;
            }
            c = Double.compare(sum, other.sum);
            if (c != 0) {
                return c;
            }
            return Double.compare(count, other.count);
        }
    }

    public AggregateMetricDoubleBlockBuilder appendLiteral(AggregateMetricDoubleLiteral literal) {
        if (literal.min != null) {
            minBuilder.appendDouble(literal.min);
        } else {
            minBuilder.appendNull();
        }
        if (literal.max != null) {
            maxBuilder.appendDouble(literal.max);
        } else {
            maxBuilder.appendNull();
        }
        if (literal.sum != null) {
            sumBuilder.appendDouble(literal.sum);
        } else {
            sumBuilder.appendNull();
        }
        if (literal.count != null) {
            countBuilder.appendInt(literal.count);
        } else {
            countBuilder.appendNull();
        }
        return this;
    }
}
