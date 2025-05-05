/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
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
    public Block.Builder copyFrom(Block b, int beginInclusive, int endExclusive) {
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

    @Override
    public AbstractBlockBuilder appendNull() {
        minBuilder.appendNull();
        maxBuilder.appendNull();
        sumBuilder.appendNull();
        countBuilder.appendNull();
        return this;
    }

    @Override
    public Block.Builder mvOrdering(Block.MvOrdering mvOrdering) {
        minBuilder.mvOrdering(mvOrdering);
        maxBuilder.mvOrdering(mvOrdering);
        sumBuilder.mvOrdering(mvOrdering);
        countBuilder.mvOrdering(mvOrdering);
        return this;
    }

    @Override
    public Block build() {
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
            AggregateMetricDoubleBlock block = new AggregateMetricDoubleBlock(minBlock, maxBlock, sumBlock, countBlock);
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
        COUNT(3, "value_count");

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
    }

    public record AggregateMetricDoubleLiteral(Double min, Double max, Double sum, Integer count) implements GenericNamedWriteable {
        public AggregateMetricDoubleLiteral {
            min = min.isNaN() ? null : min;
            max = max.isNaN() ? null : max;
            sum = sum.isNaN() ? null : sum;
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
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.ESQL_AGGREGATE_METRIC_DOUBLE_LITERAL;
        }

    }
}
