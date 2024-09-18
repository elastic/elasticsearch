/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasable;

import java.util.ArrayList;
import java.util.List;

public record GroupingKey(AggregatorMode mode, Thing thing, BlockFactory blockFactory) implements EvalOperator.ExpressionEvaluator {
    public interface Thing extends Releasable {
        int extraIntermediateBlocks();

        Block evalRawInput(Page page);

        Block evalIntermediateInput(BlockFactory blockFactory, Page page);

        void fetchIntermediateState(BlockFactory blockFactory, Block[] blocks, int positionCount);

        void replaceIntermediateKeys(BlockFactory blockFactory, Block[] blocks);
    }

    public interface Supplier {
        Factory get(AggregatorMode mode);
    }

    public interface Factory {
        GroupingKey apply(DriverContext context, int resultOffset);

        ElementType intermediateElementType();

        GroupingAggregator.Factory valuesAggregatorForGroupingsInTimeSeries(int timeBucketChannel);
    }

    public static GroupingKey.Supplier forStatelessGrouping(int channel, ElementType elementType) {
        return mode -> new Factory() {
            @Override
            public GroupingKey apply(DriverContext context, int resultOffset) {
                return new GroupingKey(mode, new Load(channel, resultOffset), context.blockFactory());
            }

            @Override
            public ElementType intermediateElementType() {
                return elementType;
            }

            @Override
            public GroupingAggregator.Factory valuesAggregatorForGroupingsInTimeSeries(int timeBucketChannel) {
                if (channel != timeBucketChannel) {
                    final List<Integer> channels = List.of(channel);
                    // TODO: perhaps introduce a specialized aggregator for this?
                    return (switch (intermediateElementType()) {
                        case BYTES_REF -> new ValuesBytesRefAggregatorFunctionSupplier(channels);
                        case DOUBLE -> new ValuesDoubleAggregatorFunctionSupplier(channels);
                        case INT -> new ValuesIntAggregatorFunctionSupplier(channels);
                        case LONG -> new ValuesLongAggregatorFunctionSupplier(channels);
                        case BOOLEAN -> new ValuesBooleanAggregatorFunctionSupplier(channels);
                        case FLOAT, NULL, DOC, COMPOSITE, UNKNOWN -> throw new IllegalArgumentException("unsupported grouping type");
                    }).groupingAggregatorFactory(AggregatorMode.SINGLE);
                }
                return null;
            }
        };
    }

    public static List<BlockHash.GroupSpec> toBlockHashGroupSpec(List<GroupingKey.Factory> keys) {
        List<BlockHash.GroupSpec> result = new ArrayList<>(keys.size());
        for (int k = 0; k < keys.size(); k++) {
            result.add(new BlockHash.GroupSpec(k, keys.get(k).intermediateElementType()));
        }
        return result;
    }

    @Override
    public Block eval(Page page) {
        return mode.isInputPartial() ? thing.evalIntermediateInput(blockFactory, page) : thing.evalRawInput(page);
    }

    public int finishBlockCount() {
        return mode.isOutputPartial() ? 1 + thing.extraIntermediateBlocks() : 1;
    }

    public void finish(Block[] blocks, IntVector selected, DriverContext driverContext) {
        if (mode.isOutputPartial()) {
            thing.fetchIntermediateState(driverContext.blockFactory(), blocks, selected.getPositionCount());
        } else {
            thing.replaceIntermediateKeys(driverContext.blockFactory(), blocks);
        }
    }

    public int extraIntermediateBlocks() {
        return thing.extraIntermediateBlocks();
    }

    @Override
    public void close() {
        thing.close();
    }

    private record Load(int channel, int resultOffset) implements Thing {
        @Override
        public int extraIntermediateBlocks() {
            return 0;
        }

        @Override
        public Block evalRawInput(Page page) {
            Block b = page.getBlock(channel);
            b.incRef();
            return b;
        }

        @Override
        public Block evalIntermediateInput(BlockFactory blockFactory, Page page) {
            Block b = page.getBlock(resultOffset);
            b.incRef();
            return b;
        }

        @Override
        public void fetchIntermediateState(BlockFactory blockFactory, Block[] blocks, int positionCount) {}

        @Override
        public void replaceIntermediateKeys(BlockFactory blockFactory, Block[] blocks) {}

        @Override
        public void close() {}
    }
}
