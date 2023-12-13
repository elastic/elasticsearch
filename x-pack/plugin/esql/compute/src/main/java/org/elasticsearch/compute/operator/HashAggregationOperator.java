/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class HashAggregationOperator implements Operator {

    public record GroupSpec(int channel, ElementType elementType) {}

    public record HashAggregationOperatorFactory(
        List<GroupSpec> groups,
        List<GroupingAggregator.Factory> aggregators,
        int maxPageSize,
        BigArrays bigArrays
    ) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new HashAggregationOperator(
                aggregators,
                () -> BlockHash.build(groups, driverContext, maxPageSize, false),
                driverContext
            );
        }

        @Override
        public String describe() {
            return "HashAggregationOperator[mode = "
                + "<not-needed>"
                + ", aggs = "
                + aggregators.stream().map(Describable::describe).collect(joining(", "))
                + "]";
        }
    }

    private boolean finished;
    private Page output;

    private final BlockHash blockHash;

    private final List<GroupingAggregator> aggregators;

    private final DriverContext driverContext;

    @SuppressWarnings("this-escape")
    public HashAggregationOperator(
        List<GroupingAggregator.Factory> aggregators,
        Supplier<BlockHash> blockHash,
        DriverContext driverContext
    ) {
        this.aggregators = new ArrayList<>(aggregators.size());
        this.driverContext = driverContext;
        boolean success = false;
        try {
            this.blockHash = blockHash.get();
            for (GroupingAggregator.Factory a : aggregators) {
                this.aggregators.add(a.apply(driverContext));
            }
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        try {
            checkState(needsInput(), "Operator is already finishing");
            requireNonNull(page, "page is null");

            GroupingAggregatorFunction.AddInput[] prepared = new GroupingAggregatorFunction.AddInput[aggregators.size()];
            for (int i = 0; i < prepared.length; i++) {
                prepared[i] = aggregators.get(i).prepareProcessPage(blockHash, page);
            }

            blockHash.add(wrapPage(page), new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntBlock groupIds) {
                    IntVector groupIdsVector = groupIds.asVector();
                    if (groupIdsVector != null) {
                        add(positionOffset, groupIdsVector);
                    } else {
                        for (GroupingAggregatorFunction.AddInput p : prepared) {
                            p.add(positionOffset, groupIds);
                        }
                    }
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    for (GroupingAggregatorFunction.AddInput p : prepared) {
                        p.add(positionOffset, groupIds);
                    }
                }
            });
        } finally {
            page.releaseBlocks();
        }
    }

    @Override
    public Page getOutput() {
        Page p = output;
        output = null;
        return p;
    }

    @Override
    public void finish() {
        if (finished) {
            return;
        }
        finished = true;
        Block[] blocks = null;
        IntVector selected = null;
        boolean success = false;
        try {
            selected = blockHash.nonEmpty();
            Block[] keys = blockHash.getKeys();
            int[] aggBlockCounts = aggregators.stream().mapToInt(GroupingAggregator::evaluateBlockCount).toArray();
            blocks = new Block[keys.length + Arrays.stream(aggBlockCounts).sum()];
            System.arraycopy(keys, 0, blocks, 0, keys.length);
            int offset = keys.length;
            for (int i = 0; i < aggregators.size(); i++) {
                var aggregator = aggregators.get(i);
                aggregator.evaluate(blocks, offset, selected, driverContext);
                offset += aggBlockCounts[i];
            }
            output = new Page(blocks);
            success = true;
        } finally {
            // selected should always be closed
            if (selected != null) {
                selected.close();
            }
            if (success == false && blocks != null) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    @Override
    public boolean isFinished() {
        return finished && output == null;
    }

    @Override
    public void close() {
        if (output != null) {
            output.releaseBlocks();
        }
        Releasables.close(blockHash, () -> Releasables.close(aggregators));
    }

    protected static void checkState(boolean condition, String msg) {
        if (condition == false) {
            throw new IllegalArgumentException(msg);
        }
    }

    protected Page wrapPage(Page page) {
        return page;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("blockHash=").append(blockHash).append(", ");
        sb.append("aggregators=").append(aggregators);
        sb.append("]");
        return sb.toString();
    }
}
