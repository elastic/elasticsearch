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
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

@Experimental
public class HashAggregationOperator implements Operator {

    // monotonically increasing state
    private static final int NEEDS_INPUT = 0;
    private static final int HAS_OUTPUT = 1;
    private static final int FINISHING = 2;
    private static final int FINISHED = 3;

    public record GroupSpec(int channel, ElementType elementType) {}

    public record HashAggregationOperatorFactory(
        List<GroupSpec> groups,
        List<GroupingAggregator.GroupingAggregatorFactory> aggregators,
        BigArrays bigArrays
    ) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new HashAggregationOperator(aggregators, () -> BlockHash.build(groups, bigArrays), driverContext);
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

    private int state;

    private final BlockHash blockHash;

    private final List<GroupingAggregator> aggregators;

    public HashAggregationOperator(
        List<GroupingAggregator.GroupingAggregatorFactory> aggregators,
        Supplier<BlockHash> blockHash,
        DriverContext driverContext
    ) {
        state = NEEDS_INPUT;

        this.aggregators = new ArrayList<>(aggregators.size());
        boolean success = false;
        try {
            for (GroupingAggregator.GroupingAggregatorFactory a : aggregators) {
                this.aggregators.add(a.apply(driverContext));
            }
            this.blockHash = blockHash.get();
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    public boolean needsInput() {
        return state == NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page) {
        checkState(needsInput(), "Operator is already finishing");
        requireNonNull(page, "page is null");

        LongBlock groupIdBlock = blockHash.add(wrapPage(page));

        for (GroupingAggregator aggregator : aggregators) {
            aggregator.processPage(groupIdBlock, page);
        }
    }

    @Override
    public Page getOutput() {
        if (state != HAS_OUTPUT) {
            return null;
        }

        state = FINISHING;  // << allows to produce output step by step

        Block[] keys = blockHash.getKeys();
        IntVector selected = blockHash.nonEmpty();
        Block[] blocks = new Block[keys.length + aggregators.size()];
        System.arraycopy(keys, 0, blocks, 0, keys.length);
        for (int i = 0; i < aggregators.size(); i++) {
            var aggregator = aggregators.get(i);
            blocks[i + keys.length] = aggregator.evaluate(selected);
        }

        Page page = new Page(blocks);
        state = FINISHED;
        return page;
    }

    @Override
    public void finish() {
        if (state == NEEDS_INPUT) {
            state = HAS_OUTPUT;
        }
    }

    @Override
    public boolean isFinished() {
        return state == FINISHED;
    }

    @Override
    public void close() {
        Releasables.close(blockHash, () -> Releasables.close(aggregators));
    }

    protected BlockHash blockHash() {
        return blockHash;
    }

    protected List<GroupingAggregator> aggregators() {
        return aggregators;
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
