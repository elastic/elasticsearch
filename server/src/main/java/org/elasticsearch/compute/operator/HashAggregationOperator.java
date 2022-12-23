/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.aggregation.BlockHash;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
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

    private int state;

    private final int groupByChannel;

    private final BlockHash blockHash;

    private final List<GroupingAggregator> aggregators;

    public record HashAggregationOperatorFactory(
        int groupByChannel,
        List<GroupingAggregator.GroupingAggregatorFactory> aggregators,
        Supplier<BlockHash> blockHash
    ) implements OperatorFactory {
        @Override
        public Operator get() {
            return new HashAggregationOperator(groupByChannel, aggregators, blockHash);
        }

        @Override
        public String describe() {
            return "HashAggregationOperator(mode = "
                + "<not-needed>"
                + ", aggs = "
                + aggregators.stream().map(Describable::describe).collect(joining(", "))
                + ")";
        }
    }

    public HashAggregationOperator(
        int groupByChannel,
        List<GroupingAggregator.GroupingAggregatorFactory> aggregators,
        Supplier<BlockHash> blockHash
    ) {
        this.groupByChannel = groupByChannel;
        state = NEEDS_INPUT;

        this.aggregators = new ArrayList<>(aggregators.size());
        boolean success = false;
        try {
            for (GroupingAggregator.GroupingAggregatorFactory a : aggregators) {
                this.aggregators.add(a.get());
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

        Block block = page.getBlock(groupByChannel);
        long[] groups = new long[block.getPositionCount()];
        for (int i = 0; i < block.getPositionCount(); i++) {
            long bucketOrd = blockHash.add(block, i);
            if (bucketOrd < 0) { // already seen
                bucketOrd = -1 - bucketOrd;
            }
            groups[i] = bucketOrd;
        }
        Vector groupIdVector = new LongVector(groups, groups.length);

        for (GroupingAggregator aggregator : aggregators) {
            aggregator.processPage(groupIdVector, page);
        }
    }

    @Override
    public Page getOutput() {
        if (state != HAS_OUTPUT) {
            return null;
        }

        state = FINISHING;  // << allows to produce output step by step

        Block[] blocks = new Block[aggregators.size() + 1];
        blocks[0] = blockHash.getKeys();
        for (int i = 0; i < aggregators.size(); i++) {
            var aggregator = aggregators.get(i);
            blocks[i + 1] = aggregator.evaluate();
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

    private static void checkState(boolean condition, String msg) {
        if (condition == false) {
            throw new IllegalArgumentException(msg);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("groupByChannel=").append(groupByChannel).append(", ");
        sb.append("aggregators=").append(aggregators);
        sb.append("]");
        return sb.toString();
    }
}
