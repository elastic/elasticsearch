/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.operator;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.xpack.sql.action.compute.aggregation.GroupingAggregator;
import org.elasticsearch.xpack.sql.action.compute.data.Block;
import org.elasticsearch.xpack.sql.action.compute.data.LongBlock;
import org.elasticsearch.xpack.sql.action.compute.data.Page;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class HashAggregationOperator implements Operator {

    // monotonically increasing state
    private static final int NEEDS_INPUT = 0;
    private static final int HAS_OUTPUT = 1;
    private static final int FINISHING = 2;
    private static final int FINISHED = 3;

    private int state;

    private final int groupByChannel;

    private final LongHash longHash;

    private final List<GroupingAggregator> aggregators;

    public HashAggregationOperator(int groupByChannel, List<GroupingAggregator> aggregators, BigArrays bigArrays) {
        Objects.requireNonNull(aggregators);
        // checkNonEmpty(aggregators);
        this.groupByChannel = groupByChannel;
        this.aggregators = aggregators;
        this.longHash = new LongHash(1, bigArrays);
        state = NEEDS_INPUT;
    }

    @Override
    public boolean needsInput() {
        return state == NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page) {
        checkState(needsInput(), "Operator is already finishing");
        requireNonNull(page, "page is null");

        LongBlock block = (LongBlock) page.getBlock(groupByChannel);
        long[] groups = new long[block.getPositionCount()];
        for (int i = 0; i < block.getPositionCount(); i++) {
            long value = block.getLong(i);
            long bucketOrd = longHash.add(value);
            if (bucketOrd < 0) { // already seen
                bucketOrd = -1 - bucketOrd;
            }
            groups[i] = bucketOrd;
        }
        Block groupIdBlock = new LongBlock(groups, groups.length);

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

        Block[] blocks = new Block[aggregators.size() + 1];
        long[] values = new long[(int) longHash.size()];
        for (int i = 0; i < (int) longHash.size(); i++) {
            values[i] = longHash.get(i);
        }
        blocks[0] = new LongBlock(values, values.length);
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
    public void close() {}

    private static void checkState(boolean condition, String msg) {
        if (condition == false) {
            throw new IllegalArgumentException(msg);
        }
    }
}
