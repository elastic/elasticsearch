/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockBuilder;
import org.elasticsearch.compute.data.Page;

import java.util.HashMap;
import java.util.Map;

@Experimental
public class LongAvgGroupingOperator implements Operator {
    boolean finished;
    boolean returnedResult;
    Page lastPage;

    private final int groupChannel;
    private final int valueChannel;

    // trivial implementation based on Java's HashMap
    private Map<Long, GroupSum> sums;

    public LongAvgGroupingOperator(int valueChannel, int groupChannel) {
        this.valueChannel = valueChannel;
        this.groupChannel = groupChannel;
        sums = new HashMap<>();
    }

    @Override
    public Page getOutput() {
        Page l = lastPage;
        if (l == null) {
            return null; // not ready
        }
        lastPage = null;
        if (finished) {
            sums = null;
        }
        return l;
    }

    @Override
    public void close() { /* no-op */ }

    @Override
    public boolean isFinished() {
        return finished && lastPage == null;
    }

    @Override
    public void finish() {
        if (finished) {
            return;
        }
        finished = true;

        int len = sums.size();
        BlockBuilder groupsBlockBuilder = BlockBuilder.newLongBlockBuilder(len);
        BlockBuilder valuesBlockBuilder = BlockBuilder.newLongBlockBuilder(len);
        int i = 0;
        for (var e : sums.entrySet()) {
            groupsBlockBuilder.appendLong(e.getKey());
            var groupSum = e.getValue();
            valuesBlockBuilder.appendLong(groupSum.sum / groupSum.count);
            i++;
        }
        Block groupBlock = groupsBlockBuilder.build();
        Block averagesBlock = valuesBlockBuilder.build();
        lastPage = new Page(groupBlock, averagesBlock);
    }

    @Override
    public boolean needsInput() {
        return finished == false && lastPage == null;
    }

    static class GroupSum {
        long count;
        long sum;
    }

    @Override
    public void addInput(Page page) {
        Block groupBlock = page.getBlock(groupChannel);
        Block valuesBlock = page.getBlock(valueChannel);
        assert groupBlock.getPositionCount() == valuesBlock.getPositionCount();
        int len = groupBlock.getPositionCount();
        for (int i = 0; i < len; i++) {
            long group = groupBlock.getLong(i);
            long value = valuesBlock.getLong(i);
            var groupSum = sums.computeIfAbsent(group, k -> new GroupSum());
            groupSum.sum += value;
            groupSum.count++;
        }
    }
}
