/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;

/**
 * Operator for the {@link org.elasticsearch.xpack.esql.plan.physical.IntoExec.Mode#MERGE_STATS} phase of an {@code INTO} command.
 * Receives one {@code [total, created]} row from each upstream driver and sums
 * them into a single final row. Emits {@code [0, 0]} when no input pages arrive.
 * Where more features are added to {@code INTO} in the future, this operator will be extended to merge additional statistics.
 */
public final class IntoMergeStatsOperator implements Operator {
    public record Factory() implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new IntoMergeStatsOperator(driverContext.blockFactory());
        }

        @Override
        public String describe() {
            return "IntoMergeStatsOperator";
        }
    }

    private final BlockFactory blockFactory;

    private long totalSum;
    private long createdSum;
    private boolean finished;
    private boolean outputEmitted;

    IntoMergeStatsOperator(BlockFactory blockFactory) {
        this.blockFactory = blockFactory;
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        try {
            LongBlock totalBlock = page.getBlock(0);
            LongBlock createdBlock = page.getBlock(1);
            for (int i = 0; i < page.getPositionCount(); i++) {
                totalSum += totalBlock.getLong(totalBlock.getFirstValueIndex(i));
                createdSum += createdBlock.getLong(createdBlock.getFirstValueIndex(i));
            }
        } finally {
            page.releaseBlocks();
        }
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return outputEmitted;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return finished && outputEmitted == false;
    }

    @Override
    public Page getOutput() {
        if (finished == false) {
            return null;
        }
        outputEmitted = true;
        return new IntoStats(totalSum, createdSum).toPage(blockFactory);
    }

    @Override
    public void close() {}
}
