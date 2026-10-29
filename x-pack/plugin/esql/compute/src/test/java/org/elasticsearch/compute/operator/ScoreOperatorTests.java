/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ConstantEvaluators;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ScoreOperatorTests extends ESTestCase {

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    public void testAddsOntoRealScoreBaseline() {
        int positions = 2;
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
        ScoreOperator scoreOp = new ScoreOperator(blockFactory, ConstantEvaluators.constantDouble(2.0).get(driverContext), 0);
        scoreOp.addInput(new Page(blockFactory.newConstantDoubleBlockWith(0.0, positions)));
        Page result = scoreOp.getOutput();
        try {
            DoubleBlock scoreOut = result.getBlock(0);
            for (int i = 0; i < positions; i++) {
                assertThat(scoreOut.getDouble(i), equalTo(2.0));
            }
        } finally {
            result.releaseBlocks();
            scoreOp.close();
        }
    }

    // Rows without a score baseline, e.g. every row of a union branch that did not ask for METADATA _score, have
    // nothing to add onto and stay null.
    public void testKeepsAllNullScoreBaseline() {
        int positions = 2;
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
        ScoreOperator scoreOp = new ScoreOperator(blockFactory, ConstantEvaluators.constantDouble(2.0).get(driverContext), 0);
        scoreOp.addInput(new Page(blockFactory.newConstantNullBlock(positions)));
        Page result = scoreOp.getOutput();
        try {
            DoubleBlock scoreOut = result.getBlock(0);
            for (int i = 0; i < positions; i++) {
                assertThat(scoreOut.isNull(i), is(true));
            }
        } finally {
            result.releaseBlocks();
            scoreOp.close();
        }
    }

    // A merge or TopN can interleave rows from a scored branch with rows from an unscored one in the same block:
    // the scored rows get the search's score added, the unscored ones stay null.
    public void testAddsOntoMixedNullScoreBaseline() {
        DoubleBlock mixedBaseline;
        try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(3)) {
            builder.appendNull();
            builder.appendDouble(0.0);
            builder.appendDouble(1.5);
            mixedBaseline = builder.build();
        }
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
        ScoreOperator scoreOp = new ScoreOperator(blockFactory, ConstantEvaluators.constantDouble(2.0).get(driverContext), 0);
        scoreOp.addInput(new Page(mixedBaseline));
        Page result = scoreOp.getOutput();
        try {
            DoubleBlock scoreOut = result.getBlock(0);
            assertThat(scoreOut.isNull(0), is(true));
            assertThat(scoreOut.getDouble(scoreOut.getFirstValueIndex(1)), equalTo(2.0));
            assertThat(scoreOut.getDouble(scoreOut.getFirstValueIndex(2)), equalTo(3.5));
        } finally {
            result.releaseBlocks();
            scoreOp.close();
        }
    }
}
