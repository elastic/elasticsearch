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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

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

    // A pre-fix federated node still answering an all-null _score baseline: nothing sound to add onto.
    public void testThrowsOnAllNullScoreBaseline() {
        int positions = 2;
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
        ScoreOperator scoreOp = new ScoreOperator(blockFactory, ConstantEvaluators.constantDouble(2.0).get(driverContext), 0);
        scoreOp.addInput(new Page(blockFactory.newConstantNullBlock(positions)));
        try {
            IllegalStateException e = expectThrows(IllegalStateException.class, scoreOp::getOutput);
            assertThat(e.getMessage(), containsString("predates federated runtime scoring support"));
        } finally {
            scoreOp.close();
        }
    }

    // A TopN can rebuild one page from rows sourced across a mixed-version cluster, interleaving a pre-fix
    // node's null _score with a fixed node's real baseline in the same block. Neither areAllValuesNull() nor
    // a real DoubleVector applies here, so the check must key off asVector() rather than "all null".
    public void testThrowsOnMixedNullScoreBaseline() {
        int positions = 2;
        DoubleBlock mixedBaseline;
        try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(positions)) {
            builder.appendNull();
            builder.appendDouble(0.0);
            mixedBaseline = builder.build();
        }
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
        ScoreOperator scoreOp = new ScoreOperator(blockFactory, ConstantEvaluators.constantDouble(2.0).get(driverContext), 0);
        scoreOp.addInput(new Page(mixedBaseline));
        try {
            IllegalStateException e = expectThrows(IllegalStateException.class, scoreOp::getOutput);
            assertThat(e.getMessage(), containsString("predates federated runtime scoring support"));
        } finally {
            scoreOp.close();
        }
    }
}
