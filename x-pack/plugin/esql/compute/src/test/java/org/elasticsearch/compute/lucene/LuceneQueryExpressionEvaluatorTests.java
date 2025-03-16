/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.search.Scorable;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.LuceneQueryEvaluator.DenseCollector;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Operator;

import static org.hamcrest.Matchers.equalTo;

public class LuceneQueryExpressionEvaluatorTests extends LuceneQueryEvaluatorTests<BooleanVector, BooleanVector.Builder> {

    private final boolean useScoring = randomBoolean();

    @Override
    protected DenseCollector<BooleanVector.Builder> createDenseCollector(int min, int max) {
        return new LuceneQueryEvaluator.DenseCollector<>(
            min,
            max,
            blockFactory().newBooleanVectorFixedBuilder(max - min + 1),
            b -> b.appendBoolean(false),
            (b, s) -> b.appendBoolean(true)
        );
    }

    @Override
    protected Scorable getScorer() {
        return null;
    }

    @Override
    protected Operator createOperator(BlockFactory blockFactory, LuceneQueryEvaluator.ShardConfig[] shards) {
        return new EvalOperator(blockFactory, new LuceneQueryExpressionEvaluator(blockFactory, shards));
    }

    @Override
    protected boolean usesScoring() {
        // Be consistent for a single test execution
        return useScoring;
    }

    @Override
    protected int resultsBlockIndex(Page page) {
        return page.getBlockCount() - 1;
    }

    @Override
    protected void assertCollectedResultMatch(BooleanVector resultVector, int position, boolean isMatch) {
        assertThat(resultVector.getBoolean(position), equalTo(isMatch));
    }

    @Override
    protected void assertTermResultMatch(BooleanVector resultVector, int position, boolean isMatch) {
        assertThat(resultVector.getBoolean(position), equalTo(isMatch));
    }
}
