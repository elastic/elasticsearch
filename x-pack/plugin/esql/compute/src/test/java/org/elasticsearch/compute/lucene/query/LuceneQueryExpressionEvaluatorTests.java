/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.search.Scorable;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.query.LuceneQueryEvaluator.DenseCollector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Operator;

import static org.hamcrest.Matchers.equalTo;

public class LuceneQueryExpressionEvaluatorTests extends LuceneQueryEvaluatorTests<BooleanBlock, BooleanBlock.Builder> {

    private final boolean useScoring = randomBoolean();

    @Override
    protected DenseCollector<BooleanBlock.Builder> createDenseCollector(int min, int max) {
        return new LuceneQueryEvaluator.DenseCollector<>(
            min,
            max,
            blockFactory().newBooleanBlockBuilder(max - min + 1),
            null,
            b -> b.appendBoolean(false),
            (b, s) -> b.appendBoolean(true),
            null
        );
    }

    @Override
    protected Scorable getScorer() {
        return null;
    }

    @Override
    protected Operator createOperator(DriverContext ctx, IndexedByShardId<LuceneQueryEvaluator.ShardConfig> shards) {
        return new EvalOperator(ctx, new LuceneQueryExpressionEvaluator(ctx.blockFactory(), shards));
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
    protected void assertCollectedResultMatch(BooleanBlock resultVector, int position, boolean isMatch) {
        assertThat(resultVector.getBoolean(position), equalTo(isMatch));
    }

    @Override
    protected void assertTermResultMatch(BooleanBlock resultVector, int position, boolean isMatch) {
        assertThat(resultVector.getBoolean(position), equalTo(isMatch));
    }
}
