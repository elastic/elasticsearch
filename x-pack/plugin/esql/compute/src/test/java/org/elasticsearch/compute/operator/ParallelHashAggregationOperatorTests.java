/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.matchesPattern;

/**
 * Runs the full {@link HashAggregationOperatorTests} suite against the parallel path by overriding
 * {@link #simpleWithMode} to wrap FINAL and SINGLE factories with
 * {@link ParallelHashAggregationOperator.Factory}.
 *
 * <p>The promotion threshold is set to 0 so PROBING→PARALLEL promotion fires immediately, ensuring
 * the parallel code paths are always exercised even for small test inputs.
 */
public class ParallelHashAggregationOperatorTests extends HashAggregationOperatorTests {

    private static final String ESQL_WORKER = "esql_worker";

    private TestThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(
                Settings.EMPTY,
                ESQL_WORKER,
                between(1, 4),
                1024,
                "esql_worker",
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    private Executor workerExecutor() {
        return threadPool.executor(ESQL_WORKER);
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return matchesPattern(
            "ParallelHashAggregationOperator\\[workers=\\d+, inner=HashAggregationOperator\\[mode = <not-needed>, aggs = sum of longs, max of longs\\]\\]"
        );
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return matchesPattern("ParallelHashAggregationOperator\\[workers=\\d+, state=PROBING\\]");
    }

    @Override
    protected Operator.OperatorFactory simpleWithMode(SimpleOptions options, AggregatorMode mode) {
        Operator.OperatorFactory inner = super.simpleWithMode(options, mode);
        if (mode.isOutputPartial()) {
            // INITIAL and INTERMEDIATE modes: PHAO only runs in FINAL/SINGLE mode.
            return inner;
        }
        List<BlockHash.GroupSpec> groupSpecs = List.of(new BlockHash.GroupSpec(0, ElementType.LONG));
        return new ParallelHashAggregationOperator.Factory(
            (HashAggregationOperator.Factory) inner,
            groupSpecs,
            between(2, 4),
            0, // threshold 0 → always promote to PARALLEL
            workerExecutor()
        );
    }
}
