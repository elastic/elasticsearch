/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.textembedding;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.inference.AbstractDenseEmbeddingOperatorTestCase;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.matchesRegex;

public class TextEmbeddingOperatorTests extends AbstractDenseEmbeddingOperatorTestCase {

    @Override
    protected Operator.OperatorFactory createOperatorFactory(InferenceService inferenceService) {
        return new TextEmbeddingOperator.Factory(
            inferenceService,
            SIMPLE_INFERENCE_ID,
            evaluatorFactory(inputChannel),
            null,
            Source.EMPTY,
            false
        );
    }

    private Operator.OperatorFactory createTolerantOperatorFactory(InferenceService inferenceService) {
        return new TextEmbeddingOperator.Factory(
            inferenceService,
            SIMPLE_INFERENCE_ID,
            evaluatorFactory(inputChannel),
            null,
            Source.EMPTY,
            true
        );
    }

    /**
     * When failures are tolerated, an inference error does not fail the query: a warning is emitted, the affected rows'
     * embeddings become null, and processing continues.
     */
    public void testToleratedInferenceFailureProducesNullAndWarns() {
        AtomicBoolean shouldFail = new AtomicBoolean(true);
        Exception failure = new ElasticsearchException("Inference service unavailable");
        InferenceService failingService = mockedInferenceService(shouldFail, failure);

        DriverContext driverContext = driverContext();
        int inputSize = between(1, 100);
        var runner = new TestDriverRunner().builder(driverContext);
        runner.input(simpleInput(runner.context().blockFactory(), inputSize));

        List<Page> results = runner.run(createTolerantOperatorFactory(failingService));
        try {
            // The query completes instead of throwing, and every embedding output value is null.
            for (Page resultPage : results) {
                FloatBlock embeddings = resultPage.getBlock(resultPage.getBlockCount() - 1);
                for (int pos = 0; pos < resultPage.getPositionCount(); pos++) {
                    assertThat(embeddings.isNull(pos), equalTo(true));
                }
            }

            // Every failure here carries the same exception and source location, so the driver's warning set collapses them
            // into exactly two entries regardless of how many rows failed: the "only first N recorded" header, and one line
            // for the exception itself.
            List<String> warnings = collectWarnings(driverContext);
            assertThat(warnings, hasSize(2));
            assertThat(warnings, hasItem(matchesRegex(".*evaluation of \\[.*\\] failed, treating result as null.*")));
            assertThat(warnings, hasItem(containsString("Inference service unavailable")));
        } finally {
            results.forEach(Page::releaseBlocks);
        }
    }

    /**
     * Tolerating failures does not extend to failures that mean the whole query is in trouble. A cancellation must still fail
     * the query rather than being turned into a page of nulls, which would look to the caller like a completed result.
     */
    public void testFatalInferenceFailureIsNotTolerated() {
        List<Exception> fatalFailures = List.of(
            new TaskCancelledException("task cancelled"),
            new CircuitBreakingException("circuit breaking", CircuitBreaker.Durability.TRANSIENT)
        );

        for (Exception fatal : fatalFailures) {
            InferenceService failingService = mockedInferenceService(new AtomicBoolean(true), fatal);

            var runner = new TestDriverRunner().builder(driverContext());
            runner.input(simpleInput(runner.context().blockFactory(), between(1, 100)));

            Exception actual = expectThrows(Exception.class, () -> runner.run(createTolerantOperatorFactory(failingService)));
            assertThat(actual.getMessage(), containsString(fatal.getMessage()));
        }
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("TextEmbeddingOperator[inference_id=[" + SIMPLE_INFERENCE_ID + "]]");
    }
}
