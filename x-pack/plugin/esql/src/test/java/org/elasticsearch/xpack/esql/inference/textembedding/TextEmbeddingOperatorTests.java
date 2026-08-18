/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.textembedding;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.inference.AbstractDenseEmbeddingOperatorTestCase;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
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

            // At least one warning is emitted for the swallowed failure.
            assertThat(collectWarnings(driverContext), hasItem(matchesRegex(".*evaluation of \\[.*\\] failed, treating result as null.*")));
        } finally {
            results.forEach(Page::releaseBlocks);
        }
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("TextEmbeddingOperator[inference_id=[" + SIMPLE_INFERENCE_ID + "]]");
    }
}
