/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.embedding;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperatorTestCase;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class EmbeddingOperatorTests extends InferenceOperatorTestCase<DenseEmbeddingResults<?>> {
    private static final String SIMPLE_INFERENCE_ID = "test_embedding";
    private static final int EMBEDDING_DIM = 384;

    private int inputChannel;

    @Before
    public void initEmbeddingChannels() {
        inputChannel = between(0, inputsCount - 1);
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new EmbeddingOperator.Factory(
            mockedInferenceService(),
            SIMPLE_INFERENCE_ID,
            evaluatorFactory(inputChannel),
            DataType.TEXT,
            DataFormat.TEXT,
            InferenceAction.Request.DEFAULT_TIMEOUT
        );
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertThat(results, hasSize(input.size()));

        for (int curPage = 0; curPage < input.size(); curPage++) {
            Page inputPage = input.get(curPage);
            Page resultPage = results.get(curPage);

            assertEquals(inputPage.getPositionCount(), resultPage.getPositionCount());
            assertEquals(inputPage.getBlockCount() + 1, resultPage.getBlockCount());

            for (int channel = 0; channel < inputPage.getBlockCount(); channel++) {
                Block inputBlock = inputPage.getBlock(channel);
                Block resultBlock = resultPage.getBlock(channel);
                assertBlockContentEquals(inputBlock, resultBlock);
            }

            assertEmbeddingResults(inputPage, resultPage);
        }
    }

    private void assertEmbeddingResults(Page inputPage, Page resultPage) {
        BytesRefBlock inputBlock = resultPage.getBlock(inputChannel);
        FloatBlock resultBlock = resultPage.getBlock(inputPage.getBlockCount());

        for (int curPos = 0; curPos < inputPage.getPositionCount(); curPos++) {
            if (inputBlock.isNull(curPos)) {
                assertThat(resultBlock.isNull(curPos), equalTo(true));
            } else {
                assertFalse(resultBlock.isNull(curPos));
                int valueCount = resultBlock.getValueCount(curPos);
                assertThat(valueCount, equalTo(EMBEDDING_DIM));

                int firstValueIndex = resultBlock.getFirstValueIndex(curPos);
                for (int i = 0; i < valueCount; i++) {
                    float component = resultBlock.getFloat(firstValueIndex + i);
                    assertFalse(Float.isNaN(component));
                    assertFalse(Float.isInfinite(component));
                }
            }
        }
    }

    @Override
    protected DenseEmbeddingResults<?> mockInferenceResult(InferenceAction.Request request) {
        List<DenseEmbeddingFloatResults.Embedding> embeddings = new ArrayList<>();
        for (String input : request.getInput()) {
            float[] vector = new float[EMBEDDING_DIM];
            int hash = input.hashCode();
            for (int i = 0; i < EMBEDDING_DIM; i++) {
                vector[i] = (float) Math.sin(hash + i) * 0.1f;
            }
            embeddings.add(new DenseEmbeddingFloatResults.Embedding(vector));
        }
        return new DenseEmbeddingFloatResults(embeddings);
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return expectedToStringOfSimple();
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("EmbeddingOperator[inference_id=[" + SIMPLE_INFERENCE_ID + "]]");
    }

    public void testInferenceFailure() {
        AtomicBoolean shouldFail = new AtomicBoolean(true);
        Exception expectedException = new ElasticsearchException("Inference service unavailable");
        InferenceService failingService = mockedInferenceService(shouldFail, expectedException);

        Operator.OperatorFactory factory = new EmbeddingOperator.Factory(
            failingService,
            SIMPLE_INFERENCE_ID,
            evaluatorFactory(inputChannel),
            DataType.TEXT,
            DataFormat.TEXT,
            InferenceAction.Request.DEFAULT_TIMEOUT
        );

        var runner = new TestDriverRunner().builder(driverContext());
        runner.input(simpleInput(runner.context().blockFactory(), between(1, 100)));
        Exception actualException = expectThrows(ElasticsearchException.class, () -> runner.run(factory));
        assertThat(actualException.getMessage(), equalTo("Inference service unavailable"));
    }

    public void testImageEmbeddingOperator() {
        Operator.OperatorFactory factory = new EmbeddingOperator.Factory(
            mockedInferenceService(),
            SIMPLE_INFERENCE_ID,
            evaluatorFactory(inputChannel),
            DataType.IMAGE,
            DataFormat.BASE64,
            InferenceAction.Request.DEFAULT_TIMEOUT
        );

        var runner = new TestDriverRunner().builder(driverContext());
        runner.input(simpleInput(runner.context().blockFactory(), between(1, 100)));
        runner.run(factory);
    }
}
