/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingResults;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.matchesRegex;

public abstract class AbstractDenseEmbeddingOperatorTestCase extends InferenceOperatorTestCase<DenseEmbeddingResults<?>> {
    protected static final String SIMPLE_INFERENCE_ID = "test_embedding";
    protected static final int EMBEDDING_DIM = 384;

    protected int inputChannel;

    @Before
    public void initInputChannel() {
        inputChannel = between(0, inputsCount - 1);
    }

    /** Builds the operator factory under test with an explicit batch size and failure-tolerance flag, shared by the tests below. */
    protected abstract Operator.OperatorFactory createOperatorFactory(
        InferenceService inferenceService,
        int batchSize,
        boolean tolerateFailures
    );

    protected final Operator.OperatorFactory createOperatorFactory(InferenceService inferenceService) {
        return createOperatorFactory(inferenceService, InferenceSettings.DENSE_VECTOR_DEFAULT_BATCH_SIZE, false);
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return createOperatorFactory(mockedInferenceService());
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
            embeddings.add(new DenseEmbeddingFloatResults.Embedding(expectedEmbedding(input)));
        }
        return new DenseEmbeddingFloatResults(embeddings);
    }

    /** The deterministic embedding vector the mock produces for a given input; lets alignment tests match each row to its input. */
    protected static float[] expectedEmbedding(String input) {
        float[] vector = new float[EMBEDDING_DIM];
        int hash = input.hashCode();
        for (int i = 0; i < EMBEDDING_DIM; i++) {
            vector[i] = (float) Math.sin(hash + i) * 0.1f;
        }
        return vector;
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return expectedToStringOfSimple();
    }

    public void testInferenceFailure() {
        AtomicBoolean shouldFail = new AtomicBoolean(true);
        Exception expectedException = new ElasticsearchException("Inference service unavailable");
        InferenceService failingService = mockedInferenceService(shouldFail, expectedException);

        Operator.OperatorFactory factory = createOperatorFactory(failingService);

        var runner = new TestDriverRunner().builder(driverContext());
        runner.input(simpleInput(runner.context().blockFactory(), between(1, 100)));
        Exception actualException = expectThrows(ElasticsearchException.class, () -> runner.run(factory));
        assertThat(actualException.getMessage(), equalTo("Inference service unavailable"));
    }

    /**
     * A page whose input column is entirely null has nothing to embed, so no inference request is issued and the query completes
     * with null embeddings. The inference service here fails every request it receives and the operator does not tolerate
     * failures, so the query completing at all is what establishes that no request was issued.
     */
    public void testAllNullInputIssuesNoInferenceRequest() {
        InferenceService failingService = mockedInferenceService(
            new AtomicBoolean(true),
            new ElasticsearchException("Inference service unavailable")
        );

        int inputSize = between(1, 100);
        var runner = new TestDriverRunner().builder(driverContext());
        runner.input(allNullInput(runner.context().blockFactory(), inputSize));

        List<Page> results = runner.run(createOperatorFactory(failingService));
        try {
            for (Page resultPage : results) {
                FloatBlock embeddings = resultPage.getBlock(resultPage.getBlockCount() - 1);
                for (int pos = 0; pos < resultPage.getPositionCount(); pos++) {
                    assertThat(embeddings.isNull(pos), equalTo(true));
                }
            }
        } finally {
            results.forEach(Page::releaseBlocks);
        }
    }

    private Page allNullInput(BlockFactory blockFactory, int size) {
        Block[] blocks = new Block[inputsCount];
        try {
            for (int b = 0; b < inputsCount; b++) {
                try (var builder = blockFactory.newBytesRefBlockBuilder(size)) {
                    for (int i = 0; i < size; i++) {
                        builder.appendNull();
                    }
                    blocks[b] = builder.build();
                }
            }
        } catch (Exception e) {
            Releasables.closeExpectNoException(blocks);
            throw e;
        }
        return new Page(blocks);
    }

    /**
     * Per-row alignment across batches: distinct inputs spanning more than one batch (with interior and trailing nulls) must each
     * receive the embedding of that row's own input, and null rows stay null. Catches within-batch misassignment/shift.
     */
    public void testBatchedPerRowEmbeddingAlignment() {
        int batchSize = 4;
        // 10 positions, 8 non-null, with an interior null (index 2) and a trailing null (index 9), spanning two batches.
        List<String> texts = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            if (i == 2 || i == 9) {
                texts.add(null);
            } else {
                texts.add("row_" + i + "_" + randomAlphaOfLength(8));
            }
        }

        DriverContext driverContext = driverContext();
        var runner = new TestDriverRunner().builder(driverContext);
        runner.input(controlledInput(runner.context().blockFactory(), texts));

        List<Page> results = runner.run(createOperatorFactory(mockedInferenceService(), batchSize, false));
        try {
            assertPerRowEmbeddings(results, texts);
        } finally {
            results.forEach(Page::releaseBlocks);
        }
    }

    /**
     * A failed request nulls every row of its batch (with a warning), while a sibling batch that succeeded keeps its real
     * per-row vectors.
     */
    public void testBatchedFailureCrossBatchIsolation() {
        int batchSize = 4;
        // Two full batches of 4 non-null rows each. The sentinel sits in the second batch (index 5), so only that request fails.
        List<String> texts = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            if (i == 5) {
                texts.add("FAIL_sentinel");
            } else {
                texts.add("row_" + i + "_" + randomAlphaOfLength(8));
            }
        }

        InferenceService failingService = mockedInferenceService(
            inputs -> inputs.stream().anyMatch(s -> s.startsWith("FAIL")),
            new ElasticsearchException("Inference service unavailable"),
            null
        );

        DriverContext driverContext = driverContext();
        var runner = new TestDriverRunner().builder(driverContext);
        runner.input(controlledInput(runner.context().blockFactory(), texts));

        List<Page> results = runner.run(createOperatorFactory(failingService, batchSize, true));
        try {
            // First batch (indices 0-3) keeps real vectors; second batch (indices 4-7) is entirely null.
            List<String> expected = new ArrayList<>(texts);
            for (int i = 4; i < 8; i++) {
                expected.set(i, null);
            }
            assertPerRowEmbeddings(results, expected);
            assertThat(collectWarnings(driverContext), hasItem(matchesRegex(".*evaluation of \\[.*\\] failed, treating result as null.*")));
        } finally {
            results.forEach(Page::releaseBlocks);
        }
    }

    /**
     * A failing batch that mixes a null-input row with text rows. The whole request fails as a unit, so every text row it carried
     * is nulled, while the null-input row stays null because it never had a value. A sibling batch that succeeds keeps its real
     * vectors. This pins that failure nulling is per batch, not per row, and that it leaves a null input as a null.
     */
    public void testBatchedFailureWithNullRowInFailingBatch() {
        int batchSize = 4;
        // Batch 1 (indices 0-3): four text rows, no sentinel, so this request succeeds.
        // Batch 2 (indices 4-7): text, null, sentinel, text. Index 4 is non-null so batch 1 does not absorb it as a trailing null,
        // and index 5 is an interior null carried by the failing request.
        List<String> texts = new ArrayList<>();
        texts.add("row_0_" + randomAlphaOfLength(8));
        texts.add("row_1_" + randomAlphaOfLength(8));
        texts.add("row_2_" + randomAlphaOfLength(8));
        texts.add("row_3_" + randomAlphaOfLength(8));
        texts.add("row_4_" + randomAlphaOfLength(8));
        texts.add(null);
        texts.add("FAIL_sentinel");
        texts.add("row_7_" + randomAlphaOfLength(8));

        InferenceService failingService = mockedInferenceService(
            inputs -> inputs.stream().anyMatch(s -> s.startsWith("FAIL")),
            new ElasticsearchException("Inference service unavailable"),
            null
        );

        DriverContext driverContext = driverContext();
        var runner = new TestDriverRunner().builder(driverContext);
        runner.input(controlledInput(runner.context().blockFactory(), texts));

        List<Page> results = runner.run(createOperatorFactory(failingService, batchSize, true));
        try {
            // Batch 1 keeps its real vectors; every position of the failing batch 2 is null, whether it held text or was already null.
            List<String> expected = new ArrayList<>(texts);
            for (int i = 4; i < 8; i++) {
                expected.set(i, null);
            }
            assertPerRowEmbeddings(results, expected);
            assertThat(collectWarnings(driverContext), hasItem(matchesRegex(".*evaluation of \\[.*\\] failed, treating result as null.*")));
        } finally {
            results.forEach(Page::releaseBlocks);
        }
    }

    /**
     * Batching actually reduces the number of inference requests: 25 non-null rows with batch size 10 must reach the service as
     * exactly 3 requests.
     */
    public void testBatchingReducesRequestCount() {
        int batchSize = 10;
        int rows = 25;
        List<String> texts = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            texts.add("row_" + i + "_" + randomAlphaOfLength(8));
        }

        AtomicInteger invocationCounter = new AtomicInteger(0);
        InferenceService countingService = mockedInferenceService(inputs -> false, new ElasticsearchException("unused"), invocationCounter);

        DriverContext driverContext = driverContext();
        var runner = new TestDriverRunner().builder(driverContext);
        runner.input(controlledInput(runner.context().blockFactory(), texts));

        List<Page> results = runner.run(createOperatorFactory(countingService, batchSize, false));
        try {
            assertThat(invocationCounter.get(), equalTo(3));
            assertPerRowEmbeddings(results, texts);
        } finally {
            results.forEach(Page::releaseBlocks);
        }
    }

    /**
     * Builds a single input page whose {@link #inputChannel} carries the given texts ({@code null} entries become null rows).
     * All input channels carry the same values so the test is agnostic to the randomly chosen {@link #inputChannel}.
     */
    protected Page controlledInput(BlockFactory blockFactory, List<String> texts) {
        Block[] blocks = new Block[inputsCount];
        try {
            for (int b = 0; b < inputsCount; b++) {
                try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(texts.size())) {
                    for (String text : texts) {
                        if (text == null) {
                            builder.appendNull();
                        } else {
                            builder.appendBytesRef(new BytesRef(text));
                        }
                    }
                    blocks[b] = builder.build();
                }
            }
        } catch (Exception e) {
            Releasables.closeExpectNoException(blocks);
            throw e;
        }
        return new Page(blocks);
    }

    /**
     * Asserts that the concatenation of result pages carries, for each position, the embedding of the corresponding
     * {@code expectedTexts} entry, or a null value when that entry is {@code null}.
     */
    private void assertPerRowEmbeddings(List<Page> results, List<String> expectedTexts) {
        int pos = 0;
        for (Page page : results) {
            FloatBlock embeddings = page.getBlock(page.getBlockCount() - 1);
            for (int p = 0; p < page.getPositionCount(); p++, pos++) {
                String expectedText = expectedTexts.get(pos);
                if (expectedText == null) {
                    assertThat("row " + pos + " should be null", embeddings.isNull(p), equalTo(true));
                } else {
                    assertThat("row " + pos + " should not be null", embeddings.isNull(p), equalTo(false));
                    assertThat(embeddings.getValueCount(p), equalTo(EMBEDDING_DIM));
                    float[] expected = expectedEmbedding(expectedText);
                    int firstValueIndex = embeddings.getFirstValueIndex(p);
                    for (int i = 0; i < EMBEDDING_DIM; i++) {
                        assertThat("row " + pos + " component " + i, embeddings.getFloat(firstValueIndex + i), equalTo(expected[i]));
                    }
                }
            }
        }
        assertThat(pos, equalTo(expectedTexts.size()));
    }
}
