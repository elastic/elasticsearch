/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.textembedding;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperatorTestCase;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TextEmbeddingOperatorTests extends InferenceOperatorTestCase<DenseEmbeddingFloatResults> {
    private static final String SIMPLE_INFERENCE_ID = "test_text_embedding";
    private static final int EMBEDDING_DIMENSION = 384; // Common embedding dimension

    private int inputChannel;

    @Before
    public void initTextEmbeddingChannels() {
        inputChannel = between(0, inputsCount - 1);
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new TextEmbeddingOperator.Factory(mockedInferenceService(), SIMPLE_INFERENCE_ID, evaluatorFactory(inputChannel));
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

            assertTextEmbeddingResults(inputPage, resultPage);
        }
    }

    private void assertTextEmbeddingResults(Page inputPage, Page resultPage) {
        BytesRefBlock inputBlock = resultPage.getBlock(inputChannel);
        FloatBlock resultBlock = (FloatBlock) resultPage.getBlock(inputPage.getBlockCount());

        BlockStringReader blockReader = new InferenceOperatorTestCase.BlockStringReader();

        for (int curPos = 0; curPos < inputPage.getPositionCount(); curPos++) {
            if (inputBlock.isNull(curPos)) {
                assertThat(resultBlock.isNull(curPos), equalTo(true));
            } else {
                // Verify that we have an embedding vector at this position
                assertThat(resultBlock.isNull(curPos), equalTo(false));
                assertThat(resultBlock.getValueCount(curPos), equalTo(EMBEDDING_DIMENSION));

                // Get the input text to verify our mock embedding generation
                String inputText = blockReader.readString(inputBlock, curPos);

                // Verify the embedding values match our mock generation pattern
                int firstValueIndex = resultBlock.getFirstValueIndex(curPos);
                for (int i = 0; i < EMBEDDING_DIMENSION; i++) {
                    float expectedValue = generateMockEmbeddingValue(inputText, i);
                    float actualValue = resultBlock.getFloat(firstValueIndex + i);
                    assertThat(actualValue, equalTo(expectedValue));
                }
            }
        }
    }

    @Override
    protected DenseEmbeddingFloatResults mockInferenceResult(InferenceAction.Request request) {
        // For text embedding, we expect one input text per request
        String inputText = request.getInput().get(0);

        // Generate a deterministic mock embedding based on the input text
        float[] mockEmbedding = generateMockEmbedding(inputText, EMBEDDING_DIMENSION);

        var embeddingResult = new DenseEmbeddingFloatResults.Embedding(mockEmbedding);
        return new DenseEmbeddingFloatResults(List.of(embeddingResult));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return expectedToStringOfSimple();
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("TextEmbeddingOperator[inference_id=[" + SIMPLE_INFERENCE_ID + "]]");
    }

    /**
     * Generates a deterministic mock embedding vector based on the input text.
     * This ensures our tests are repeatable and verifiable.
     */
    private float[] generateMockEmbedding(String inputText, int dimension) {
        float[] embedding = new float[dimension];
        int textHash = inputText.hashCode();

        for (int i = 0; i < dimension; i++) {
            embedding[i] = generateMockEmbeddingValue(inputText, i);
        }

        return embedding;
    }

    /**
     * Generates a single embedding value for a specific dimension based on input text.
     * Uses a deterministic function so tests are repeatable.
     */
    private float generateMockEmbeddingValue(String inputText, int dimension) {
        // Create a deterministic value based on input text and dimension
        int hash = (inputText.hashCode() + dimension * 31) % 10000;
        return hash / 10000.0f; // Normalize to [0, 1) range
    }
}
