/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.textembedding;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceResponseItem;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class TextEmbeddingOutputBuilderTests extends ComputeTestCase {
    private static final int EMBEDDING_DIM = 384; // Common embedding dimension

    public void testBuildSmallOutput() throws Exception {
        assertBuildOutput(between(1, 100));
    }

    public void testBuildLargeOutput() throws Exception {
        assertBuildOutput(between(1_000, 10_000));
    }

    public void testBuildOutputWithNulls() throws Exception {
        final int size = between(10, 100);
        final Page inputPage = randomInputPage(size, between(1, 20));

        TextEmbeddingOutputBuilder outputBuilder = new TextEmbeddingOutputBuilder(blockFactory());
        List<BulkInferenceResponseItem> responses = new ArrayList<>();

        for (int currentPos = 0; currentPos < inputPage.getPositionCount(); currentPos++) {
            // Mix null and non-null responses
            if (randomBoolean()) {
                // Null response with shape [0]
                responses.add(new BulkInferenceResponseItem(null, new int[] { 0 }, currentPos));
            } else {
                // Regular response with shape [1]
                DenseEmbeddingResults<?> results = createFloatEmbedding(1);
                InferenceAction.Response response = new InferenceAction.Response(results);
                responses.add(new BulkInferenceResponseItem(response, new int[] { 1 }, currentPos));
            }
        }

        try (Page outputPage = outputBuilder.buildOutputPage(inputPage, responses)) {
            assertThat(outputPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            assertThat(outputPage.getBlockCount(), equalTo(inputPage.getBlockCount() + 1));

            // Verify output content matches expected pattern
            FloatBlock outputBlock = outputPage.getBlock(outputPage.getBlockCount() - 1);
            for (int pos = 0; pos < responses.size(); pos++) {
                if (responses.get(pos).inferenceResponse() == null || responses.get(pos).shape()[0] == 0) {
                    assertTrue(outputBlock.isNull(pos));
                } else {
                    assertFalse(outputBlock.isNull(pos));
                    assertThat(outputBlock.getValueCount(pos), equalTo(EMBEDDING_DIM));
                }
            }
        }

        allBreakersEmpty();
    }

    public void testBuildOutputWithByteEmbeddings() throws Exception {
        final int size = between(10, 100);
        final Page inputPage = randomInputPage(size, 2);

        TextEmbeddingOutputBuilder outputBuilder = new TextEmbeddingOutputBuilder(blockFactory());
        List<BulkInferenceResponseItem> responses = new ArrayList<>();

        for (int currentPos = 0; currentPos < inputPage.getPositionCount(); currentPos++) {
            // Create byte embedding instead of float
            DenseEmbeddingResults<?> results = createByteEmbedding(1);
            InferenceAction.Response response = new InferenceAction.Response(results);
            responses.add(new BulkInferenceResponseItem(response, new int[] { 1 }, currentPos));
        }

        try (Page outputPage = outputBuilder.buildOutputPage(inputPage, responses)) {
            assertThat(outputPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            assertThat(outputPage.getBlockCount(), equalTo(inputPage.getBlockCount() + 1));

            // Verify byte embeddings were converted to floats
            FloatBlock outputBlock = outputPage.getBlock(outputPage.getBlockCount() - 1);
            for (int pos = 0; pos < responses.size(); pos++) {
                assertFalse(outputBlock.isNull(pos));
                assertThat(outputBlock.getValueCount(pos), equalTo(EMBEDDING_DIM));

                // Verify all values are valid floats (byte embeddings converted)
                int firstValueIndex = outputBlock.getFirstValueIndex(pos);
                for (int i = 0; i < EMBEDDING_DIM; i++) {
                    float value = outputBlock.getFloat(firstValueIndex + i);
                    assertFalse(Float.isNaN(value));
                    assertFalse(Float.isInfinite(value));
                }
            }
        }

        allBreakersEmpty();
    }

    public void testBuildOutputWithEmptyEmbeddings() throws Exception {
        final int size = 5;
        final Page inputPage = randomInputPage(size, 2);

        TextEmbeddingOutputBuilder outputBuilder = new TextEmbeddingOutputBuilder(blockFactory());
        List<BulkInferenceResponseItem> responses = new ArrayList<>();

        // Create a single response that covers all 5 positions with all nulls
        int[] shape = new int[size];
        responses.add(new BulkInferenceResponseItem(null, shape, 0));

        try (Page outputPage = outputBuilder.buildOutputPage(inputPage, responses)) {
            assertThat(outputPage.getPositionCount(), equalTo(inputPage.getPositionCount()));

            // All should be null
            FloatBlock outputBlock = outputPage.getBlock(outputPage.getBlockCount() - 1);
            for (int pos = 0; pos < size; pos++) {
                assertTrue(outputBlock.isNull(pos));
            }
        }

        allBreakersEmpty();
    }

    public void testBuildOutputVerifyEmbeddingValues() throws Exception {
        final int size = 10;
        final Page inputPage = randomInputPage(size, 1);

        TextEmbeddingOutputBuilder outputBuilder = new TextEmbeddingOutputBuilder(blockFactory());
        List<BulkInferenceResponseItem> responses = new ArrayList<>();
        List<float[]> expectedEmbeddings = new ArrayList<>();

        for (int currentPos = 0; currentPos < inputPage.getPositionCount(); currentPos++) {
            // Create embedding with known values
            float[] embedding = new float[EMBEDDING_DIM];
            for (int i = 0; i < EMBEDDING_DIM; i++) {
                embedding[i] = currentPos + i * 0.1f;
            }
            expectedEmbeddings.add(embedding);

            DenseEmbeddingFloatResults.Embedding embeddingObj = new DenseEmbeddingFloatResults.Embedding(embedding);
            DenseEmbeddingFloatResults results = new DenseEmbeddingFloatResults(List.of(embeddingObj));
            InferenceAction.Response response = new InferenceAction.Response(results);
            responses.add(new BulkInferenceResponseItem(response, new int[] { 1 }, currentPos));
        }

        try (Page outputPage = outputBuilder.buildOutputPage(inputPage, responses)) {
            FloatBlock outputBlock = outputPage.getBlock(outputPage.getBlockCount() - 1);

            // Verify embedding values match
            for (int pos = 0; pos < responses.size(); pos++) {
                assertFalse(outputBlock.isNull(pos));
                int firstValueIndex = outputBlock.getFirstValueIndex(pos);
                float[] expected = expectedEmbeddings.get(pos);

                for (int i = 0; i < EMBEDDING_DIM; i++) {
                    float actual = outputBlock.getFloat(firstValueIndex + i);
                    assertThat(actual, equalTo(expected[i]));
                }
            }
        }

        allBreakersEmpty();
    }

    private void assertBuildOutput(int size) throws Exception {
        final Page inputPage = randomInputPage(size, between(1, 20));

        TextEmbeddingOutputBuilder outputBuilder = new TextEmbeddingOutputBuilder(blockFactory());
        List<BulkInferenceResponseItem> responses = new ArrayList<>();

        for (int currentPos = 0; currentPos < inputPage.getPositionCount(); currentPos++) {
            DenseEmbeddingResults<?> results = createFloatEmbedding(1);
            InferenceAction.Response response = new InferenceAction.Response(results);
            responses.add(new BulkInferenceResponseItem(response, new int[] { 1 }, currentPos));
        }

        try (Page outputPage = outputBuilder.buildOutputPage(inputPage, responses)) {
            assertThat(outputPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            assertThat(outputPage.getBlockCount(), equalTo(inputPage.getBlockCount() + 1));
            assertOutputContent(outputPage.getBlock(outputPage.getBlockCount() - 1));
        }

        allBreakersEmpty();
    }

    private void assertOutputContent(FloatBlock block) {
        for (int currentPos = 0; currentPos < block.getPositionCount(); currentPos++) {
            assertThat(block.isNull(currentPos), equalTo(false));
            assertThat(block.getValueCount(currentPos), equalTo(EMBEDDING_DIM));

            // Verify all components are valid floats
            int firstValueIndex = block.getFirstValueIndex(currentPos);
            for (int i = 0; i < EMBEDDING_DIM; i++) {
                float value = block.getFloat(firstValueIndex + i);
                assertFalse(Float.isNaN(value));
                assertFalse(Float.isInfinite(value));
            }
        }
    }

    private DenseEmbeddingFloatResults createFloatEmbedding(int count) {
        List<DenseEmbeddingFloatResults.Embedding> embeddings = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            float[] vector = new float[EMBEDDING_DIM];
            for (int j = 0; j < EMBEDDING_DIM; j++) {
                vector[j] = randomFloat();
            }
            embeddings.add(new DenseEmbeddingFloatResults.Embedding(vector));
        }
        return new DenseEmbeddingFloatResults(embeddings);
    }

    private DenseEmbeddingByteResults createByteEmbedding(int count) {
        List<DenseEmbeddingByteResults.Embedding> embeddings = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            byte[] vector = new byte[EMBEDDING_DIM];
            for (int j = 0; j < EMBEDDING_DIM; j++) {
                vector[j] = randomByte();
            }
            embeddings.add(new DenseEmbeddingByteResults.Embedding(vector));
        }
        return new DenseEmbeddingByteResults(embeddings);
    }

    private Page randomInputPage(int positionCount, int columnCount) {
        final Block[] blocks = new Block[columnCount];
        try {
            for (int i = 0; i < columnCount; i++) {
                blocks[i] = RandomBlock.randomBlock(
                    blockFactory(),
                    RandomBlock.randomElementExcluding(List.of(ElementType.AGGREGATE_METRIC_DOUBLE)),
                    positionCount,
                    randomBoolean(),
                    0,
                    0,
                    randomInt(10),
                    randomInt(10)
                ).block();
            }

            return new Page(blocks);
        } catch (Exception e) {
            Releasables.close(blocks);
            throw (e);
        }
    }
}
