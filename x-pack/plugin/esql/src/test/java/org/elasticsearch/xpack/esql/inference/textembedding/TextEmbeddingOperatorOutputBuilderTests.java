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
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestItem;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceResponse;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class TextEmbeddingOperatorOutputBuilderTests extends ComputeTestCase {

    public void testBuildSmallOutputWithFloatEmbeddings() throws Exception {
        assertBuildOutputWithFloatEmbeddings(between(1, 100));
    }

    public void testBuildLargeOutputWithFloatEmbeddings() throws Exception {
        assertBuildOutputWithFloatEmbeddings(between(1_000, 10_000));
    }

    public void testBuildSmallOutputWithByteEmbeddings() throws Exception {
        assertBuildOutputWithByteEmbeddings(between(1, 100));
    }

    public void testBuildLargeOutputWithByteEmbeddings() throws Exception {
        assertBuildOutputWithByteEmbeddings(between(1_000, 10_000));
    }

    public void testHandleEmptyEmbeddings() throws Exception {
        final int size = between(5, 50);
        final Page inputPage = randomInputPage(size, between(1, 3));

        try (
            TextEmbeddingOperatorOutputBuilder outputBuilder = new TextEmbeddingOperatorOutputBuilder(
                blockFactory().newFloatBlockBuilder(size),
                inputPage
            )
        ) {
            // Add responses with empty embeddings
            for (int currentPos = 0; currentPos < inputPage.getPositionCount(); currentPos++) {
                outputBuilder.addInferenceResponse(createEmptyFloatEmbeddingResponse());
            }

            final Page outputPage = outputBuilder.buildOutput();
            FloatBlock outputBlock = (FloatBlock) outputPage.getBlock(outputPage.getBlockCount() - 1);

            // All positions should be null due to empty embeddings
            for (int pos = 0; pos < outputBlock.getPositionCount(); pos++) {
                assertThat(outputBlock.isNull(pos), equalTo(true));
            }

            outputPage.releaseBlocks();
        }

        allBreakersEmpty();
    }

    private void assertBuildOutputWithFloatEmbeddings(int size) throws Exception {
        final Page inputPage = randomInputPage(size, between(1, 10));
        final int embeddingDim = randomIntBetween(50, 1536); // Common embedding dimensions
        final float[][] expectedEmbeddings = new float[size][];

        try (
            TextEmbeddingOperatorOutputBuilder outputBuilder = new TextEmbeddingOperatorOutputBuilder(
                blockFactory().newFloatBlockBuilder(size),
                inputPage
            )
        ) {
            for (int currentPos = 0; currentPos < inputPage.getPositionCount(); currentPos++) {
                float[] embedding = randomFloatEmbedding(embeddingDim);
                expectedEmbeddings[currentPos] = embedding;
                outputBuilder.addInferenceResponse(createFloatEmbeddingResponse(embedding));
            }

            final Page outputPage = outputBuilder.buildOutput();
            assertThat(outputPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            assertThat(outputPage.getBlockCount(), equalTo(inputPage.getBlockCount() + 1));

            assertFloatEmbeddingContent((FloatBlock) outputPage.getBlock(outputPage.getBlockCount() - 1), expectedEmbeddings);

            outputPage.releaseBlocks();
        }

        allBreakersEmpty();
    }

    private void assertBuildOutputWithByteEmbeddings(int size) throws Exception {
        final Page inputPage = randomInputPage(size, between(1, 10));
        final int embeddingDim = randomIntBetween(50, 1536);
        final byte[][] expectedByteEmbeddings = new byte[size][];

        try (
            TextEmbeddingOperatorOutputBuilder outputBuilder = new TextEmbeddingOperatorOutputBuilder(
                blockFactory().newFloatBlockBuilder(size),
                inputPage
            )
        ) {
            for (int currentPos = 0; currentPos < inputPage.getPositionCount(); currentPos++) {
                byte[] embedding = randomByteEmbedding(embeddingDim);
                expectedByteEmbeddings[currentPos] = embedding;
                outputBuilder.addInferenceResponse(createByteEmbeddingResponse(embedding));
            }

            final Page outputPage = outputBuilder.buildOutput();
            assertThat(outputPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            assertThat(outputPage.getBlockCount(), equalTo(inputPage.getBlockCount() + 1));

            assertByteEmbeddingContent((FloatBlock) outputPage.getBlock(outputPage.getBlockCount() - 1), expectedByteEmbeddings);

            outputPage.releaseBlocks();
        }

        allBreakersEmpty();
    }

    private void assertFloatEmbeddingContent(FloatBlock block, float[][] expectedEmbeddings) {
        for (int currentPos = 0; currentPos < block.getPositionCount(); currentPos++) {
            assertThat(block.isNull(currentPos), equalTo(false));
            assertThat(block.getValueCount(currentPos), equalTo(expectedEmbeddings[currentPos].length));

            int firstValueIndex = block.getFirstValueIndex(currentPos);
            for (int i = 0; i < expectedEmbeddings[currentPos].length; i++) {
                float actualValue = block.getFloat(firstValueIndex + i);
                float expectedValue = expectedEmbeddings[currentPos][i];
                assertThat(actualValue, equalTo(expectedValue));
            }
        }
    }

    private void assertByteEmbeddingContent(FloatBlock block, byte[][] expectedByteEmbeddings) {
        for (int currentPos = 0; currentPos < block.getPositionCount(); currentPos++) {
            assertThat(block.isNull(currentPos), equalTo(false));
            assertThat(block.getValueCount(currentPos), equalTo(expectedByteEmbeddings[currentPos].length));

            int firstValueIndex = block.getFirstValueIndex(currentPos);
            for (int i = 0; i < expectedByteEmbeddings[currentPos].length; i++) {
                float actualValue = block.getFloat(firstValueIndex + i);
                // Convert byte to float the same way as DenseEmbeddingByteResults.Embedding.toFloatArray()
                float expectedValue = expectedByteEmbeddings[currentPos][i];
                assertThat(actualValue, equalTo(expectedValue));
            }
        }
    }

    private float[] randomFloatEmbedding(int dimension) {
        float[] embedding = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            embedding[i] = randomFloat();
        }
        return embedding;
    }

    private byte[] randomByteEmbedding(int dimension) {
        byte[] embedding = new byte[dimension];
        for (int i = 0; i < dimension; i++) {
            embedding[i] = randomByte();
        }
        return embedding;
    }

    private static BulkInferenceResponse createFloatEmbeddingResponse(float[] embedding) {
        var embeddingResult = new DenseEmbeddingFloatResults.Embedding(embedding);
        var denseEmbeddingResults = new DenseEmbeddingFloatResults(List.of(embeddingResult));
        return createBulkInferenceResponse(new InferenceAction.Response(denseEmbeddingResults), new int[] { 1 });
    }

    private static BulkInferenceResponse createByteEmbeddingResponse(byte[] embedding) {
        var embeddingResult = new DenseEmbeddingByteResults.Embedding(embedding);
        var denseEmbeddingResults = new DenseEmbeddingByteResults(List.of(embeddingResult));
        return createBulkInferenceResponse(new InferenceAction.Response(denseEmbeddingResults), new int[] { 1 });
    }

    private static BulkInferenceResponse createEmptyFloatEmbeddingResponse() {
        return createBulkInferenceResponse(null, new int[] { 0 });
    }

    private static BulkInferenceResponse createBulkInferenceResponse(InferenceAction.Response inferenceResponse, int[] shape) {
        return new BulkInferenceResponse(new BulkInferenceRequestItem(null, shape), inferenceResponse);
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
