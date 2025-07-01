/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.embedding;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingBitResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class DenseEmbeddingOperatorOutputBuilderTests extends ComputeTestCase {

    private final static List<Integer> DIMENSIONS = List.of(1, 32, 128, 512, 2048, 5096);
    private final static List<Integer> INPUT_SIZES = List.of(10, 100, 1_000, 10_000);
    private final static List<Integer> BATCH_SIZES = List.of(1, 10, 100, 1000);
    private final static List<Class<? extends TextEmbeddingResults<?>>> EMBEDDING_TYPES = List.of(
        TextEmbeddingBitResults.class,
        TextEmbeddingByteResults.class,
        TextEmbeddingFloatResults.class
    );

    private final static String TEST_PARAMS_FORMATING = "dims=%d, input_size=%d, batch_size=%d, embedding_type=%s";

    private final int dimensions;
    private final int inputPageSize;
    private final int batchSize;
    private final Class<? extends TextEmbeddingResults<?>> embeddingType;

    @ParametersFactory(argumentFormatting = TEST_PARAMS_FORMATING)
    public static Iterable<Object[]> parameters() {
        List<Object[]> params = new ArrayList<>();
        params.add(new Object[] {});

        for (List<?> axis : List.of(DIMENSIONS, INPUT_SIZES, BATCH_SIZES, EMBEDDING_TYPES)) {

            List<Object[]> newParams = new ArrayList<>();
            for (Object[] combination : params) {
                for (Object element : axis) {
                    Object[] newCombination = new Object[combination.length + 1];
                    System.arraycopy(combination, 0, newCombination, 0, combination.length);
                    newCombination[newCombination.length - 1] = element;
                    newParams.add(newCombination);
                }
            }
            params = newParams;
        }

        return params;
    }

    public DenseEmbeddingOperatorOutputBuilderTests(
        int dimensions,
        int inputPageSize,
        int batchSize,
        Class<? extends TextEmbeddingBitResults> embeddingType
    ) {
        this.dimensions = dimensions;
        this.inputPageSize = inputPageSize;
        this.batchSize = batchSize;
        this.embeddingType = embeddingType;
    }

    public void testOutput() {
        final Page inputPage = randomInputPage(inputPageSize, between(1, 20));
        try (
            DenseEmbeddingOperatorOutputBuilder outputBuilder = new DenseEmbeddingOperatorOutputBuilder(
                blockFactory().newFloatBlockBuilder(inputPageSize * dimensions),
                inputPage,
                dimensions
            )
        ) {
            for (int currentPos = 0; currentPos < inputPage.getPositionCount(); currentPos += batchSize) {
                outputBuilder.addInferenceResponse(
                    DenseEmbeddingUtils.inferenceResponse(
                        currentPos,
                        Math.min(inputPage.getPositionCount() - currentPos, batchSize),
                        embeddingType,
                        dimensions
                    )
                );
            }

            final Page outputPage = outputBuilder.buildOutput();
            try {
                assertThat(outputPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
                assertThat(outputPage.getBlockCount(), equalTo(inputPage.getBlockCount() + 1));
                assertOutputContent(outputPage.getBlock(inputPage.getBlockCount()));
            } finally {
                outputPage.releaseBlocks();
            }

        } finally {
            inputPage.releaseBlocks();
        }
    }

    private void assertOutputContent(FloatBlock block) {

        for (int currentPos = 0; currentPos < block.getPositionCount(); currentPos++) {
            assertThat(block.isNull(currentPos), equalTo(false));
            assertThat(block.getValueCount(currentPos), equalTo(dimensions));
            float[] expectedEmbeddingValues = new float[dimensions];
            if (embeddingType.equals(TextEmbeddingByteResults.class) || embeddingType.equals(TextEmbeddingBitResults.class)) {
                byte[] bytesValues = embeddingType.equals(TextEmbeddingBitResults.class)
                    ? DenseEmbeddingUtils.toBitArray(currentPos, dimensions)
                    : DenseEmbeddingUtils.toByteArray(currentPos, dimensions);
                assertThat(bytesValues.length, equalTo(dimensions));
                for (int i = 0; i < bytesValues.length; i++) {
                    expectedEmbeddingValues[i] = bytesValues[i];
                }
            } else if (embeddingType.equals(TextEmbeddingFloatResults.class)) {
                expectedEmbeddingValues = DenseEmbeddingUtils.toFloatArray(currentPos, dimensions);
            }

            for (int valueIndex = 0; valueIndex < block.getValueCount(currentPos); valueIndex++) {
                assertThat(block.getFloat(block.getFirstValueIndex(currentPos) + valueIndex), equalTo(expectedEmbeddingValues[valueIndex]));
            }
        }
    }

    private Page randomInputPage(int positionCount, int columnCount) {
        final Block[] blocks = new Block[columnCount];
        try {
            for (int i = 0; i < columnCount; i++) {
                blocks[i] = RandomBlock.randomBlock(
                    blockFactory(),
                    RandomBlock.randomElementType(),
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
