/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.textembedding;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.EmbeddingResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceResponseItem;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.OutputBuilder;

import java.util.List;
import java.util.stream.IntStream;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * Builds output pages for text embedding inference operations.
 * <p>
 * Converts {@link DenseEmbeddingResults} from inference responses into a {@link FloatBlock} that is appended
 * to the input page. Each embedding vector is stored as a multi-valued position containing the vector components.
 * </p>
 */
class TextEmbeddingOutputBuilder implements OutputBuilder {

    private final BlockFactory blockFactory;

    TextEmbeddingOutputBuilder(BlockFactory blockFactory) {
        this.blockFactory = blockFactory;
    }

    /**
     * Builds the output page by converting inference responses into a {@link FloatBlock}.
     * <p>
     * The shape array in each response determines how output values are distributed across rows:
     * <ul>
     *   <li>shape[i] = 0: produces a null value for row i (no embedding)</li>
     *   <li>shape[i] = 1: produces one embedding vector for row i</li>
     * </ul>
     * Each embedding vector is stored as a multi-valued entry in the output block.
     *
     * @param inputPage The original input page
     * @param responses The ordered list of inference responses corresponding to the input rows
     * @return A new page with the embedding vectors appended as an additional block
     */
    @Override
    public Page buildOutputPage(Page inputPage, List<BulkInferenceResponseItem> responses) {
        int positionCount = inputPage.getPositionCount();
        int dimension = responses.stream().mapToInt(this::dimensionCount).max().orElse(1);
        try (FloatBlock.Builder outputBlockBuilder = blockFactory.newFloatBlockBuilder(positionCount * dimension)) {
            for (BulkInferenceResponseItem response : responses) {
                appendResponseToBlock(outputBlockBuilder, response);
            }

            return inputPage.appendBlock(outputBlockBuilder.build());
        }
    }

    private int dimensionCount(BulkInferenceResponseItem response) {
        if (response == null || response.shape() == null || response.inferenceResponse() == null) {
            return 0;
        }

        if (response.inferenceResponse().getResults() instanceof DenseEmbeddingResults<?> embeddingResults) {
            if (embeddingResults.embeddings().isEmpty()) {
                return 0;
            }

            EmbeddingResults.Embedding<?> firstEmbedding = embeddingResults.embeddings().getFirst();
            return switch (firstEmbedding) {
                case DenseEmbeddingFloatResults.Embedding floatEmbedding -> floatEmbedding.values().length;
                case DenseEmbeddingByteResults.Embedding byteEmbedding -> byteEmbedding.values().length;
                default -> 0;
            };
        }

        return 0;
    }

    /**
     * Appends embeddings from a single inference response to the output block.
     * <p>
     * The response shape array determines how embeddings are distributed:
     * <ul>
     *   <li>For each position with shape value 0: appends a null embedding</li>
     *   <li>For each position with shape value 1: appends the corresponding embedding vector</li>
     * </ul>
     *
     * @param builder  The block builder to append to
     * @param response The inference response item containing the embedding results
     * @throws IllegalStateException if the number of embeddings doesn't match the sum of shape values
     */
    private void appendResponseToBlock(FloatBlock.Builder builder, BulkInferenceResponseItem response) {
        // Handle null responses or null shape
        if (response == null || response.shape() == null) {
            return;
        }

        // Extract embeddings if the response is non-null
        float[][] embeddings = null;
        if (response.inferenceResponse() != null) {
            DenseEmbeddingResults<?> embeddingResults = extractEmbeddingResults(response);
            embeddings = embeddingResults.embeddings()
                .stream()
                .map(TextEmbeddingOutputBuilder::getEmbeddingAsFloatArray)
                .toArray(float[][]::new);
        }

        // Validate that the number of embeddings matches the expected count from the shape
        int expectedEmbeddingCount = IntStream.of(response.shape()).sum();
        if (embeddings != null && embeddings.length != expectedEmbeddingCount) {
            throw new IllegalStateException(
                format(
                    "Mismatch between embedding count and shape: expected {} embeddings but got {}",
                    expectedEmbeddingCount,
                    embeddings.length
                )
            );
        }

        int currentEmbeddingIndex = 0;

        for (int valueCount : response.shape()) {
            if (valueCount == 0) {
                // No embedding for this position, append a null value to the block
                builder.appendNull();
            } else {
                // Append the embedding vector as a multi-valued position
                if (embeddings == null) {
                    throw new IllegalStateException("Expected embeddings but response was null");
                }

                float[] embeddingArray = embeddings[currentEmbeddingIndex++];

                builder.beginPositionEntry();
                for (float component : embeddingArray) {
                    builder.appendFloat(component);
                }
                builder.endPositionEntry();
            }
        }
    }

    /**
     * Extracts {@link DenseEmbeddingResults} from an inference response.
     *
     * @param responseItem The inference response item to extract from
     * @return The dense embedding results
     * @throws IllegalStateException if the response is null, has no results, or is not of type {@link DenseEmbeddingResults}
     */
    private DenseEmbeddingResults<?> extractEmbeddingResults(BulkInferenceResponseItem responseItem) {
        if (responseItem == null || responseItem.inferenceResponse() == null) {
            throw new IllegalStateException("Inference response is null");
        }

        if (responseItem.inferenceResponse().getResults() instanceof DenseEmbeddingResults<?> embeddingResults) {
            if (embeddingResults.embeddings().isEmpty()) {
                throw new IllegalStateException("Embedding results are empty");
            }
            return embeddingResults;
        }

        throw new IllegalStateException(
            format(
                "Inference result has wrong type. Got [{}] while expecting [{}]",
                responseItem.inferenceResponse().getResults().getClass().getName(),
                DenseEmbeddingResults.class.getName()
            )
        );
    }

    /**
     * Extracts the embedding as a float array from an embedding object.
     * <p>
     * Handles both {@link DenseEmbeddingFloatResults.Embedding} and {@link DenseEmbeddingByteResults.Embedding},
     * converting byte embeddings to floats when necessary.
     * </p>
     *
     * @param embedding The embedding object containing the vector
     * @return The embedding vector as a float array
     * @throws IllegalArgumentException if the embedding type is not supported
     */
    private static float[] getEmbeddingAsFloatArray(EmbeddingResults.Embedding<?> embedding) {
        return switch (embedding) {
            case DenseEmbeddingFloatResults.Embedding floatEmbedding -> floatEmbedding.values();
            case DenseEmbeddingByteResults.Embedding byteEmbedding -> toFloatArray(byteEmbedding.values());
            default -> throw new IllegalArgumentException(
                format(
                    "Unsupported embedding type: [{}]. Expected [{}] or [{}].",
                    embedding.getClass().getName(),
                    DenseEmbeddingFloatResults.Embedding.class.getSimpleName(),
                    DenseEmbeddingByteResults.Embedding.class.getSimpleName()
                )
            );
        };
    }

    /**
     * Converts a byte array to a float array.
     *
     * @param values The byte array to convert
     * @return The converted float array
     */
    private static float[] toFloatArray(byte[] values) {
        float[] floatArray = new float[values.length];
        for (int i = 0; i < values.length; i++) {
            floatArray[i] = values[i];
        }
        return floatArray;
    }
}
