/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.textembedding;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperator;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceResponse;

import java.util.List;

/**
 * {@link TextEmbeddingOperatorOutputBuilder} builds the output page for text embedding by converting
 * {@link DenseEmbeddingResults} into a {@link FloatBlock} containing dense vector embeddings.
 */
class TextEmbeddingOperatorOutputBuilder implements InferenceOperator.OutputBuilder {
    private final Page inputPage;
    private final FloatBlock.Builder outputBlockBuilder;

    TextEmbeddingOperatorOutputBuilder(FloatBlock.Builder outputBlockBuilder, Page inputPage) {
        this.inputPage = inputPage;
        this.outputBlockBuilder = outputBlockBuilder;
    }

    @Override
    public void close() {
        Releasables.close(outputBlockBuilder);
    }

    @Override
    public void addInferenceResponse(BulkInferenceResponse bulkInferenceResponse) {
        List<DenseEmbeddingResults.Embedding<?>> embeddings = inferenceResults(bulkInferenceResponse.response());
        int currentIndex = 0;

        for (int valueCount : bulkInferenceResponse.shape()) {
            if (valueCount == 0) {
                outputBlockBuilder.appendNull();
                continue;
            }

            outputBlockBuilder.beginPositionEntry();
            for (int i = 0; i < valueCount; i++) {
                DenseEmbeddingResults.Embedding<?> embedding = embeddings.get(currentIndex++);
                float[] embeddingArray = getEmbeddingAsFloatArray(embedding);
                for (float component : embeddingArray) {
                    outputBlockBuilder.appendFloat(component);
                }
            }
            outputBlockBuilder.endPositionEntry();
        }
    }

    /**
     * Builds the final output page by appending the embedding output block to the input page.
     */
    @Override
    public Page buildOutput() {
        Block outputBlock = outputBlockBuilder.build();
        assert outputBlock.getPositionCount() == inputPage.getPositionCount();
        return inputPage.appendBlock(outputBlock);
    }

    @SuppressWarnings("unchecked")
    private List<DenseEmbeddingResults.Embedding<?>> inferenceResults(InferenceAction.Response inferenceResponse) {
        if (inferenceResponse == null) {
            return List.of();
        }

        return (List<DenseEmbeddingResults.Embedding<?>>) (List<?>) InferenceOperator.OutputBuilder.inferenceResults(
            inferenceResponse,
            DenseEmbeddingResults.class
        ).embeddings();
    }

    /**
     * Extracts the embedding as a float array from the embedding result.
     */
    private static float[] getEmbeddingAsFloatArray(DenseEmbeddingResults.Embedding<?> embedding) {
        return switch (embedding) {
            case DenseEmbeddingFloatResults.Embedding floatEmbedding -> floatEmbedding.values();
            case DenseEmbeddingByteResults.Embedding byteEmbedding -> toFloatArray(byteEmbedding.values());
            default -> throw new IllegalArgumentException(
                "Unsupported embedding type: "
                    + embedding.getClass().getName()
                    + ". Expected "
                    + DenseEmbeddingFloatResults.Embedding.class.getSimpleName()
                    + " or "
                    + DenseEmbeddingByteResults.Embedding.class.getSimpleName()
                    + "."
            );
        };
    }

    private static float[] toFloatArray(byte[] values) {
        float[] floatArray = new float[values.length];
        for (int i = 0; i < values.length; i++) {
            floatArray[i] = (float) values[i];
        }
        return floatArray;
    }
}
