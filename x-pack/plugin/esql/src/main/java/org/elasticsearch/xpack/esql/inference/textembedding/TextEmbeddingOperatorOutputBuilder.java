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

    /**
     * Adds an inference response to the output builder.
     *
     * <p>
     * If the response is null or not of type {@link DenseEmbeddingResults} an {@link IllegalStateException} is thrown.
     * Else, the embedding vector is added to the output block as a multi-value position.
     * </p>
     *
     * <p>
     * The responses must be added in the same order as the corresponding inference requests were generated.
     * Failing to preserve order may lead to incorrect or misaligned output rows.
     * </p>
     */
    @Override
    public void addInferenceResponse(InferenceAction.Response inferenceResponse) {
        if (inferenceResponse == null) {
            outputBlockBuilder.appendNull();
            return;
        }

        DenseEmbeddingResults<?> embeddingResults = inferenceResults(inferenceResponse);

        var embeddings = embeddingResults.embeddings();
        if (embeddings.isEmpty()) {
            outputBlockBuilder.appendNull();
            return;
        }

        float[] embeddingArray = getEmbeddingAsFloatArray(embeddingResults);

        outputBlockBuilder.beginPositionEntry();
        for (float component : embeddingArray) {
            outputBlockBuilder.appendFloat(component);
        }
        outputBlockBuilder.endPositionEntry();
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

    private DenseEmbeddingResults<?> inferenceResults(InferenceAction.Response inferenceResponse) {
        return InferenceOperator.OutputBuilder.inferenceResults(inferenceResponse, DenseEmbeddingResults.class);
    }

    /**
     * Extracts the embedding as a float array from the embedding result.
     */
    private static float[] getEmbeddingAsFloatArray(DenseEmbeddingResults<?> embedding) {
        return switch (embedding.embeddings().get(0)) {
            case DenseEmbeddingFloatResults.Embedding floatEmbedding -> floatEmbedding.values();
            case DenseEmbeddingByteResults.Embedding byteEmbedding -> toFloatArray(byteEmbedding.values());
            default -> throw new IllegalArgumentException(
                "Unsupported embedding type: "
                    + embedding.embeddings().get(0).getClass().getName()
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
            floatArray[i] = ((Byte) values[i]).floatValue();
        }
        return floatArray;
    }
}
