/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.textembedding;

import org.elasticsearch.compute.data.FloatArrayBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingBitResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperator;

/**
 * {@link TextEmbeddingOperatorOutputBuilder} builds the output page for {@link TextEmbeddingOperator} by converting
 * {@link TextEmbeddingResults} into a {@link FloatArrayBlock}.
 */
public class TextEmbeddingOperatorOutputBuilder implements InferenceOperator.OutputBuilder {
    private final Page inputPage;
    private final FloatBlock.Builder outputBlockBuilder;

    public TextEmbeddingOperatorOutputBuilder(FloatBlock.Builder outputBlockBuilder, Page inputPage) {
        this.inputPage = inputPage;
        this.outputBlockBuilder = outputBlockBuilder;
    }

    @Override
    public void close() {
        Releasables.close(outputBlockBuilder);
        releasePageOnAnyThread(inputPage);
    }

    /**
     * Adds an inference response to the output builder.
     *
     * <p>
     * If the response is null or not of type {@link TextEmbeddingResults} an {@link IllegalStateException} is thrown.
     * Else, the embedding values are extracted and added to the output block.
     * </p>
     *
     * <p>
     * The responses must be added in the same order as the corresponding inference requests were generated.
     * Failing to preserve order may lead to incorrect or misaligned output rows.
     * </p>
     *
     * @param inferenceResponse The inference response to include.
     */
    @Override
    public void addInferenceResponse(InferenceAction.Response inferenceResponse) {
        var textEmbeddingResults = inferenceResults(inferenceResponse);

        // Extract the first embedding (text embedding typically returns one embedding per input)
        if (textEmbeddingResults.embeddings().isEmpty()) {
            throw new IllegalStateException("Text embedding response contains no embeddings");
        }

        float[] embeddingValues = getEmbeddingValues(textEmbeddingResults);

        outputBlockBuilder.beginPositionEntry();
        for (int i = 0; i < embeddingValues.length; i++) {
            outputBlockBuilder.appendFloat(embeddingValues[i]);
        }
        outputBlockBuilder.endPositionEntry();
    }

    /**
     * Builds the final output page by appending the embedding output block to a shallow copy of the input page.
     */
    @Override
    public Page buildOutput() {
        return new Page(outputBlockBuilder.build());
    }

    private TextEmbeddingResults<?> inferenceResults(InferenceAction.Response inferenceResponse) {
        return InferenceOperator.OutputBuilder.inferenceResults(inferenceResponse, TextEmbeddingResults.class);
    }

    /**
     * Extracts float array values from different types of text embedding results.
     * Handles TextEmbeddingFloatResults, TextEmbeddingByteResults, and TextEmbeddingBitResults.
     */
    private float[] getEmbeddingValues(TextEmbeddingResults<?> result) {
        return switch (result) {
            case TextEmbeddingFloatResults floatEmbeddingResults -> floatEmbeddingResults.embeddings().get(0).values();
            case TextEmbeddingByteResults bytesEmbeddingResults -> bytesEmbeddingResults.embeddings().get(0).toFloatArray();
            case TextEmbeddingBitResults bitsEmbeddingResults -> bitsEmbeddingResults.embeddings().get(0).toFloatArray();
            default -> throw new IllegalArgumentException("Inference response should be of type TextEmbeddingResults");
        };
    }
}
