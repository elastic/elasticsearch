/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.embedding;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.EmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperator;

import java.util.Iterator;
import java.util.stream.IntStream;

/**
 * Builds the output page for the {@link DenseEmbeddingOperator}.
 */
public class DenseEmbeddingOperatorOutputBuilder implements InferenceOperator.OutputBuilder {

    private final FloatBlock.Builder outputBlockBuilder;
    private final Page inputPage;
    private final int dimensions;

    public DenseEmbeddingOperatorOutputBuilder(FloatBlock.Builder outputBlockBuilder, Page inputPage, int dimensions) {
        this.outputBlockBuilder = outputBlockBuilder;
        this.inputPage = inputPage;
        this.dimensions = dimensions;
    }

    @Override
    public void close() {
        Releasables.close(outputBlockBuilder);
        releasePageOnAnyThread(inputPage);
    }

    /**
     * Constructs a new output {@link Page} with dense embedding in the last column.
     */
    @Override
    public Page buildOutput() {
        Block outputBlock = outputBlockBuilder.build();
        assert outputBlock.getPositionCount() == inputPage.getPositionCount();
        return inputPage.shallowCopy().appendBlock(outputBlock);
    }

    /**
     * Extracts the embedding results from the inference response and append them to the output block builder.
     * <p>
     * If the response is not of type {@link TextEmbeddingResults} an {@link IllegalStateException} is thrown.
     * </p>
     * <p>
     * The responses must be added in the same order as the corresponding inference requests were generated.
     * Failing to preserve order may lead to incorrect or misaligned output rows.
     * </p>
     */
    @Override
    public void addInferenceResponse(InferenceAction.Response inferenceResponse) {
        EmbeddingValueReader embeddingValueReader = EmbeddingValueReader.of(inferenceResponse, dimensions);
        while (embeddingValueReader.hasNext()) {
            writeEmbeddings(embeddingValueReader.next());
        }
    }

    private void writeEmbeddings(float[] values) {
        outputBlockBuilder.beginPositionEntry();
        for (float value : values) {
            outputBlockBuilder.appendFloat(value);
        }
        outputBlockBuilder.endPositionEntry();
    }

    private static class EmbeddingValueReader implements Iterator<float[]> {
        private final int dimensions;

        private final Iterator<? extends EmbeddingResults.Embedding<?>> embeddingsIterator;

        private EmbeddingValueReader(Iterator<? extends EmbeddingResults.Embedding<?>> embeddingsIterator, int dimensions) {
            this.dimensions = dimensions;
            this.embeddingsIterator = embeddingsIterator;
        }

        public boolean hasNext() {
            return embeddingsIterator.hasNext();
        }

        public float[] next() {
            EmbeddingResults.Embedding<?> embedding = embeddingsIterator.next();
            float[] values = switch (embedding) {
                case TextEmbeddingFloatResults.Embedding textEmbeddingFloat -> textEmbeddingFloat.values();
                case TextEmbeddingByteResults.Embedding textEmbeddingBytes -> toFloatArray(textEmbeddingBytes.values());
                default -> throw new IllegalStateException("Unsupported embedding type [" + embedding.getClass() + "]");
            };

            assert values.length == dimensions : "Unexpected vector size: " + values.length;

            return values;
        }

        private static float[] toFloatArray(byte[] bytes) {
            float[] floatValues = new float[bytes.length];
            IntStream.range(0, floatValues.length).forEach(i -> floatValues[i] = ((Byte) bytes[i]).floatValue());
            return floatValues;
        }

        public static EmbeddingValueReader of(InferenceAction.Response inferenceResponse, int dimensions) {
            TextEmbeddingResults<?> inferenceResults = InferenceOperator.OutputBuilder.inferenceResults(
                inferenceResponse,
                TextEmbeddingResults.class
            );
            return new EmbeddingValueReader(inferenceResults.embeddings().iterator(), dimensions);
        }
    }
}
