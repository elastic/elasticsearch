/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.Model;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.EmbeddingResults;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A {@link FieldInferenceResponse} for inference results produced from a single typed {@link InferenceString} input.
 */
final class InferenceStringFieldInferenceResponse extends FieldInferenceResponse {
    /** The position of the input within its source field. */
    private final int sourceFieldInputIndex;
    /** the inference results. */
    private final EmbeddingResults.Embedding<?> inferenceResults;

    InferenceStringFieldInferenceResponse(
        String field,
        String sourceField,
        int fieldInputOrder,
        int sourceFieldInputIndex,
        @Nullable Model model,
        EmbeddingResults.Embedding<?> inferenceResults
    ) {
        super(field, sourceField, fieldInputOrder, model);
        this.sourceFieldInputIndex = sourceFieldInputIndex;
        this.inferenceResults = inferenceResults;
    }

    public int sourceFieldInputIndex() {
        return sourceFieldInputIndex;
    }

    public EmbeddingResults.Embedding<?> inferenceResults() {
        return inferenceResults;
    }

    @Override
    public List<SemanticTextField.Chunk> toChunks(boolean useLegacyFormat, XContentType contentType) throws IOException {
        if (useLegacyFormat) {
            throw new IllegalStateException("Legacy semantic text format does not support non-text chunks for field [" + field() + "]");
        }
        return List.of(SemanticTextField.toSemanticFieldChunk(sourceFieldInputIndex, inferenceResults, contentType));
    }

    @Override
    public String legacyInput() {
        throw new IllegalStateException(
            "legacyInput should not be called on " + InferenceStringFieldInferenceResponse.class.getSimpleName()
        );
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o) == false) return false;
        InferenceStringFieldInferenceResponse that = (InferenceStringFieldInferenceResponse) o;
        return sourceFieldInputIndex == that.sourceFieldInputIndex && Objects.equals(inferenceResults, that.inferenceResults);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sourceFieldInputIndex, inferenceResults);
    }
}
