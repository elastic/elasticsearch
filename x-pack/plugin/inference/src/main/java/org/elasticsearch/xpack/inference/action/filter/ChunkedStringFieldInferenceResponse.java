/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.Model;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A {@link FieldInferenceResponse} for chunked inference results produced from a single string input.
 */
final class ChunkedStringFieldInferenceResponse extends FieldInferenceResponse {
    /** The input that was used to run inference, or {@code null} when not retained (non-legacy format). */
    private final String input;
    /** The adjustment to apply to the chunk text offsets. */
    private final int offsetAdjustment;
    /** The actual chunked inference results. */
    private final ChunkedInference chunkedResults;

    ChunkedStringFieldInferenceResponse(
        String field,
        String sourceField,
        @Nullable String input,
        int fieldInputOrder,
        int offsetAdjustment,
        @Nullable Model model,
        ChunkedInference chunkedResults
    ) {
        super(field, sourceField, fieldInputOrder, model);
        this.input = input;
        this.offsetAdjustment = offsetAdjustment;
        this.chunkedResults = chunkedResults;
    }

    public int offsetAdjustment() {
        return offsetAdjustment;
    }

    public ChunkedInference chunkedResults() {
        return chunkedResults;
    }

    @Override
    public List<SemanticTextField.Chunk> toChunks(boolean useLegacyFormat, XContentType contentType) throws IOException {
        return useLegacyFormat
            ? SemanticTextField.toSemanticTextFieldChunksLegacy(input, chunkedResults, contentType)
            : SemanticTextField.toSemanticTextFieldChunks(offsetAdjustment, chunkedResults, contentType);
    }

    @Override
    public String legacyInput() {
        return input;
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o) == false) return false;
        ChunkedStringFieldInferenceResponse that = (ChunkedStringFieldInferenceResponse) o;
        return offsetAdjustment == that.offsetAdjustment
            && Objects.equals(input, that.input)
            && Objects.equals(chunkedResults, that.chunkedResults);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), input, offsetAdjustment, chunkedResults);
    }
}
