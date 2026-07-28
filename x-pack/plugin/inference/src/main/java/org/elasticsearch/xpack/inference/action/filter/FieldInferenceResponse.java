/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.Model;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A field inference response.
 * <p>
 * This is the abstract base for concrete response shapes that carry payload-specific data (e.g. chunked text vs.
 * binary results). It holds the routing information shared by all variants: which field the response is for, which
 * source field the input came from, the input's position relative to siblings, and the model that produced it.
 */
abstract sealed class FieldInferenceResponse permits InferenceStringFieldInferenceResponse, ChunkedStringFieldInferenceResponse {
    /** The target field. */
    private final String field;
    /** The source field that the input was extracted from. */
    private final String sourceField;
    /** The original order of the input across all source fields. */
    private final int fieldInputOrder;
    /** The model used to run inference, or {@code null} for cleared/empty responses. */
    private final Model model;

    protected FieldInferenceResponse(String field, String sourceField, int fieldInputOrder, @Nullable Model model) {
        this.field = field;
        this.sourceField = sourceField;
        this.fieldInputOrder = fieldInputOrder;
        this.model = model;
    }

    public String field() {
        return field;
    }

    public String sourceField() {
        return sourceField;
    }

    public int fieldInputOrder() {
        return fieldInputOrder;
    }

    @Nullable
    public Model model() {
        return model;
    }

    public abstract List<SemanticTextField.Chunk> toChunks(boolean useLegacyFormat, XContentType contentType) throws IOException;

    /**
     * Returns the original input as recorded for the legacy semantic text format, or throws if this response shape
     * does not participate in the legacy format.
     */
    @Nullable
    public abstract String legacyInput();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldInferenceResponse that = (FieldInferenceResponse) o;
        return fieldInputOrder == that.fieldInputOrder
            && Objects.equals(field, that.field)
            && Objects.equals(sourceField, that.sourceField)
            && Objects.equals(model, that.model);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, sourceField, fieldInputOrder, model);
    }
}
