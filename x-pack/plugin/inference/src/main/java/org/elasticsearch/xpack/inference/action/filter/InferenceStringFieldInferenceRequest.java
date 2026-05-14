/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

import org.elasticsearch.inference.InferenceString;

import java.util.Objects;

/**
 * A {@link FieldInferenceRequest} for a single typed {@link InferenceString} input.
 */
final class InferenceStringFieldInferenceRequest extends FieldInferenceRequest {
    /** The input to run inference on. */
    private final InferenceString input;
    /** The position of the input within its source field. */
    private final int sourceFieldInputIndex;

    InferenceStringFieldInferenceRequest(
        int bulkItemIndex,
        String field,
        String sourceField,
        InferenceString input,
        int fieldInputOrder,
        int sourceFieldInputIndex
    ) {
        super(bulkItemIndex, field, sourceField, fieldInputOrder);
        this.input = input;
        this.sourceFieldInputIndex = sourceFieldInputIndex;
    }

    public InferenceString input() {
        return input;
    }

    public int sourceFieldInputIndex() {
        return sourceFieldInputIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o) == false) return false;
        InferenceStringFieldInferenceRequest that = (InferenceStringFieldInferenceRequest) o;
        return sourceFieldInputIndex == that.sourceFieldInputIndex && Objects.equals(input, that.input);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), input, sourceFieldInputIndex);
    }
}
