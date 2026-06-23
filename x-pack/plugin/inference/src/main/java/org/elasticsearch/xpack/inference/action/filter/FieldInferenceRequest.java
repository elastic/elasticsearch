/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

import java.util.Objects;

/**
 * A field inference request.
 * <p>
 * This is the abstract base for concrete request shapes that carry payload-specific data (e.g. text input vs. binary
 * input). It holds the routing information shared by all variants: where the request originated in the bulk request,
 * which field it targets, which source field it was extracted from, and its position relative to sibling inputs.
 */
abstract sealed class FieldInferenceRequest permits InferenceStringFieldInferenceRequest, ChunkedStringFieldInferenceRequest {
    /** The index of the item in the original bulk request. */
    private final int bulkItemIndex;
    /** The target field. */
    private final String field;
    /** The source field. */
    private final String sourceField;
    /** The original order of the input across all source fields. */
    private final int fieldInputOrder;

    protected FieldInferenceRequest(int bulkItemIndex, String field, String sourceField, int fieldInputOrder) {
        this.bulkItemIndex = bulkItemIndex;
        this.field = field;
        this.sourceField = sourceField;
        this.fieldInputOrder = fieldInputOrder;
    }

    public int bulkItemIndex() {
        return bulkItemIndex;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldInferenceRequest that = (FieldInferenceRequest) o;
        return bulkItemIndex == that.bulkItemIndex
            && fieldInputOrder == that.fieldInputOrder
            && Objects.equals(field, that.field)
            && Objects.equals(sourceField, that.sourceField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bulkItemIndex, field, sourceField, fieldInputOrder);
    }
}
