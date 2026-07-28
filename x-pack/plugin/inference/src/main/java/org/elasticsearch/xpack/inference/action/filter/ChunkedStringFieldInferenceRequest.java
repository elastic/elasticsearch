/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;

import java.util.Objects;

/**
 * A {@link FieldInferenceRequest} for a single string input that should be chunked.
 */
final class ChunkedStringFieldInferenceRequest extends FieldInferenceRequest {
    /** The input to run inference on. */
    private final String input;
    /** The adjustment to apply to the chunk text offsets. */
    private final int offsetAdjustment;
    /** Additional explicitly specified chunking settings, or null to use model defaults. */
    private final ChunkingSettings chunkingSettings;

    ChunkedStringFieldInferenceRequest(
        int bulkItemIndex,
        String field,
        String sourceField,
        String input,
        int fieldInputOrder,
        int offsetAdjustment,
        @Nullable ChunkingSettings chunkingSettings
    ) {
        super(bulkItemIndex, field, sourceField, fieldInputOrder);
        this.input = input;
        this.offsetAdjustment = offsetAdjustment;
        this.chunkingSettings = chunkingSettings;
    }

    public String input() {
        return input;
    }

    public int offsetAdjustment() {
        return offsetAdjustment;
    }

    public ChunkingSettings chunkingSettings() {
        return chunkingSettings;
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o) == false) return false;
        ChunkedStringFieldInferenceRequest that = (ChunkedStringFieldInferenceRequest) o;
        return offsetAdjustment == that.offsetAdjustment
            && Objects.equals(input, that.input)
            && Objects.equals(chunkingSettings, that.chunkingSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), input, offsetAdjustment, chunkingSettings);
    }
}
