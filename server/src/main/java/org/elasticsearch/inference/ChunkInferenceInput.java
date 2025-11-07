/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.core.Nullable;

import java.util.List;

import static org.elasticsearch.inference.InferenceString.DataType.TEXT;

public record ChunkInferenceInput(InferenceString input, @Nullable ChunkingSettings chunkingSettings) {

    public ChunkInferenceInput(String input) {
        this(new InferenceString(input, TEXT), null);
    }

    public static List<InferenceString> inputs(List<ChunkInferenceInput> chunkInferenceInputs) {
        return chunkInferenceInputs.stream().map(ChunkInferenceInput::input).toList();
    }

    public String inputText() {
        assert input.isText();
        return input.value();
    }
}
