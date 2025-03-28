/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InputType;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class EmbeddingsInput extends InferenceInputs {

    public static EmbeddingsInput of(InferenceInputs inferenceInputs) {
        if (inferenceInputs instanceof EmbeddingsInput == false) {
            throw createUnsupportedTypeException(inferenceInputs, EmbeddingsInput.class);
        }

        return (EmbeddingsInput) inferenceInputs;
    }

    private final List<ChunkInferenceInput> input;

    private final InputType inputType;

    public EmbeddingsInput(List<ChunkInferenceInput> input, @Nullable InputType inputType) {
        this(input, inputType, false);
    }

    public EmbeddingsInput(List<String> input, @Nullable ChunkingSettings chunkingSettings, @Nullable InputType inputType) {
        this(input.stream().map(i -> new ChunkInferenceInput(i, chunkingSettings)).collect(Collectors.toList()), inputType, false);
    }

    public EmbeddingsInput(List<ChunkInferenceInput> input, @Nullable InputType inputType, boolean stream) {
        super(stream);
        this.input = Objects.requireNonNull(input);
        this.inputType = inputType;
    }

    public List<ChunkInferenceInput> getInputs() {
        return this.input;
    }

    public List<String> getStringInputs() {
        return this.input.stream().map(ChunkInferenceInput::input).collect(Collectors.toList());
    }

    public InputType getInputType() {
        return this.inputType;
    }

    public int inputSize() {
        return input.size();
    }
}
