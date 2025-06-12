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
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class EmbeddingsInput extends InferenceInputs {

    public static EmbeddingsInput of(InferenceInputs inferenceInputs) {
        if (inferenceInputs instanceof EmbeddingsInput == false) {
            throw createUnsupportedTypeException(inferenceInputs, EmbeddingsInput.class);
        }

        return (EmbeddingsInput) inferenceInputs;
    }

    private final Supplier<List<ChunkInferenceInput>> listSupplier;
    private final InputType inputType;

    public EmbeddingsInput(List<ChunkInferenceInput> input, @Nullable InputType inputType) {
        this(input, inputType, false);
    }

    public EmbeddingsInput(Supplier<List<ChunkInferenceInput>> inputSupplier, @Nullable InputType inputType) {
        super(false);
        this.listSupplier = Objects.requireNonNull(inputSupplier);
        this.inputType = inputType;
    }

    public EmbeddingsInput(List<String> input, @Nullable ChunkingSettings chunkingSettings, @Nullable InputType inputType) {
        this(input.stream().map(i -> new ChunkInferenceInput(i, chunkingSettings)).collect(Collectors.toList()), inputType, false);
    }

    public EmbeddingsInput(List<ChunkInferenceInput> input, @Nullable InputType inputType, boolean stream) {
        super(stream);
        Objects.requireNonNull(input);
        this.listSupplier = () -> input;
        this.inputType = inputType;
    }

    public List<ChunkInferenceInput> getInputs() {
        return this.listSupplier.get();
    }

    public static EmbeddingsInput fromStrings(List<String> input, @Nullable InputType inputType) {
        return new EmbeddingsInput(input, null, inputType);
    }

    public List<String> getStringInputs() {
        return getInputs().stream().map(ChunkInferenceInput::input).collect(Collectors.toList());
    }

    public InputType getInputType() {
        return this.inputType;
    }

    @Override
    public boolean isSingleInput() {
        // We can't measure the size of the input list without executing
        // the supplier.
        return false;
    }
}
