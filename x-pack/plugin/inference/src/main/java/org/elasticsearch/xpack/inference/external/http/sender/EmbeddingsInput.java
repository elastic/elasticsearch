/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;

import java.util.List;
import java.util.Objects;

public class EmbeddingsInput extends InferenceInputs {

    public static EmbeddingsInput of(InferenceInputs inferenceInputs) {
        if (inferenceInputs instanceof EmbeddingsInput == false) {
            throw createUnsupportedTypeException(inferenceInputs, EmbeddingsInput.class);
        }

        return (EmbeddingsInput) inferenceInputs;
    }

    private final List<String> input;

    private final InputType inputType;

    public EmbeddingsInput(List<String> input, @Nullable InputType inputType) {
        this(input, inputType, false);
    }

    public EmbeddingsInput(List<String> input, @Nullable InputType inputType, boolean stream) {
        super(stream);
        this.input = Objects.requireNonNull(input);
        this.inputType = inputType;
    }

    public List<String> getInputs() {
        return this.input;
    }

    public InputType getInputType() {
        return this.inputType;
    }

    public int inputSize() {
        return input.size();
    }
}
