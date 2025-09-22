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
import java.util.function.Supplier;

public class EmbeddingsInput extends InferenceInputs {
    private Supplier<List<String>> inputListSupplier;
    private final InputType inputType;

    public EmbeddingsInput(List<String> input, @Nullable InputType inputType) {
        this(() -> input, inputType, false);
    }

    public EmbeddingsInput(List<String> input, @Nullable InputType inputType, boolean stream) {
        this(() -> input, inputType, stream);
    }

    public EmbeddingsInput(Supplier<List<String>> inputSupplier, @Nullable InputType inputType) {
        this(inputSupplier, inputType, false);
    }

    private EmbeddingsInput(Supplier<List<String>> inputSupplier, @Nullable InputType inputType, boolean stream) {
        super(stream);
        this.inputListSupplier = Objects.requireNonNull(inputSupplier);
        this.inputType = inputType;
    }

    public List<String> getInputs() {
        // The supplier should only be invoked once
        assert inputListSupplier != null;
        List<String> strings = inputListSupplier.get();
        inputListSupplier = null;
        return strings;
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
