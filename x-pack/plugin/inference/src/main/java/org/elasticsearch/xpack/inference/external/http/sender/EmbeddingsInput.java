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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class EmbeddingsInput extends InferenceInputs {
    private final Supplier<List<String>> inputListSupplier;
    private final InputType inputType;
    private final AtomicBoolean supplierInvoked = new AtomicBoolean();

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

    /**
     * Calling this method twice will result in the {@link #inputListSupplier} being invoked twice. In the case where the supplier simply
     * returns the list passed into the constructor, this is not a problem, but in the case where a supplier that will chunk the input
     * Strings when invoked is passed into the constructor, this will result in multiple copies of the input Strings being created. Calling
     * this method twice in a non-production environment will cause an {@link AssertionError} to be thrown.
     *
     * @return a list of String embedding inputs
     */
    public List<String> getInputs() {
        assert supplierInvoked.compareAndSet(false, true) : "EmbeddingsInput supplier invoked twice";
        return inputListSupplier.get();
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
