/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InputType;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.inference.InferenceString.DataType.TEXT;

public class EmbeddingsInput extends InferenceInputs {
    private final Supplier<List<InferenceString>> inputListSupplier;
    private final InputType inputType;
    private final AtomicBoolean supplierInvoked = new AtomicBoolean();

    public EmbeddingsInput(List<String> input, @Nullable InputType inputType) {
        this(() -> input.stream().map(s -> new InferenceString(s, TEXT)).collect(Collectors.toList()), inputType, false);
    }

    public EmbeddingsInput(List<String> input, @Nullable InputType inputType, boolean stream) {
        this(() -> input.stream().map(s -> new InferenceString(s, TEXT)).collect(Collectors.toList()), inputType, stream);
    }

    public EmbeddingsInput(Supplier<List<InferenceString>> inputSupplier, @Nullable InputType inputType) {
        this(inputSupplier, inputType, false);
    }

    private EmbeddingsInput(Supplier<List<InferenceString>> inputSupplier, @Nullable InputType inputType, boolean stream) {
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
     * @return a list of {@link InferenceString} embedding inputs
     */
    public List<InferenceString> getInputs() {
        assert supplierInvoked.compareAndSet(false, true) : "EmbeddingsInput supplier invoked twice";
        return inputListSupplier.get();
    }

    /**
     * This method should only be called in code paths that do not deal with multimodal embeddings; where all inputs are guaranteed to be
     * raw text, since it discards the {@link org.elasticsearch.inference.InferenceString.DataType} associated with each input.
     * <p>
     * Calling this method twice will result in the {@link #inputListSupplier} being invoked twice. In the case where the supplier simply
     * returns the list passed into the constructor, this is not a problem, but in the case where a supplier that will chunk the input
     * Strings when invoked is passed into the constructor, this will result in multiple copies of the input Strings being created. Calling
     * this method twice in a non-production environment will cause an {@link AssertionError} to be thrown.
     *
     * @return a list of String embedding inputs that do not contain any non-text inputs
     * @see InferenceString#toStringList(List)
     */
    public List<String> getTextInputs() {
        assert supplierInvoked.compareAndSet(false, true) : "EmbeddingsInput supplier invoked twice";
        return InferenceString.toStringList(inputListSupplier.get());
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
