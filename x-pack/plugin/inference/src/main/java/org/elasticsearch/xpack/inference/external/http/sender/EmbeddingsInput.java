/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class EmbeddingsInput extends InferenceInputs {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(EmbeddingsInput.class);

    private final Supplier<List<InferenceStringGroup>> inputListSupplier;
    private final InputType inputType;
    private final AtomicBoolean supplierInvoked = new AtomicBoolean();
    private final long estimatedSizeInBytes;

    public EmbeddingsInput(List<InferenceStringGroup> input, @Nullable InputType inputType, boolean stream) {
        this(() -> input, estimateSizeInBytes(input), inputType, stream);
    }

    public EmbeddingsInput(List<InferenceStringGroup> input, @Nullable InputType inputType) {
        this(() -> input, input.stream().mapToLong(InferenceStringGroup::ramBytesUsed).sum(), inputType);
    }

    public EmbeddingsInput(Supplier<List<InferenceStringGroup>> inputSupplier, long estimatedSizeInBytes, @Nullable InputType inputType) {
        this(inputSupplier, estimatedSizeInBytes, inputType, false);
    }

    private EmbeddingsInput(
        Supplier<List<InferenceStringGroup>> inputSupplier,
        long estimatedSizeInBytes,
        @Nullable InputType inputType,
        boolean stream
    ) {
        super(stream);
        this.inputListSupplier = Objects.requireNonNull(inputSupplier);
        this.inputType = inputType;
        this.estimatedSizeInBytes = estimatedSizeInBytes;
    }

    public static EmbeddingsInput fromStrings(List<String> input, @Nullable InputType inputType, boolean stream) {
        var ramBytesUsed = input.stream().mapToLong(RamUsageEstimator::sizeOf).sum();
        return new EmbeddingsInput(
            () -> input.stream().map(InferenceStringGroup::new).collect(Collectors.toList()),
            ramBytesUsed,
            inputType,
            stream
        );
    }

    private static long estimateSizeInBytes(List<InferenceStringGroup> input) {
        return input.stream().mapToLong(InferenceStringGroup::ramBytesUsed).sum();
    }

    /**
     * Calling this method twice will result in the {@link #inputListSupplier} being invoked twice. In the case where the supplier simply
     * returns the list passed into the constructor, this is not a problem, but in the case where a supplier that will chunk the input
     * Strings when invoked is passed into the constructor, this will result in multiple copies of the input Strings being created. Calling
     * this method twice in a non-production environment will cause an {@link AssertionError} to be thrown.
     *
     * @return a list of {@link InferenceString} embedding inputs
     */
    public List<InferenceStringGroup> getInputs() {
        assert supplierInvoked.compareAndSet(false, true) : "EmbeddingsInput supplier invoked twice";
        return inputListSupplier.get();
    }

    /**
     * This method should only be called in code paths that both do not handle grouped embedding inputs AND that do not deal with
     * multimodal inputs, i.e. code paths that expect to generate one embedding per input rather than one embedding for multiple inputs,
     * AND where all inputs are guaranteed to be raw text, since it discards the {@link DataType} associated with
     * each input.
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
        return InferenceStringGroup.toStringList(inputListSupplier.get());
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

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + estimatedSizeInBytes;
    }
}
