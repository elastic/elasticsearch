/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.elasticsearch.inference.InferenceString.DataType.IMAGE_BASE64;
import static org.elasticsearch.inference.InferenceString.DataType.TEXT;
import static org.hamcrest.Matchers.is;

public class EmbeddingsInputTests extends ESTestCase {
    public void testCallingGetInputs_invokesSupplier() {
        AtomicBoolean invoked = new AtomicBoolean();
        final List<InferenceString> list = List.of(new InferenceString("input1", TEXT), new InferenceString("image_url", IMAGE_BASE64));
        Supplier<List<InferenceString>> supplier = () -> {
            invoked.set(true);
            return list;
        };
        EmbeddingsInput input = new EmbeddingsInput(supplier, null);
        // Ensure we don't invoke the supplier until we call getInputs()
        assertThat(invoked.get(), is(false));

        assertThat(input.getInputs(), is(list));
        assertThat(invoked.get(), is(true));
    }

    public void testCallingGetTextInputs_invokesSupplier() {
        AtomicBoolean invoked = new AtomicBoolean();
        var textInputs = List.of("input1", "input2");
        final List<InferenceString> list = textInputs.stream().map(i -> new InferenceString(i, TEXT)).toList();
        Supplier<List<InferenceString>> supplier = () -> {
            invoked.set(true);
            return list;
        };
        EmbeddingsInput input = new EmbeddingsInput(supplier, null);
        // Ensure we don't invoke the supplier until we call getTextInputs()
        assertThat(invoked.get(), is(false));

        assertThat(input.getTextInputs(), is(textInputs));
        assertThat(invoked.get(), is(true));
    }

    public void testCallingGetTextInputs_withNonTextInput_throws() {
        Supplier<List<InferenceString>> supplier = () -> List.of(
            new InferenceString("input1", TEXT),
            new InferenceString("image_url", IMAGE_BASE64)
        );
        EmbeddingsInput input = new EmbeddingsInput(supplier, null);
        var exception = expectThrows(AssertionError.class, input::getTextInputs);
        assertThat(exception.getMessage(), is("Non-text input passed to InferenceString.toStringList"));
    }

    public void testCallingGetInputsTwice_throws() {
        Supplier<List<InferenceString>> supplier = () -> List.of(new InferenceString("input1", TEXT));
        EmbeddingsInput input = new EmbeddingsInput(supplier, null);
        input.getInputs();
        var exception = expectThrows(AssertionError.class, input::getInputs);
        assertThat(exception.getMessage(), is("EmbeddingsInput supplier invoked twice"));
    }

    public void testCallingGetTextInputsTwice_throws() {
        Supplier<List<InferenceString>> supplier = () -> List.of(new InferenceString("input1", TEXT));
        EmbeddingsInput input = new EmbeddingsInput(supplier, null);
        input.getTextInputs();
        var exception = expectThrows(AssertionError.class, input::getTextInputs);
        assertThat(exception.getMessage(), is("EmbeddingsInput supplier invoked twice"));
    }

    public void testCallingEitherGetInputsMethodTwice_throws() {
        Supplier<List<InferenceString>> supplier = () -> List.of(new InferenceString("input1", TEXT));
        EmbeddingsInput input = new EmbeddingsInput(supplier, null);
        input.getInputs();
        var exception = expectThrows(AssertionError.class, input::getTextInputs);
        assertThat(exception.getMessage(), is("EmbeddingsInput supplier invoked twice"));
    }
}
