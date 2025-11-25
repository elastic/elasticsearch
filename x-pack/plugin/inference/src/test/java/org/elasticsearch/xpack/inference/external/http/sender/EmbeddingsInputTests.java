/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.is;

public class EmbeddingsInputTests extends ESTestCase {
    public void testCallingGetInputs_invokesSupplier() {
        AtomicBoolean invoked = new AtomicBoolean();
        final List<String> list = List.of("input1", "input2");
        Supplier<List<String>> supplier = () -> {
            invoked.set(true);
            return list;
        };
        EmbeddingsInput input = new EmbeddingsInput(supplier, null);
        // Ensure we don't invoke the supplier until we call getInputs()
        assertThat(invoked.get(), is(false));

        assertThat(input.getInputs(), is(list));
        assertThat(invoked.get(), is(true));
    }

    public void testCallingGetInputsTwice_throws() {
        Supplier<List<String>> supplier = () -> List.of("input");
        EmbeddingsInput input = new EmbeddingsInput(supplier, null);
        input.getInputs();
        var exception = expectThrows(AssertionError.class, input::getInputs);
        assertThat(exception.getMessage(), is("EmbeddingsInput supplier invoked twice"));
    }
}
