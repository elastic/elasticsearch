/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import java.util.List;
import java.util.Objects;

public class ChatCompletionInput extends InferenceInputs {
    private final List<String> input;

    public ChatCompletionInput(List<String> input) {
        this(input, false);
    }

    public ChatCompletionInput(List<String> input, boolean stream) {
        super(stream);
        this.input = Objects.requireNonNull(input);
    }

    public List<String> getInputs() {
        return this.input;
    }

    public int inputSize() {
        return input.size();
    }
}
