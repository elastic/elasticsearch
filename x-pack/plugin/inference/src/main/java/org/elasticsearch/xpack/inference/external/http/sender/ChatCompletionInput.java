/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.inference.TaskType;

import java.util.List;
import java.util.Objects;

/**
 * This class encapsulates the input text passed by the request and indicates whether the response should be streamed.
 * The main difference between this class and {@link UnifiedChatInput} is this should only be used for
 * {@link org.elasticsearch.inference.TaskType#COMPLETION} originating through the
 * {@link org.elasticsearch.inference.InferenceService#infer} code path. These are requests sent to the
 * API without using the {@link TaskType#CHAT_COMPLETION} task type.
 */
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

    @Override
    public boolean isSingleInput() {
        return input.size() == 1;
    }
}
