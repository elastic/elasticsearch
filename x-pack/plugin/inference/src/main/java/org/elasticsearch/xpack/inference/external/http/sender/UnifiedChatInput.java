/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.inference.UnifiedCompletionRequest;

import java.util.List;
import java.util.Objects;

public class UnifiedChatInput extends InferenceInputs {
    private final UnifiedCompletionRequest request;
    private final boolean stream;

    public UnifiedChatInput(UnifiedCompletionRequest request, boolean stream) {
        this.request = Objects.requireNonNull(request);
        this.stream = stream;
    }

    public UnifiedChatInput(ChatCompletionInput completionInput, String roleValue) {
        this(completionInput.getInputs(), roleValue, completionInput.stream());
    }

    public UnifiedChatInput(List<String> inputs, String roleValue, boolean stream) {
        this(UnifiedCompletionRequest.of(convertToMessages(inputs, roleValue)), stream);
    }

    private static List<UnifiedCompletionRequest.Message> convertToMessages(List<String> inputs, String roleValue) {
        return inputs.stream()
            .map(
                value -> new UnifiedCompletionRequest.Message(
                    new UnifiedCompletionRequest.ContentString(value),
                    roleValue,
                    null,
                    null,
                    null
                )
            )
            .toList();
    }

    public UnifiedCompletionRequest getRequest() {
        return request;
    }

    public boolean stream() {
        return stream;
    }

    public int inputSize() {
        return request.messages().size();
    }
}
