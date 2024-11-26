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

import static org.elasticsearch.xpack.inference.external.request.openai.OpenAiUnifiedChatCompletionRequestEntity.USER_FIELD;

public class UnifiedChatInput extends InferenceInputs {

    public static UnifiedChatInput of(InferenceInputs inferenceInputs) {
        if (inferenceInputs instanceof UnifiedChatInput == false) {
            throw createUnsupportedTypeException(inferenceInputs, UnifiedChatInput.class);
        }

        return (UnifiedChatInput) inferenceInputs;
    }

    public static UnifiedChatInput of(List<String> input, boolean stream) {
        var unifiedRequest = new UnifiedCompletionRequest(
            convertToMessages(input),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            // TODO we need to get the user field from task settings if it is there
            null
        );

        return new UnifiedChatInput(unifiedRequest, stream);
    }

    private static List<UnifiedCompletionRequest.Message> convertToMessages(List<String> inputs) {
        return inputs.stream()
            .map(doc -> new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString(doc), USER_FIELD, null, null, null))
            .toList();
    }

    private final UnifiedCompletionRequest request;
    private final boolean stream;

    public UnifiedChatInput(UnifiedCompletionRequest request, boolean stream) {
        this.request = Objects.requireNonNull(request);
        this.stream = stream;
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
