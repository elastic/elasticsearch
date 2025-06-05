/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;

import java.util.List;
import java.util.Objects;

/**
 * This class encapsulates the unified request.
 * The main difference between this class and {@link ChatCompletionInput} is this should only be used for
 * {@link org.elasticsearch.inference.TaskType#COMPLETION} originating through the
 * {@link org.elasticsearch.inference.InferenceService#unifiedCompletionInfer(Model, UnifiedCompletionRequest, TimeValue, ActionListener)}
 * code path. These are requests sent to the API with the <code>_stream</code> route and {@link TaskType#CHAT_COMPLETION}.
 */
public class UnifiedChatInput extends InferenceInputs {
    private final UnifiedCompletionRequest request;

    public UnifiedChatInput(UnifiedCompletionRequest request, boolean stream) {
        super(stream);
        this.request = Objects.requireNonNull(request);
    }

    public UnifiedChatInput(ChatCompletionInput completionInput, String roleValue) {
        this(completionInput.getInputs(), roleValue, completionInput.stream());
    }

    public UnifiedChatInput(List<String> inputs, String roleValue, boolean stream) {
        this(UnifiedCompletionRequest.of(convertToMessages(inputs, roleValue)), stream);
    }

    private static List<UnifiedCompletionRequest.Message> convertToMessages(List<String> inputs, String roleValue) {
        return inputs.stream()
            .map(value -> new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString(value), roleValue, null, null))
            .toList();
    }

    public UnifiedCompletionRequest getRequest() {
        return request;
    }

    public boolean isSingleInput() {
        return request.messages().size() == 1;
    }
}
