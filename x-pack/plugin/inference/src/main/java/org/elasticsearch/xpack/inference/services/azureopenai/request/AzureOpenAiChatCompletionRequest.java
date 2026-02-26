/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.azureopenai.completion.AzureOpenAiCompletionModel;

import java.util.Objects;

public class AzureOpenAiChatCompletionRequest extends AzureOpenAiRequest {

    private final UnifiedChatInput chatInput;

    public AzureOpenAiChatCompletionRequest(UnifiedChatInput chatInput, AzureOpenAiCompletionModel model) {
        super(Objects.requireNonNull(model), model.getTaskSettings(), createRequestEntity(Objects.requireNonNull(chatInput), model));
        this.chatInput = chatInput;
    }

    private static String createRequestEntity(UnifiedChatInput chatInput, AzureOpenAiCompletionModel model) {
        var user = model.getTaskSettings().user().orElse(null);
        return Strings.toString(new AzureOpenAiChatCompletionRequestEntity(chatInput, user));
    }

    @Override
    public boolean isStreaming() {
        return chatInput.stream();
    }
}
