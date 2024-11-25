/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.xpack.inference.external.request.openai.OpenAiUnifiedChatCompletionRequestEntity;

import java.util.Objects;

public class UnifiedChatInput extends InferenceInputs {

    public static UnifiedChatInput of(InferenceInputs inferenceInputs) {

        if (inferenceInputs instanceof DocumentsOnlyInput docsOnly) {
            return new UnifiedChatInput(new OpenAiUnifiedChatCompletionRequestEntity(docsOnly));
        } else if (inferenceInputs instanceof UnifiedChatInput == false) {
            throw createUnsupportedTypeException(inferenceInputs);
        }

        return (UnifiedChatInput) inferenceInputs;
    }

    public OpenAiUnifiedChatCompletionRequestEntity getRequestEntity() {
        return requestEntity;
    }

    private final OpenAiUnifiedChatCompletionRequestEntity requestEntity;

    public UnifiedChatInput(OpenAiUnifiedChatCompletionRequestEntity requestEntity) {
        this.requestEntity = Objects.requireNonNull(requestEntity);
    }

    public boolean stream() {
        return requestEntity.isStream();
    }
}
