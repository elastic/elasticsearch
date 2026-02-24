/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.inference.services.azureopenai.completion.AzureOpenAiCompletionModel;

import java.util.List;
import java.util.Objects;

public class AzureOpenAiCompletionRequest extends AzureOpenAiRequest {

    private final boolean stream;

    public AzureOpenAiCompletionRequest(List<String> input, AzureOpenAiCompletionModel model, boolean stream) {
        super(Objects.requireNonNull(model), model.getTaskSettings(), createRequestEntity(Objects.requireNonNull(input), model, stream));
        this.stream = stream;
    }

    private static String createRequestEntity(List<String> input, AzureOpenAiCompletionModel model, boolean stream) {
        return Strings.toString(new AzureOpenAiCompletionRequestEntity(input, model.getTaskSettings().user(), stream));
    }

    @Override
    public boolean isStreaming() {
        return stream;
    }
}
