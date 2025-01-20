/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.completion;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.USER;

public record AzureOpenAiCompletionRequestTaskSettings(@Nullable String user) {

    public static final AzureOpenAiCompletionRequestTaskSettings EMPTY_SETTINGS = new AzureOpenAiCompletionRequestTaskSettings(null);

    public static AzureOpenAiCompletionRequestTaskSettings fromMap(Map<String, Object> map) {
        if (map.isEmpty()) {
            return AzureOpenAiCompletionRequestTaskSettings.EMPTY_SETTINGS;
        }

        ValidationException validationException = new ValidationException();

        String user = extractOptionalString(map, USER, ModelConfigurations.TASK_SETTINGS, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AzureOpenAiCompletionRequestTaskSettings(user);
    }
}
