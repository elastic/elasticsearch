/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.completion;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeAsType;

/**
 * This class handles extracting OpenAI task settings from a request. The difference between this class and
 * {@link AnthropicChatCompletionTaskSettings} is that this class considers all fields as optional. It will not throw an error if a field
 * is missing. This allows overriding persistent task settings.
 * @param optionalSettings an object to pass through to the Anthropic messages API
 */
public record AnthropicChatCompletionRequestTaskSettings(@Nullable Map<String, Object> optionalSettings) {

    public static final AnthropicChatCompletionRequestTaskSettings EMPTY_SETTINGS = new AnthropicChatCompletionRequestTaskSettings(null);

    /**
     * Extracts the task settings from a map. All settings are considered optional and the absence of a setting
     * does not throw an error.
     *
     * @param map the settings received from a request
     * @return a {@link AnthropicChatCompletionRequestTaskSettings}
     */
    public static AnthropicChatCompletionRequestTaskSettings fromMap(Map<String, Object> map) {
        if (map.isEmpty()) {
            return AnthropicChatCompletionRequestTaskSettings.EMPTY_SETTINGS;
        }

        ValidationException validationException = new ValidationException();

        @SuppressWarnings("unchecked")
        Map<String, Object> optionalSettings = (Map<String, Object>) removeAsType(map, "optional_settings", Map.class);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AnthropicChatCompletionRequestTaskSettings(optionalSettings);
    }

}
