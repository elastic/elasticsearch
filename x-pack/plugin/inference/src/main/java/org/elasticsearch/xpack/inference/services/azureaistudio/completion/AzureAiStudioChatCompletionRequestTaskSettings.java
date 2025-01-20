/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.completion;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalDoubleInRange;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.DO_SAMPLE_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.MAX_NEW_TOKENS_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TEMPERATURE_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TOP_P_FIELD;

public record AzureAiStudioChatCompletionRequestTaskSettings(
    @Nullable Double temperature,
    @Nullable Double topP,
    @Nullable Boolean doSample,
    @Nullable Integer maxNewTokens
) {

    public static final AzureAiStudioChatCompletionRequestTaskSettings EMPTY_SETTINGS = new AzureAiStudioChatCompletionRequestTaskSettings(
        null,
        null,
        null,
        null
    );

    /**
     * Extracts the task settings from a map. All settings are considered optional and the absence of a setting
     * does not throw an error.
     *
     * @param map the settings received from a request
     * @return a {@link AzureAiStudioChatCompletionRequestTaskSettings}
     */
    public static AzureAiStudioChatCompletionRequestTaskSettings fromMap(Map<String, Object> map) {
        if (map.isEmpty()) {
            return AzureAiStudioChatCompletionRequestTaskSettings.EMPTY_SETTINGS;
        }

        ValidationException validationException = new ValidationException();

        var temperature = extractOptionalDoubleInRange(
            map,
            TEMPERATURE_FIELD,
            AzureAiStudioConstants.MIN_TEMPERATURE_TOP_P,
            AzureAiStudioConstants.MAX_TEMPERATURE_TOP_P,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );
        var topP = extractOptionalDoubleInRange(
            map,
            TOP_P_FIELD,
            AzureAiStudioConstants.MIN_TEMPERATURE_TOP_P,
            AzureAiStudioConstants.MAX_TEMPERATURE_TOP_P,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );
        Boolean doSample = extractOptionalBoolean(map, DO_SAMPLE_FIELD, validationException);
        Integer maxNewTokens = extractOptionalPositiveInteger(
            map,
            MAX_NEW_TOKENS_FIELD,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AzureAiStudioChatCompletionRequestTaskSettings(temperature, topP, doSample, maxNewTokens);
    }
}
