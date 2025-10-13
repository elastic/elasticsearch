/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.rerank;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.RETURN_DOCUMENTS_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TOP_N_FIELD;

public record AzureAiStudioRerankRequestTaskSettings(@Nullable Boolean returnDocuments, @Nullable Integer topN) {

    public static final AzureAiStudioRerankRequestTaskSettings EMPTY_SETTINGS = new AzureAiStudioRerankRequestTaskSettings(null, null);

    /**
     * Extracts the task settings from a map. All settings are considered optional and the absence of a setting
     * does not throw an error.
     *
     * @param map the settings received from a request
     * @return a {@link AzureAiStudioRerankRequestTaskSettings}
     */
    public static AzureAiStudioRerankRequestTaskSettings fromMap(Map<String, Object> map) {
        if (map.isEmpty()) {
            return AzureAiStudioRerankRequestTaskSettings.EMPTY_SETTINGS;
        }

        final var validationException = new ValidationException();

        final var returnDocuments = extractOptionalBoolean(map, RETURN_DOCUMENTS_FIELD, validationException);
        final var topN = extractOptionalPositiveInteger(map, TOP_N_FIELD, ModelConfigurations.TASK_SETTINGS, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AzureAiStudioRerankRequestTaskSettings(returnDocuments, topN);
    }
}
