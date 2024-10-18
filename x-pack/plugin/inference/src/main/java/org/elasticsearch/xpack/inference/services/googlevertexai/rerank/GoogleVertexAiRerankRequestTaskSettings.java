/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.rerank;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;

public record GoogleVertexAiRerankRequestTaskSettings(@Nullable Integer topN) {

    public static final GoogleVertexAiRerankRequestTaskSettings EMPTY_SETTINGS = new GoogleVertexAiRerankRequestTaskSettings(null);

    public static GoogleVertexAiRerankRequestTaskSettings fromMap(Map<String, Object> map) {
        if (map.isEmpty()) {
            return GoogleVertexAiRerankRequestTaskSettings.EMPTY_SETTINGS;
        }

        ValidationException validationException = new ValidationException();

        Integer topN = extractOptionalPositiveInteger(
            map,
            GoogleVertexAiRerankTaskSettings.TOP_N,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new GoogleVertexAiRerankRequestTaskSettings(topN);
    }

}
