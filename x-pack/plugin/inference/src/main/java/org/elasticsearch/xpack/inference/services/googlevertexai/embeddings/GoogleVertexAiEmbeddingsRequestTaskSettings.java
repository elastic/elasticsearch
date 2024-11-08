/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.embeddings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;

public record GoogleVertexAiEmbeddingsRequestTaskSettings(@Nullable Boolean autoTruncate) {

    public static final GoogleVertexAiEmbeddingsRequestTaskSettings EMPTY_SETTINGS = new GoogleVertexAiEmbeddingsRequestTaskSettings(null);

    public static GoogleVertexAiEmbeddingsRequestTaskSettings fromMap(Map<String, Object> map) {
        if (map.isEmpty()) {
            return GoogleVertexAiEmbeddingsRequestTaskSettings.EMPTY_SETTINGS;
        }

        ValidationException validationException = new ValidationException();

        Boolean autoTruncate = extractOptionalBoolean(map, GoogleVertexAiEmbeddingsTaskSettings.AUTO_TRUNCATE, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new GoogleVertexAiEmbeddingsRequestTaskSettings(autoTruncate);
    }

}
