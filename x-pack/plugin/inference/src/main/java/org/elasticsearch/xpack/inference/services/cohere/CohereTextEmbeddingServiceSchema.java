/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.elasticsearch.core.Strings;
import org.elasticsearch.xpack.inference.services.custom.PredefinedCustomServiceSchema;

import java.util.List;
import java.util.Map;

public class CohereTextEmbeddingServiceSchema implements PredefinedCustomServiceSchema {

    @Override
    public List<String> getNonParameterServiceSettings() {
        return List.of("similarity");
    }

    @Override
    public List<String> getServiceSettingsParameters() {
        return List.of("model_id", "embedding_type");
    }

    @Override
    public List<String> getServiceSettingsSecretParameters() {
        return List.of("api_key");
    }

    @Override
    public List<String> getTaskSettingsParameters() {
        return List.of();
        // TODO: Do we need anything here?
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public String generateServiceSettings(Map<String, Object> parameters) {
        var embeddingType = parameters.getOrDefault("embedding_type", "binary");
        return Strings.format(
            """
                {
                    "url": "https://api.cohere.com/v2/embed",
                    "headers": {
                        "Authorization": "bearer ${api_key}",
                        "Content-Type": "application/json"
                    },
                    "request": "{\\"texts\\": ${input}, \\"model\\": ${model_id}, \\"input_type\\": ${input_type}, \\"embedding_types\\": [\\"%s\\"]}",
                    "response": {
                        "json_parser": {
                            "text_embeddings":"$.embeddings.%s[*]",
                            "embedding_type": "%s"
                        }
                    },
                    "input_type": {
                        "translation": {
                            "ingest": "search_document",
                            "search": "search_query"
                        },
                        "default": "search_document"
                    }
                }
                """,
            embeddingType,
            embeddingType,
            embeddingType
        );
    }
}
