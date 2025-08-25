/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.elasticsearch.xpack.inference.services.custom.PredefinedCustomServiceSchema;

import java.util.List;
import java.util.Map;

public class CohereRerankServiceSchema implements PredefinedCustomServiceSchema {

    @Override
    public List<String> getNonParameterServiceSettings() {
        return List.of("similarity");
    }

    @Override
    public List<String> getServiceSettingsParameters() {
        return List.of("model_id");
    }

    @Override
    public List<String> getServiceSettingsSecretParameters() {
        return List.of("api_key");
    }

    @Override
    public List<String> getTaskSettingsParameters() {
        return List.of("top_n", "return_documents");
        // TODO: Check if return_documents is actually being used in the request?
    }

    // TODO: In CustomService inference, we are not properly overriding task settings for inference requests.
    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public String generateServiceSettings(Map<String, Object> parameters) {
        return """
            {
                "url": "https://api.cohere.com/v2/rerank",
                "headers": {
                    "Authorization": "bearer ${api_key}",
                    "Content-Type": "application/json"
                },
                "request": "{\\"documents\\": ${input}, \\"query\\": ${query}, \\"model\\": ${model_id}, \\"top_n\\": ${top_n}, \\"return_documents\\": ${return_documents}}",
                "response": {
                    "json_parser": {
                        "reranked_index":"$.results[*].index",
                        "relevance_score":"$.results[*].relevance_score"
                    }
                }
            }
            """;
    }
}
