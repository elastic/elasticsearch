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

public class CohereCompletionServiceSchema implements PredefinedCustomServiceSchema {
    @Override
    public List<String> getNonParameterServiceSettings() {
        return List.of();
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
        return List.of();
    }

    // TODO: Make message into a map with role and content fields.
    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public String generateServiceSettings(Map<String, Object> parameters) {
        return """
            {
                "url": "https://api.cohere.com/v2/chat",
                "headers": {
                    "Authorization": "bearer ${api_key}",
                    "Content-Type": "application/json"
                },
                "request": "{\\"messages\\": [{\\"content\\": ${input}, \\"role\\": \\"user\\"}], \\"model\\": ${model_id}, \\"stream\\": false}",
                "response": {
                    "json_parser":{
                        "completion_result":"$.message.content[*].text"
                      }
                }
            }
            """;
    }
}
