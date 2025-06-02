/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.custom.PredefinedCustomServiceSchema;

import java.util.EnumSet;
import java.util.List;

public class CohereServiceSchema implements PredefinedCustomServiceSchema {
    public static final String NAME = "cohere";

    // TODO: Find a way to handle multiple task types. This may mean one schema file per service with a map of task type to schema JSON
    // or it may be easier to have one task type per schema file.

    @Override
    public String getName() {
        return NAME;
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public String getSchema() {
        return """
            {
                "url": "https://api.cohere.com/v2/embed",
                "headers": {
                    "Authorization": "bearer ${api_key}",
                    "Content-Type": "application/json"
                },
                "request": "{\\"texts\\": ${input}, \\"model\\": ${model_id}, \\"input_type\\": ${input_type}, \\"embedding_types\\": [\\"binary\\"]}",
                "response": {
                    "json_parser": {
                        "text_embeddings":"$.embeddings.binary[*]",
                        "embedding_type": "binary"
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
            """;
    }

    @Override
    public List<String> getParameters() {
        return List.of("model_id");
    }

    @Override
    public List<String> getSecretParameters() {
        return List.of("api_key");
    }

    @Override
    public EnumSet<TaskType> getSupportedTaskTypes() {
        return EnumSet.of(TaskType.TEXT_EMBEDDING);
    }
}
