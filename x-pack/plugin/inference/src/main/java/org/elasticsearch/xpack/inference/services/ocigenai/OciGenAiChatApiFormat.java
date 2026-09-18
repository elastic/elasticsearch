/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

import java.util.Locale;

/**
 * The request/response format of the OCI Generative AI {@code chat} action. Cohere Command models use the {@code COHERE} format,
 * every other model family hosted by OCI Generative AI (Meta Llama, OpenAI, xAI Grok, Google Gemini, ...) uses the {@code GENERIC}
 * format. The format is selected from the model id prefix.
 */
public enum OciGenAiChatApiFormat {
    GENERIC,
    COHERE;

    private static final String COHERE_MODEL_PREFIX = "cohere.";

    public static OciGenAiChatApiFormat fromModelId(String modelId) {
        if (modelId != null && modelId.toLowerCase(Locale.ROOT).startsWith(COHERE_MODEL_PREFIX)) {
            return COHERE;
        }
        return GENERIC;
    }
}
