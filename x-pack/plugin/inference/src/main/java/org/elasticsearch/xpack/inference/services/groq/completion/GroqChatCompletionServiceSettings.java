/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq.completion;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.groq.GroqServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Represents the settings for a Groq chat completion service.
 * This class encapsulates the model ID, URI, and rate limit settings for the Groq chat completion service.
 */
public class GroqChatCompletionServiceSettings extends GroqServiceSettings {
    public static final String NAME = "groq_chat_completion_service_settings";

    /**
     * Creates a new instance of {@link GroqChatCompletionServiceSettings} from a map of settings.
     *
     * @param map the map containing the service settings
     * @param context the context for parsing configuration settings
     * @return a new instance of {@link GroqChatCompletionServiceSettings}
     * @throws ValidationException if required fields are missing or invalid
     */
    public static GroqChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return fromMap(
            map,
            context,
            commonServiceSettings -> new GroqChatCompletionServiceSettings(
                commonServiceSettings.model(),
                commonServiceSettings.rateLimitSettings()
            )
        );
    }

    /**
     * Constructs a new instance of {@link GroqChatCompletionServiceSettings} from a {@link StreamInput}.
     *
     * @param in the {@link StreamInput} to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public GroqChatCompletionServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Constructs a new instance of {@link GroqChatCompletionServiceSettings} with the specified model ID and rate limit settings.
     *
     * @param modelId the model identifier
     * @param rateLimitSettings the rate limit settings for the service
     */
    public GroqChatCompletionServiceSettings(String modelId, @Nullable RateLimitSettings rateLimitSettings) {
        super(modelId, rateLimitSettings);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GroqChatCompletionServiceSettings that = (GroqChatCompletionServiceSettings) o;
        return Objects.equals(modelId, that.modelId) && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, rateLimitSettings);
    }
}
