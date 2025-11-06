/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.completion;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.openshiftai.OpenShiftAiServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;

/**
 * Represents the settings for an OpenShift AI chat completion service.
 * This class encapsulates the model ID, URI, and rate limit settings for the OpenShift AI chat completion service.
 */
public class OpenShiftAiChatCompletionServiceSettings extends OpenShiftAiServiceSettings {
    public static final String NAME = "openshift_ai_completion_service_settings";

    /**
     * Creates a new instance of OpenShiftAiChatCompletionServiceSettings from a map of settings.
     *
     * @param map the map containing the service settings
     * @param context the context for parsing configuration settings
     * @return a new instance of OpenShiftAiChatCompletionServiceSettings
     * @throws ValidationException if required fields are missing or invalid
     */
    public static OpenShiftAiChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return fromMap(
            map,
            context,
            commonServiceSettings -> new OpenShiftAiChatCompletionServiceSettings(
                commonServiceSettings.model(),
                commonServiceSettings.uri(),
                commonServiceSettings.rateLimitSettings()
            )
        );
    }

    /**
     * Constructs a new OpenShiftAiChatCompletionServiceSettings from a StreamInput.
     *
     * @param in the StreamInput to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public OpenShiftAiChatCompletionServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Constructs a new OpenShiftAiChatCompletionServiceSettings.
     *
     * @param modelId the ID of the model
     * @param uri the URI of the service
     * @param rateLimitSettings the rate limit settings for the service
     */
    public OpenShiftAiChatCompletionServiceSettings(@Nullable String modelId, URI uri, @Nullable RateLimitSettings rateLimitSettings) {
        super(modelId, uri, rateLimitSettings);
    }

    /**
     * Constructs a new OpenShiftAiChatCompletionServiceSettings.
     *
     * @param modelId the ID of the model
     * @param url the URL of the OpenShift AI service
     * @param rateLimitSettings the rate limit settings for the service
     */
    public OpenShiftAiChatCompletionServiceSettings(@Nullable String modelId, String url, @Nullable RateLimitSettings rateLimitSettings) {
        super(modelId, createUri(url), rateLimitSettings);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OpenShiftAiChatCompletionServiceSettings that = (OpenShiftAiChatCompletionServiceSettings) o;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(uri, that.uri)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, uri, rateLimitSettings);
    }
}
