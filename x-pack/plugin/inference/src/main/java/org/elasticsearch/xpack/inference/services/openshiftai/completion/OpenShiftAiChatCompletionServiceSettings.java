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
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.openshiftai.OpenShiftAiService;
import org.elasticsearch.xpack.inference.services.openshiftai.OpenShiftAiServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractUri;

/**
 * Represents the settings for a OpenShift AI chat completion service.
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
        ValidationException validationException = new ValidationException();

        var model = extractOptionalString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var uri = extractUri(map, URL, validationException);
        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            OpenShiftAiService.NAME,
            context
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new OpenShiftAiChatCompletionServiceSettings(model, uri, rateLimitSettings);
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
     * @param modelId the ID of the model ID
     * @param uri the URI of the service
     * @param rateLimitSettings the rate limit settings for the service
     */
    public OpenShiftAiChatCompletionServiceSettings(@Nullable String modelId, URI uri, @Nullable RateLimitSettings rateLimitSettings) {
        super(modelId, uri, rateLimitSettings);
    }

    /**
     * Constructs a new OpenShiftAiChatCompletionServiceSettings.
     *
     * @param modelId the ID of the model ID
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
