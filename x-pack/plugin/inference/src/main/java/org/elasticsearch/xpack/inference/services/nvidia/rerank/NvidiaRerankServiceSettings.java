/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.rerank;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.nvidia.NvidiaService;
import org.elasticsearch.xpack.inference.services.nvidia.NvidiaServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;

/**
 * Represents the settings for an Nvidia rerank service.
 * This class encapsulates the model ID, URI, and rate limit settings for the Nvidia rerank service.
 */
public class NvidiaRerankServiceSettings extends NvidiaServiceSettings {
    public static final String NAME = "nvidia_rerank_service_settings";

    /**
     * Creates a new instance of NvidiaRerankServiceSettings from a map of settings.
     *
     * @param map the map containing the service settings
     * @param context the context for parsing configuration settings
     * @return a new instance of NvidiaRerankServiceSettings
     * @throws ValidationException if required fields are missing or invalid
     */
    public static NvidiaRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        var model = extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var uri = extractOptionalUri(map, URL, validationException);
        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            NvidiaService.NAME,
            context
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new NvidiaRerankServiceSettings(model, uri, rateLimitSettings);
    }

    /**
     * Constructs a new NvidiaRerankServiceSettings from a StreamInput.
     *
     * @param in the StreamInput to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public NvidiaRerankServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Constructs a new NvidiaRerankServiceSettings with the specified model ID, URI, and rate limit settings.
     *
     * @param modelId the ID of the model
     * @param uri the URI of the service
     * @param rateLimitSettings the rate limit settings for the service
     */
    public NvidiaRerankServiceSettings(String modelId, @Nullable URI uri, @Nullable RateLimitSettings rateLimitSettings) {
        super(modelId, uri, rateLimitSettings);
    }

    /**
     * Constructs a new NvidiaRerankServiceSettings with the specified model ID and URL.
     * The rate limit settings will be set to the default value.
     *
     * @param modelId the ID of the model
     * @param url the URL of the service
     * @param rateLimitSettings the rate limit settings for the service, can be null
     */
    public NvidiaRerankServiceSettings(String modelId, @Nullable String url, @Nullable RateLimitSettings rateLimitSettings) {
        this(modelId, createOptionalUri(url), rateLimitSettings);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NvidiaRerankServiceSettings that = (NvidiaRerankServiceSettings) o;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(uri, that.uri)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, uri, rateLimitSettings);
    }
}
