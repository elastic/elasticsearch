/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.rerank;

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
 * Represents the settings for an OpenShift AI rerank service.
 * This class encapsulates the model ID, URI, and rate limit settings for the OpenShift AI rerank service.
 */
public class OpenShiftAiRerankServiceSettings extends OpenShiftAiServiceSettings {
    public static final String NAME = "openshift_ai_rerank_service_settings";

    /**
     * Creates a new instance of OpenShiftAiRerankServiceSettings from a map of settings.
     *
     * @param map the map containing the service settings
     * @param context the context for parsing configuration settings
     * @return a new instance of OpenShiftAiRerankServiceSettings
     * @throws ValidationException if required fields are missing or invalid
     */
    public static OpenShiftAiRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return fromMap(
            map,
            context,
            commonServiceSettings -> new OpenShiftAiRerankServiceSettings(
                commonServiceSettings.model(),
                commonServiceSettings.uri(),
                commonServiceSettings.rateLimitSettings()
            )
        );
    }

    /**
     * Constructs a new OpenShiftAiRerankServiceSettings from a StreamInput.
     *
     * @param in the StreamInput to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public OpenShiftAiRerankServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Constructs a new OpenShiftAiRerankServiceSettings with the specified model ID, URI, and rate limit settings.
     *
     * @param modelId the ID of the model
     * @param uri the URI of the service
     * @param rateLimitSettings the rate limit settings for the service
     */
    public OpenShiftAiRerankServiceSettings(@Nullable String modelId, URI uri, @Nullable RateLimitSettings rateLimitSettings) {
        super(modelId, uri, rateLimitSettings);
    }

    /**
     * Constructs a new OpenShiftAiRerankServiceSettings with the specified model ID and URL.
     * The rate limit settings will be set to the default value.
     *
     * @param modelId the ID of the model
     * @param url the URL of the service
     */
    public OpenShiftAiRerankServiceSettings(@Nullable String modelId, String url, @Nullable RateLimitSettings rateLimitSettings) {
        this(modelId, createUri(url), rateLimitSettings);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OpenShiftAiRerankServiceSettings that = (OpenShiftAiRerankServiceSettings) o;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(uri, that.uri)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, uri, rateLimitSettings);
    }
}
