/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.completion;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.nvidia.NvidiaServiceSettings;
import org.elasticsearch.xpack.inference.services.nvidia.NvidiaUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;

/**
 * Represents the settings for an Nvidia chat completion service.
 * This class encapsulates the model ID, URI, and rate limit settings for the Nvidia chat completion service.
 */
public class NvidiaChatCompletionServiceSettings extends NvidiaServiceSettings {
    public static final String NAME = "nvidia_chat_completion_service_settings";
    private static final URIBuilder DEFAULT_URI_BUILDER = new URIBuilder().setScheme("https")
        .setHost(NvidiaUtils.HOST)
        .setPathSegments(NvidiaUtils.VERSION_1, NvidiaUtils.CHAT_PATH, NvidiaUtils.COMPLETIONS_PATH);

    /**
     * Creates a new instance of {@link NvidiaChatCompletionServiceSettings} from a map of settings.
     *
     * @param map the map containing the service settings
     * @param context the context for parsing configuration settings
     * @return a new instance of {@link NvidiaChatCompletionServiceSettings}
     * @throws ValidationException if required fields are missing or invalid
     */
    public static NvidiaChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return fromMap(
            map,
            context,
            commonServiceSettings -> new NvidiaChatCompletionServiceSettings(
                commonServiceSettings.model(),
                commonServiceSettings.uri(),
                commonServiceSettings.rateLimitSettings()
            )
        );
    }

    /**
     * Constructs a new instance of {@link NvidiaChatCompletionServiceSettings} from a {@link StreamInput}.
     *
     * @param in the {@link StreamInput} to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public NvidiaChatCompletionServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Constructs a new instance of {@link NvidiaChatCompletionServiceSettings} with the specified model ID, URI, and rate limit settings.
     *
     * @param modelId the model identifier
     * @param uri the URI of the service
     * @param rateLimitSettings the rate limit settings for the service
     */
    public NvidiaChatCompletionServiceSettings(String modelId, @Nullable URI uri, @Nullable RateLimitSettings rateLimitSettings) {
        super(modelId, uri, rateLimitSettings);
    }

    @Override
    protected URI buildDefaultUri() throws URISyntaxException {
        return DEFAULT_URI_BUILDER.build();
    }

    /**
     * Constructs a new instance of {@link NvidiaChatCompletionServiceSettings} with the specified model ID and URL.
     *
     * @param modelId the model identifier
     * @param url the URL of the service
     * @param rateLimitSettings the rate limit settings for the service, can be null
     */
    public NvidiaChatCompletionServiceSettings(String modelId, @Nullable String url, @Nullable RateLimitSettings rateLimitSettings) {
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
        NvidiaChatCompletionServiceSettings that = (NvidiaChatCompletionServiceSettings) o;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(uri, that.uri)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, uri, rateLimitSettings);
    }
}
