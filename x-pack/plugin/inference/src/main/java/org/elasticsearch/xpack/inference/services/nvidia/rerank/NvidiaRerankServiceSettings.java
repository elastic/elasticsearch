/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.rerank;

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
 * Represents the settings for an Nvidia rerank service.
 * This class encapsulates the model ID, URI, and rate limit settings for the Nvidia rerank service.
 */
public class NvidiaRerankServiceSettings extends NvidiaServiceSettings {
    public static final String NAME = "nvidia_rerank_service_settings";
    private static final URIBuilder DEFAULT_URI_BUILDER = new URIBuilder().setScheme("https")
        .setHost(NvidiaUtils.RERANK_HOST)
        .setPathSegments(NvidiaUtils.VERSION_1, NvidiaUtils.RETRIEVAL_PATH, NvidiaUtils.NVIDIA_PATH, NvidiaUtils.RERANKING_PATH);

    /**
     * Creates a new instance of {@link NvidiaRerankServiceSettings} from a map of settings.
     *
     * @param map the map containing the service settings
     * @param context the context for parsing configuration settings
     * @return a new instance of {@link NvidiaRerankServiceSettings}
     * @throws ValidationException if required fields are missing or invalid
     */
    public static NvidiaRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return fromMap(
            map,
            context,
            commonServiceSettings -> new NvidiaRerankServiceSettings(
                commonServiceSettings.model(),
                commonServiceSettings.uri(),
                commonServiceSettings.rateLimitSettings()
            )
        );
    }

    /**
     * Constructs a new instance of {@link NvidiaRerankServiceSettings} from a {@link StreamInput}.
     *
     * @param in the StreamInput to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public NvidiaRerankServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Constructs a new instance of {@link NvidiaRerankServiceSettings} with the specified model ID, URI, and rate limit settings.
     *
     * @param modelId the model identifier
     * @param uri the URI of the service
     * @param rateLimitSettings the rate limit settings for the service, if null the default will be used
     */
    public NvidiaRerankServiceSettings(String modelId, @Nullable URI uri, @Nullable RateLimitSettings rateLimitSettings) {
        super(modelId, uri, rateLimitSettings);
    }

    @Override
    protected URI buildDefaultUri() throws URISyntaxException {
        return DEFAULT_URI_BUILDER.build();
    }

    /**
     * Constructs a new instance of {@link NvidiaRerankServiceSettings} with the specified model identifier and URL.
     * The rate limit settings will be set to the default value.
     *
     * @param modelId the model identifier
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
