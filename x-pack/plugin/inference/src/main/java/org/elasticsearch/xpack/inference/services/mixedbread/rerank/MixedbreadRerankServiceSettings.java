/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.rerank;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadServiceSettings;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;

/**
 * Represents the settings for a Mixedbread rerank service.
 * This class encapsulates the model ID, URI, and rate limit settings for the Mixedbread rerank service.
 */
public class MixedbreadRerankServiceSettings extends MixedbreadServiceSettings {
    public static final String NAME = "mixedbread_rerank_service_settings";
    private static final URIBuilder DEFAULT_URI_BUILDER = new URIBuilder().setScheme("https")
        .setHost(MixedbreadUtils.HOST)
        .setPathSegments(MixedbreadUtils.VERSION_1, MixedbreadUtils.RERANK_PATH);

    /**
     * Creates a new instance of {@link MixedbreadRerankServiceSettings} from a map of settings.
     *
     * @param map the map containing the service settings
     * @param context the context for parsing configuration settings
     * @return a new instance of {@link MixedbreadRerankServiceSettings}
     */
    public static MixedbreadRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return fromMap(
            map,
            context,
            commonServiceSettings -> new MixedbreadRerankServiceSettings(
                commonServiceSettings.model(),
                commonServiceSettings.uri(),
                commonServiceSettings.rateLimitSettings()
            )
        );
    }

    /**
     * Constructs a new instance of {@link MixedbreadRerankServiceSettings} with the specified model ID, URI, and rate limit settings.
     *
     * @param modelId the model identifier
     * @param uri the URI of the service
     * @param rateLimitSettings the rate limit settings for the service, if null the default will be used
     */
    public MixedbreadRerankServiceSettings(String modelId, @Nullable URI uri, @Nullable RateLimitSettings rateLimitSettings) {
        super(modelId, uri, rateLimitSettings);
    }

    /**
     * Constructs a new instance of {@link MixedbreadRerankServiceSettings} with the specified model identifier and URL.
     * The rate limit settings will be set to the default value.
     *
     * @param modelId the model identifier
     * @param url the URL of the service
     * @param rateLimitSettings the rate limit settings for the service, can be null
     */
    public MixedbreadRerankServiceSettings(String modelId, @Nullable String url, @Nullable RateLimitSettings rateLimitSettings) {
        this(modelId, createOptionalUri(url), rateLimitSettings);
    }

    public MixedbreadRerankServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected URI buildDefaultUri() throws URISyntaxException {
        return DEFAULT_URI_BUILDER.build();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MixedbreadRerankServiceSettings that = (MixedbreadRerankServiceSettings) o;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(uri, that.uri)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, uri, rateLimitSettings);
    }
}
