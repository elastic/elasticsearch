/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.fireworksai.FireworksAiRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.fireworksai.FireworksAiService;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;

/**
 * Defines the service settings for FireworksAI rerank models.
 */
public class FireworksAiRerankServiceSettings extends FilteredXContentObject
    implements
        FireworksAiRateLimitServiceSettings,
        ServiceSettings {

    public static final String NAME = "fireworksai_rerank_service_settings";

    private static final String DEFAULT_URL = "https://api.fireworks.ai/inference/v1/rerank";

    // Default rate limit settings - adjust based on FireworksAI's actual limits
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(1000);

    public static FireworksAiRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        String url = extractOptionalString(map, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String modelId = extractOptionalString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);

        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            FireworksAiService.NAME,
            context
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        URI uri = url != null ? ServiceUtils.createUri(url) : ServiceUtils.createUri(DEFAULT_URL);
        return new FireworksAiRerankServiceSettings(uri, modelId, rateLimitSettings);
    }

    private final URI uri;
    private final String modelId;
    private final RateLimitSettings rateLimitSettings;

    public FireworksAiRerankServiceSettings(URI uri, @Nullable String modelId, @Nullable RateLimitSettings rateLimitSettings) {
        this.uri = Objects.requireNonNull(uri);
        this.modelId = modelId;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public FireworksAiRerankServiceSettings(StreamInput in) throws IOException {
        this.uri = ServiceUtils.createUri(in.readString());
        this.modelId = in.readString();
        this.rateLimitSettings = Objects.requireNonNullElse(in.readOptionalWriteable(RateLimitSettings::new), DEFAULT_RATE_LIMIT_SETTINGS);
    }

    @Override
    public URI uri() {
        return uri;
    }

    public String modelId() {
        return modelId;
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(URL, uri.toString());
        builder.field(MODEL_ID, modelId);

        rateLimitSettings.toXContent(builder, params);

        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(URL, uri.toString());
        builder.field(MODEL_ID, modelId);

        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(uri.toString());
        out.writeString(modelId);
        out.writeOptionalWriteable(rateLimitSettings);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        FireworksAiRerankServiceSettings that = (FireworksAiRerankServiceSettings) object;
        return Objects.equals(uri, that.uri)
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, modelId, rateLimitSettings);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_15_0;
    }
}
