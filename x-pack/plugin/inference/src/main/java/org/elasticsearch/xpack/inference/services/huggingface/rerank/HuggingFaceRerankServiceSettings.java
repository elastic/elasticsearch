/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceService;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceServiceSettings.extractUri;

public class HuggingFaceRerankServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        HuggingFaceRateLimitServiceSettings {

    public static final String NAME = "hugging_face_rerank_service_settings";
    public static final String URL = "url";
    private static final int RERANK_TOKEN_LIMIT = 512;
    // At the time of writing HuggingFace hasn't posted the default rate limit for inference endpoints so the value his is only a guess
    // 3000 requests per minute
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(3000);

    public static HuggingFaceRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();
        var uri = extractUri(map, URL, validationException);
        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            HuggingFaceService.NAME,
            context
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }
        return new HuggingFaceRerankServiceSettings(uri, rateLimitSettings);
    }

    private final URI uri;
    private final RateLimitSettings rateLimitSettings;

    public HuggingFaceRerankServiceSettings(String url) {
        uri = createUri(url);
        rateLimitSettings = DEFAULT_RATE_LIMIT_SETTINGS;
    }

    // default for testing
    HuggingFaceRerankServiceSettings(URI uri, @Nullable RateLimitSettings rateLimitSettings) {
        this.uri = Objects.requireNonNull(uri);
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public HuggingFaceRerankServiceSettings(StreamInput in) throws IOException {
        uri = createUri(in.readString());

        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            rateLimitSettings = new RateLimitSettings(in);
        } else {
            rateLimitSettings = DEFAULT_RATE_LIMIT_SETTINGS;
        }
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public URI uri() {
        return uri;
    }

    public int maxInputTokens() {
        return RERANK_TOKEN_LIMIT;
    }

    // Sometimes the model is not defined in the service settings.
    // This can happen for external providers (e.g. hugging face, azure ai studio)
    // where the provider requires that the model be chosen when initializing a deployment within their service.
    // In this situation, return null.
    @Override
    public String modelId() {
        return null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragmentOfExposedFields(builder, params);
        builder.endObject();

        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(URL, uri.toString());
        builder.field(MAX_INPUT_TOKENS, RERANK_TOKEN_LIMIT);
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_12_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(uri.toString());

        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            rateLimitSettings.writeTo(out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HuggingFaceRerankServiceSettings that = (HuggingFaceRerankServiceSettings) o;
        return Objects.equals(uri, that.uri) && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, rateLimitSettings);
    }
}
