/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.convertToUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;

public class JinaAIServiceSettings extends FilteredXContentObject implements ServiceSettings, JinaAIRateLimitServiceSettings {

    public static final String NAME = "jinaai_service_settings";
    public static final String MODEL_ID = "model_id";
    private static final Logger logger = LogManager.getLogger(JinaAIServiceSettings.class);
    // See https://jina.ai/contact-sales/#rate-limit
    public static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(2_000);

    public static JinaAIServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        String url = extractOptionalString(map, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        URI uri = convertToUri(url, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            JinaAIService.NAME,
            context
        );

        String modelId = extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new JinaAIServiceSettings(uri, modelId, rateLimitSettings);
    }

    private final URI uri;
    private final String modelId;
    private final RateLimitSettings rateLimitSettings;

    public JinaAIServiceSettings(@Nullable URI uri, String modelId, @Nullable RateLimitSettings rateLimitSettings) {
        this.uri = uri;
        this.modelId = Objects.requireNonNull(modelId);
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public JinaAIServiceSettings(@Nullable String url, String modelId, @Nullable RateLimitSettings rateLimitSettings) {
        this(createOptionalUri(url), modelId, rateLimitSettings);
    }

    public JinaAIServiceSettings(StreamInput in) throws IOException {
        uri = createOptionalUri(in.readOptionalString());
        modelId = in.readOptionalString();
        rateLimitSettings = new RateLimitSettings(in);
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    public URI uri() {
        return uri;
    }

    @Override
    public String modelId() {
        return modelId;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        toXContentFragment(builder, params);

        builder.endObject();
        return builder;
    }

    public XContentBuilder toXContentFragment(XContentBuilder builder, Params params) throws IOException {
        return toXContentFragmentOfExposedFields(builder, params);
    }

    @Override
    public XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        if (uri != null) {
            builder.field(URL, uri.toString());
        }
        if (modelId != null) {
            builder.field(MODEL_ID, modelId);
        }
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.JINA_AI_INTEGRATION_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        var uriToWrite = uri != null ? uri.toString() : null;
        out.writeOptionalString(uriToWrite);
        out.writeOptionalString(modelId);
        rateLimitSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JinaAIServiceSettings that = (JinaAIServiceSettings) o;
        return Objects.equals(uri, that.uri)
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, modelId, rateLimitSettings);
    }
}
