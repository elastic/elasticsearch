/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadService;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.convertToUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;

public class MixedbreadRerankServiceSettings extends FilteredXContentObject implements ServiceSettings, MixedbreadRateLimitServiceSettings {

    public static final String NAME = "mixedbread_ai_rerank_service_settings";

    /**
     * Applied different rate limits based on the type of operation performed:

     * Operation Type	Limit	Burst Capacity	Window
     * Read	    1,200	1,000	1-minute
     * List	    600	    200	    1-minute
     * Write	360	    120	    1-minute
     * Update	480	    160	    1-minute
     * Delete	240	    80	    1-minute
     * <a href="https://www.mixedbread.com/api-reference/rate-limits">Rate Limiting</a>.
     */
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(240);

    public static MixedbreadRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        String url = extractOptionalString(map, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);

        URI uri = convertToUri(url, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String model = extractOptionalString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            MixedbreadService.NAME,
            context
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new MixedbreadRerankServiceSettings(model, rateLimitSettings, uri);
    }

    private final String model;

    private final RateLimitSettings rateLimitSettings;
    private final URI uri;

    public MixedbreadRerankServiceSettings(@Nullable String model, @Nullable RateLimitSettings rateLimitSettings, @Nullable URI uri) {
        this.model = model;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
        this.uri = uri;
    }

    public MixedbreadRerankServiceSettings(StreamInput in) throws IOException {
        this.model = in.readOptionalString();
        this.rateLimitSettings = new RateLimitSettings(in);
        this.uri = createOptionalUri(in.readOptionalString());
    }

    @Override
    public String modelId() {
        return model;
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public URI uri() {
        return uri;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        if (model != null) {
            builder.field(MODEL_ID, model);
        }

        rateLimitSettings.toXContent(builder, params);

        if (uri != null) {
            builder.field(URL, uri.toString());
        }

        return builder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        toXContentFragmentOfExposedFields(builder, params);

        builder.endObject();

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(model);
        rateLimitSettings.writeTo(out);
        var uriToWrite = uri != null ? uri.toString() : null;
        out.writeOptionalString(uriToWrite);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        MixedbreadRerankServiceSettings that = (MixedbreadRerankServiceSettings) object;
        return Objects.equals(model, that.modelId()) && Objects.equals(rateLimitSettings, that.rateLimitSettings());
    }

    @Override
    public int hashCode() {
        return Objects.hash(model, rateLimitSettings);
    }
}
