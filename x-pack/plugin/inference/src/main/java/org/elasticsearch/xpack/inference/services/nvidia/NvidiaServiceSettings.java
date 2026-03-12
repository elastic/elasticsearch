/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;

/**
 * Represents the settings for an Nvidia service.
 * This class encapsulates the model ID, URI, and rate limit settings for the Nvidia service.
 */
public abstract class NvidiaServiceSettings extends FilteredXContentObject implements ServiceSettings {
    // There is no default rate limit for Nvidia, so we set a reasonable default of 3000 requests per minute
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(3000);

    protected final String modelId;
    protected final URI uri;
    protected final RateLimitSettings rateLimitSettings;

    /**
     * Constructs new {@link NvidiaServiceSettings} from a {@link StreamInput}.
     *
     * @param in the {@link StreamInput} to read from
     * @throws IOException if an I/O error occurs during reading
     */
    protected NvidiaServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.uri = ServiceUtils.createUri(in.readString());
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    protected NvidiaServiceSettings(String modelId, @Nullable URI uri, @Nullable RateLimitSettings rateLimitSettings) {
        this.modelId = Objects.requireNonNull(modelId);
        this.uri = buildUri(uri, NvidiaService.NAME, this::buildDefaultUri);
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    protected abstract URI buildDefaultUri() throws URISyntaxException;

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        assert false : "should never be called when supportsVersion is used";
        return NvidiaUtils.ML_INFERENCE_NVIDIA_ADDED;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return NvidiaUtils.supportsNvidia(version);
    }

    @Override
    public String modelId() {
        return this.modelId;
    }

    /**
     * Returns the URI of the Nvidia service.
     *
     * @return the URI of the service
     */
    public URI uri() {
        return this.uri;
    }

    /**
     * Returns the rate limit settings for the Nvidia service.
     *
     * @return the rate limit settings
     */
    public RateLimitSettings rateLimitSettings() {
        return this.rateLimitSettings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeString(uri.toString());
        rateLimitSettings.writeTo(out);
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
        builder.field(MODEL_ID, modelId);
        builder.field(URL, uri.toString());
        rateLimitSettings.toXContent(builder, params);
        return builder;
    }

    /**
     * Creates an instance of T from the provided map using the given factory function.
     * @param map the map containing the service settings
     * @param context the context for parsing configuration settings
     * @param factory the factory function to create an instance of T
     * @return an instance of T
     * @param <T> the type of {@link NvidiaServiceSettings} to create
     */
    protected static <T extends NvidiaServiceSettings> T fromMap(
        Map<String, Object> map,
        ConfigurationParseContext context,
        Function<NvidiaCommonServiceSettings, T> factory
    ) {
        var validationException = new ValidationException();
        var commonServiceSettings = extractNvidiaCommonServiceSettings(map, context, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return factory.apply(commonServiceSettings);
    }

    /**
     * Extracts common Nvidia service settings from the provided map.
     * @param map the map containing the service settings
     * @param context the context for parsing configuration settings
     * @param validationException the validation exception to collect validation errors
     * @return an instance of {@link NvidiaCommonServiceSettings}
     */
    protected static NvidiaCommonServiceSettings extractNvidiaCommonServiceSettings(
        Map<String, Object> map,
        ConfigurationParseContext context,
        ValidationException validationException
    ) {
        var model = ServiceUtils.extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var uri = ServiceUtils.extractOptionalUri(map, URL, validationException);
        var rateLimitSettings = RateLimitSettings.of(map, DEFAULT_RATE_LIMIT_SETTINGS, validationException, NvidiaService.NAME, context);
        return new NvidiaCommonServiceSettings(model, uri, rateLimitSettings);
    }

    protected record NvidiaCommonServiceSettings(String model, @Nullable URI uri, RateLimitSettings rateLimitSettings) {}

}
