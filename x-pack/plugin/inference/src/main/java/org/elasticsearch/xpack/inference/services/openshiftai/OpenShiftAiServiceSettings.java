/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai;

import org.elasticsearch.TransportVersion;
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
import java.util.function.Function;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractUri;

/**
 * Represents the settings for an OpenShift AI service.
 * This class encapsulates the model ID, URI, and rate limit settings for the OpenShift AI service.
 */
public abstract class OpenShiftAiServiceSettings extends FilteredXContentObject implements ServiceSettings {
    // There is no default rate limit for OpenShift AI, so we set a reasonable default of 3000 requests per minute
    protected static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(3000);

    protected final String modelId;
    protected final URI uri;
    protected final RateLimitSettings rateLimitSettings;

    /**
     * Constructs a new OpenShiftAiServiceSettings from a StreamInput.
     *
     * @param in the StreamInput to read from
     * @throws IOException if an I/O error occurs during reading
     */
    protected OpenShiftAiServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readOptionalString();
        this.uri = createUri(in.readString());
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    /**
     * Constructs a new OpenShiftAiServiceSettings.
     *
     * @param modelId the ID of the model
     * @param uri the URI of the service
     * @param rateLimitSettings the rate limit settings for the service
     */
    protected OpenShiftAiServiceSettings(@Nullable String modelId, URI uri, @Nullable RateLimitSettings rateLimitSettings) {
        this.modelId = modelId;
        this.uri = Objects.requireNonNull(uri);
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        assert false : "should never be called when supportsVersion is used";
        return OpenShiftAiUtils.ML_INFERENCE_OPENSHIFT_AI_ADDED;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return OpenShiftAiUtils.supportsOpenShiftAi(version);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(modelId);
        out.writeString(uri.toString());
        rateLimitSettings.writeTo(out);
    }

    @Override
    public String modelId() {
        return this.modelId;
    }

    /**
     * Returns the URI of the OpenShift AI service.
     *
     * @return the URI of the service
     */
    public URI uri() {
        return this.uri;
    }

    /**
     * Returns the rate limit settings for the OpenShift AI service.
     *
     * @return the rate limit settings
     */
    public RateLimitSettings rateLimitSettings() {
        return this.rateLimitSettings;
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
        if (modelId != null) {
            builder.field(MODEL_ID, modelId);
        }
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
     * @param <T> the type of {@link OpenShiftAiServiceSettings} to create
     */
    protected static <T extends OpenShiftAiServiceSettings> T fromMap(
        Map<String, Object> map,
        ConfigurationParseContext context,
        Function<OpenShiftAiCommonServiceSettings, T> factory
    ) {
        var validationException = new ValidationException();
        var commonServiceSettings = extractOpenShiftAiCommonServiceSettings(map, context, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return factory.apply(commonServiceSettings);
    }

    /**
     * Extracts common OpenShift AI service settings from the provided map.
     * @param map the map containing the service settings
     * @param context the context for parsing configuration settings
     * @param validationException the validation exception to collect validation errors
     * @return an instance of {@link OpenShiftAiCommonServiceSettings}
     */
    protected static OpenShiftAiCommonServiceSettings extractOpenShiftAiCommonServiceSettings(
        Map<String, Object> map,
        ConfigurationParseContext context,
        ValidationException validationException
    ) {
        var model = extractOptionalString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var uri = extractUri(map, URL, validationException);
        var rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            OpenShiftAiService.NAME,
            context
        );
        return new OpenShiftAiCommonServiceSettings(model, uri, rateLimitSettings);
    }

    protected record OpenShiftAiCommonServiceSettings(String model, URI uri, RateLimitSettings rateLimitSettings) {}
}
