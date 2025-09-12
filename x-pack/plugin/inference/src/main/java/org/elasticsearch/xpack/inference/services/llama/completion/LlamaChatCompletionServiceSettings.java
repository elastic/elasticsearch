/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.completion;

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
import org.elasticsearch.xpack.inference.services.llama.LlamaService;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractUri;

/**
 * Represents the settings for a Llama chat completion service.
 * This class encapsulates the model ID, URI, and rate limit settings for the Llama chat completion service.
 */
public class LlamaChatCompletionServiceSettings extends FilteredXContentObject implements ServiceSettings {
    public static final String NAME = "llama_completion_service_settings";
    // There is no default rate limit for Llama, so we set a reasonable default of 3000 requests per minute
    protected static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(3000);

    private final String modelId;
    private final URI uri;
    private final RateLimitSettings rateLimitSettings;

    /**
     * Creates a new instance of LlamaChatCompletionServiceSettings from a map of settings.
     *
     * @param map the map containing the service settings
     * @param context the context for parsing configuration settings
     * @return a new instance of LlamaChatCompletionServiceSettings
     * @throws ValidationException if required fields are missing or invalid
     */
    public static LlamaChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        var model = extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var uri = extractUri(map, URL, validationException);
        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            LlamaService.NAME,
            context
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new LlamaChatCompletionServiceSettings(model, uri, rateLimitSettings);
    }

    /**
     * Constructs a new LlamaChatCompletionServiceSettings from a StreamInput.
     *
     * @param in the StreamInput to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public LlamaChatCompletionServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.uri = createUri(in.readString());
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    /**
     * Constructs a new LlamaChatCompletionServiceSettings with the specified model ID, URI, and rate limit settings.
     *
     * @param modelId the ID of the model
     * @param uri the URI of the service
     * @param rateLimitSettings the rate limit settings for the service
     */
    public LlamaChatCompletionServiceSettings(String modelId, URI uri, @Nullable RateLimitSettings rateLimitSettings) {
        this.modelId = modelId;
        this.uri = uri;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    /**
     * Constructs a new LlamaChatCompletionServiceSettings with the specified model ID and URL.
     * The rate limit settings will be set to the default value.
     *
     * @param modelId the ID of the model
     * @param url the URL of the service
     */
    public LlamaChatCompletionServiceSettings(String modelId, String url, @Nullable RateLimitSettings rateLimitSettings) {
        this(modelId, createUri(url), rateLimitSettings);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_LLAMA_ADDED;
    }

    @Override
    public String modelId() {
        return this.modelId;
    }

    /**
     * Returns the URI of the Llama chat completion service.
     *
     * @return the URI of the service
     */
    public URI uri() {
        return this.uri;
    }

    /**
     * Returns the rate limit settings for the Llama chat completion service.
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LlamaChatCompletionServiceSettings that = (LlamaChatCompletionServiceSettings) o;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(uri, that.uri)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, uri, rateLimitSettings);
    }
}
