/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceService;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;

public class MixedbreadRerankServiceSettings extends FilteredXContentObject implements ServiceSettings {
    public static final String NAME = "mixedbread_rerank_service_settings";
    // There is no default rate limit for Mixedbread, so we set a reasonable default of 3000 requests per minute
    protected static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(3000);

    /**
     * Creates a new instance of MixedbreadRerankServiceSettings from a map of settings.
     *
     * @param map the map containing the settings
     * @param context the context for parsing configuration settings
     * @return a new instance of MixedbreadRerankServiceSettings
     * @throws ValidationException if any required fields are missing or invalid
     */
    public static MixedbreadRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();
        var uri = HuggingFaceServiceSettings.extractUri(map, URL, validationException);
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
        return new MixedbreadRerankServiceSettings(uri, rateLimitSettings);
    }

    private final URI uri;
    private final RateLimitSettings rateLimitSettings;

    public MixedbreadRerankServiceSettings(String url) {
        uri = createUri(url);
        rateLimitSettings = DEFAULT_RATE_LIMIT_SETTINGS;
    }

    MixedbreadRerankServiceSettings(URI uri, @Nullable RateLimitSettings rateLimitSettings) {
        this.uri = Objects.requireNonNull(uri);
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    /**
     * Constructs a new MixedbreadEmbeddingsServiceSettings from a StreamInput.
     *
     * @param in the StreamInput to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public MixedbreadRerankServiceSettings(StreamInput in) throws IOException {
        this.uri = createUri(in.readString());
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    /**
     * Constructs a new MixedbreadRerankServiceSettings with the specified parameters.
     *
     * @param modelId the identifier for the model
     * @param uri the URI of the Mixedbread service
     * @param rateLimitSettings the rate limit settings for the service, can be null
     */
    public MixedbreadRerankServiceSettings(String modelId, URI uri, @Nullable RateLimitSettings rateLimitSettings) {
        this.uri = uri;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public URI uri() {
        return this.uri;
    }

    /**
     * Returns the rate limit settings for this service.
     *
     * @return the rate limit settings, never null
     */
    public RateLimitSettings rateLimitSettings() {
        return this.rateLimitSettings;
    }

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
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(URL, uri.toString());
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_MIXEDBREAD_ADDED;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return version.onOrAfter(TransportVersions.ML_INFERENCE_HUGGING_FACE_RERANK_ADDED)
            || version.isPatchFrom(TransportVersions.ML_INFERENCE_HUGGING_FACE_RERANK_ADDED_8_19);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(uri.toString());
        rateLimitSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MixedbreadRerankServiceSettings that = (MixedbreadRerankServiceSettings) o;
        return Objects.equals(uri, that.uri) && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, rateLimitSettings);
    }
}
