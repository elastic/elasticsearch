/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadService;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings.IbmWatsonxEmbeddingsServiceSettings.extractUri;

public class MixedbreadEmbeddingsServiceSettings extends FilteredXContentObject implements ServiceSettings {
    public static final String NAME = "mixedbread_embeddings_service_settings";
    // There is no default rate limit for Mixedbread, so we set a reasonable default of 3000 requests per minute
    protected static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(3000);

    private final String modelId;
    private final URI uri;
    private final Integer dimensions;
    private final RateLimitSettings rateLimitSettings;

    /**
     * Creates a new instance of MixedbreadEmbeddingsServiceSettings from a map of settings.
     *
     * @param map the map containing the settings
     * @param context the context for parsing configuration settings
     * @return a new instance of MixedbreadEmbeddingsServiceSettings
     * @throws ValidationException if any required fields are missing or invalid
     */
    public static MixedbreadEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        var model = extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var uri = extractUri(map, URL, validationException);
        var dimensions = extractOptionalPositiveInteger(map, DIMENSIONS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            MixedbreadService.NAME,
            context
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new MixedbreadEmbeddingsServiceSettings(model, uri, dimensions, rateLimitSettings);
    }

    /**
     * Constructs a new MixedbreadEmbeddingsServiceSettings from a StreamInput.
     *
     * @param in the StreamInput to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public MixedbreadEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.uri = createUri(in.readString());
        this.dimensions = in.readOptionalVInt();
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    /**
     * Constructs a new MixedbreadEmbeddingsServiceSettings with the specified parameters.
     *
     * @param modelId the identifier for the model
     * @param uri the URI of the Mixedbread service
     * @param dimensions the number of dimensions for the embeddings, can be null
     * @param rateLimitSettings the rate limit settings for the service, can be null
     */
    public MixedbreadEmbeddingsServiceSettings(
        String modelId,
        URI uri,
        @Nullable Integer dimensions,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.modelId = modelId;
        this.uri = uri;
        this.dimensions = dimensions;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    /**
     * Constructs a new MixedbreadEmbeddingsServiceSettings with the specified parameters.
     *
     * @param modelId the identifier for the model
     * @param url the URL of the Mixedbread service
     * @param dimensions the number of dimensions for the embeddings, can be null
     * @param rateLimitSettings the rate limit settings for the service, can be null
     */
    public MixedbreadEmbeddingsServiceSettings(
        String modelId,
        String url,
        @Nullable Integer dimensions,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this(modelId, createUri(url), dimensions, rateLimitSettings);
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
    public String modelId() {
        return this.modelId;
    }

    public URI uri() {
        return this.uri;
    }

    @Override
    public Integer dimensions() {
        return this.dimensions;
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return DenseVectorFieldMapper.ElementType.FLOAT;
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
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeString(uri.toString());
        out.writeOptionalVInt(dimensions);
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
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(MODEL_ID, modelId);
        builder.field(URL, uri.toString());

        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MixedbreadEmbeddingsServiceSettings that = (MixedbreadEmbeddingsServiceSettings) o;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(uri, that.uri)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, uri, dimensions, rateLimitSettings);
    }

}
