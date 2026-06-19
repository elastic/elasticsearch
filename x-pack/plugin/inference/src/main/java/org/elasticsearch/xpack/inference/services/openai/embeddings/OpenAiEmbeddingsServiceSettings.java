/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.openai.OpenAiOAuth2Settings;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2Settings.WAIT_FOR_UPGRADE_TO_COMPLETE_ERROR_MESSAGE;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields.ORGANIZATION;

/**
 * Defines the service settings for interacting with OpenAI's text embedding models.
 */
public class OpenAiEmbeddingsServiceSettings extends OpenAiServiceSettings {

    public static final String NAME = "openai_service_settings";

    // The rate limit for usage tier 1 is 3000 request per minute for the text embedding models
    // To find this information you need to access your account's limits https://platform.openai.com/account/limits
    // 3000 requests per minute
    public static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(3000);

    public static OpenAiEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return switch (context) {
            case REQUEST -> fromRequestMap(map);
            case PERSISTENT -> fromPersistentMap(map);
        };
    }

    private static OpenAiEmbeddingsServiceSettings fromPersistentMap(Map<String, Object> map) {
        // Reading previously persisted config, assume the validation
        // passed at that time and never throw.
        var validationException = new ValidationException();

        var commonEmbeddingFields = fromMap(map, validationException, ConfigurationParseContext.PERSISTENT);

        var dimensionsSetByUser = extractOptionalBoolean(map, ServiceFields.DIMENSIONS_SET_BY_USER, validationException);
        if (dimensionsSetByUser == null) {
            // Setting added in 8.13, default to false for configs created prior
            dimensionsSetByUser = Boolean.FALSE;
        }

        return new OpenAiEmbeddingsServiceSettings(commonEmbeddingFields, dimensionsSetByUser);
    }

    private static OpenAiEmbeddingsServiceSettings fromRequestMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        var commonEmbeddingFields = fromMap(map, validationException, ConfigurationParseContext.REQUEST);

        validationException.throwIfValidationErrorsExist();

        return new OpenAiEmbeddingsServiceSettings(commonEmbeddingFields, commonEmbeddingFields.dimensions != null);
    }

    private static CommonEmbeddingFields fromMap(
        Map<String, Object> map,
        ValidationException validationException,
        ConfigurationParseContext context
    ) {
        var uri = extractOptionalUri(map, URL, validationException);
        var organizationId = extractOptionalString(map, ORGANIZATION, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var maxInputTokens = extractOptionalPositiveInteger(
            map,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var dimensions = extractOptionalPositiveInteger(map, DIMENSIONS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var modelId = extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var rateLimitSettings = RateLimitSettings.of(map, DEFAULT_RATE_LIMIT_SETTINGS, validationException, context);
        var commonSettings = parseCommonSettings(map, validationException);

        return new CommonEmbeddingFields(
            modelId,
            uri,
            organizationId,
            similarity,
            maxInputTokens,
            dimensions,
            rateLimitSettings,
            commonSettings.oAuth2Settings()
        );
    }

    @Override
    public OpenAiEmbeddingsServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();

        var extractedOrganizationId = extractOptionalString(
            serviceSettings,
            ORGANIZATION,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var extractedMaxInputTokens = extractOptionalPositiveInteger(
            serviceSettings,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var extractedRateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            this.rateLimitSettings,
            validationException,
            ConfigurationParseContext.REQUEST
        );

        var commonSettings = updateCommonSettings(serviceSettings, validationException);

        validationException.throwIfValidationErrorsExist();

        return new OpenAiEmbeddingsServiceSettings(
            this.modelId,
            this.uri,
            extractedOrganizationId != null ? extractedOrganizationId : this.organizationId,
            this.similarity,
            this.dimensions,
            extractedMaxInputTokens != null ? extractedMaxInputTokens : this.maxInputTokens,
            this.dimensionsSetByUser,
            extractedRateLimitSettings,
            commonSettings.oAuth2Settings()
        );
    }

    private record CommonEmbeddingFields(
        String modelId,
        @Nullable URI uri,
        @Nullable String organizationId,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer maxInputTokens,
        @Nullable Integer dimensions,
        RateLimitSettings rateLimitSettings,
        @Nullable OpenAiOAuth2Settings oAuth2Settings
    ) {}

    private final String modelId;
    private final URI uri;
    private final String organizationId;
    private final SimilarityMeasure similarity;
    private final Integer dimensions;
    private final Integer maxInputTokens;
    private final boolean dimensionsSetByUser;
    private final RateLimitSettings rateLimitSettings;

    public OpenAiEmbeddingsServiceSettings(
        String modelId,
        @Nullable URI uri,
        @Nullable String organizationId,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        boolean dimensionsSetByUser,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this(modelId, uri, organizationId, similarity, dimensions, maxInputTokens, dimensionsSetByUser, rateLimitSettings, null);
    }

    public OpenAiEmbeddingsServiceSettings(
        String modelId,
        @Nullable URI uri,
        @Nullable String organizationId,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        boolean dimensionsSetByUser,
        @Nullable RateLimitSettings rateLimitSettings,
        @Nullable OpenAiOAuth2Settings oAuth2Settings
    ) {
        this(uri, organizationId, similarity, dimensions, maxInputTokens, dimensionsSetByUser, modelId, rateLimitSettings, oAuth2Settings);
    }

    OpenAiEmbeddingsServiceSettings(
        String modelId,
        @Nullable String uri,
        @Nullable String organizationId,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        boolean dimensionsSetByUser,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this(
            modelId,
            createOptionalUri(uri),
            organizationId,
            similarity,
            dimensions,
            maxInputTokens,
            dimensionsSetByUser,
            rateLimitSettings,
            null
        );
    }

    public OpenAiEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this(
            createOptionalUri(in.readOptionalString()),
            in.readOptionalString(),
            in.readOptionalEnum(SimilarityMeasure.class),
            in.readOptionalVInt(),
            in.readOptionalVInt(),
            in.readBoolean(),
            in.readString(),
            new RateLimitSettings(in),
            in.getTransportVersion().supports(OpenAiOAuth2Settings.OPENAI_OAUTH2_SETTINGS)
                ? in.readOptionalWriteable(OpenAiOAuth2Settings::new)
                : null
        );
    }

    private OpenAiEmbeddingsServiceSettings(CommonEmbeddingFields fields, boolean dimensionsSetByUser) {
        this(
            fields.uri,
            fields.organizationId,
            fields.similarity,
            fields.dimensions,
            fields.maxInputTokens,
            dimensionsSetByUser,
            fields.modelId,
            fields.rateLimitSettings,
            fields.oAuth2Settings
        );
    }

    /**
     * Single field-assigning constructor. Parameter order mirrors the wire format (uri first, modelId after
     * dimensionsSetByUser) so that the {@link StreamInput} constructor can delegate directly.
     */
    private OpenAiEmbeddingsServiceSettings(
        @Nullable URI uri,
        @Nullable String organizationId,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        boolean dimensionsSetByUser,
        String modelId,
        RateLimitSettings rateLimitSettings,
        @Nullable OpenAiOAuth2Settings oAuth2Settings
    ) {
        super(oAuth2Settings);
        this.uri = uri;
        this.organizationId = organizationId;
        this.similarity = similarity;
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
        this.dimensionsSetByUser = dimensionsSetByUser;
        this.modelId = Objects.requireNonNull(modelId);
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
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
    public String organizationId() {
        return organizationId;
    }

    @Override
    public SimilarityMeasure similarity() {
        return similarity;
    }

    @Override
    public Integer dimensions() {
        return dimensions;
    }

    @Override
    public Boolean dimensionsSetByUser() {
        return dimensionsSetByUser;
    }

    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public String modelId() {
        return modelId;
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return DenseVectorFieldMapper.ElementType.FLOAT;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragmentOfExposedFields(builder, params);
        builder.field(ServiceFields.DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        super.toXContentFragmentOfExposedFields(builder, params);
        builder.field(MODEL_ID, modelId);
        if (uri != null) {
            builder.field(URL, uri.toString());
        }
        if (organizationId != null) {
            builder.field(ORGANIZATION, organizationId);
        }
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        var uriToWrite = uri != null ? uri.toString() : null;
        out.writeOptionalString(uriToWrite);
        out.writeOptionalString(organizationId);
        out.writeOptionalEnum(similarity);
        out.writeOptionalVInt(dimensions);
        out.writeOptionalVInt(maxInputTokens);
        out.writeBoolean(dimensionsSetByUser);
        out.writeString(modelId);
        rateLimitSettings.writeTo(out);
        if (out.getTransportVersion().supports(OpenAiOAuth2Settings.OPENAI_OAUTH2_SETTINGS)) {
            out.writeOptionalWriteable(oAuth2Settings);
        } else if (oAuth2Settings != null) {
            throw new ElasticsearchStatusException(WAIT_FOR_UPGRADE_TO_COMPLETE_ERROR_MESSAGE, RestStatus.BAD_REQUEST);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (super.equals(o) == false) return false;
        OpenAiEmbeddingsServiceSettings that = (OpenAiEmbeddingsServiceSettings) o;
        return Objects.equals(uri, that.uri)
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(organizationId, that.organizationId)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(dimensionsSetByUser, that.dimensionsSetByUser)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            uri,
            modelId,
            organizationId,
            similarity,
            dimensions,
            maxInputTokens,
            dimensionsSetByUser,
            rateLimitSettings
        );
    }
}
