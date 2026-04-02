/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.embeddings;

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
import org.elasticsearch.xpack.core.inference.InferenceUtils;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiOAuth2Settings;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiOAuth2Settings.AZURE_OPENAI_OAUTH_SETTINGS;

/**
 * Defines the service settings for interacting with OpenAI's text embedding models.
 */
public class AzureOpenAiEmbeddingsServiceSettings extends AzureOpenAiServiceSettings {

    public static final String NAME = "azure_openai_embeddings_service_settings";

    /**
     * Rate limit documentation can be found here:
     * Limits per region per model id
     * https://learn.microsoft.com/en-us/azure/ai-services/openai/quotas-limits
     * <p>
     * How to change the limits
     * https://learn.microsoft.com/en-us/azure/ai-services/openai/how-to/quota?tabs=rest
     * <p>
     * Blog giving some examples
     * https://techcommunity.microsoft.com/t5/fasttrack-for-azure/optimizing-azure-openai-a-guide-to-limits-quotas-and-best/ba-p/4076268
     * <p>
     * According to the docs 1000 tokens per minute (TPM) = 6 requests per minute (RPM). The limits change depending on the region
     * and model. The lowest text embedding limit is 240K TPM, so we'll default to that.
     * Calculation: 240K TPM = 240 * 6 = 1440 requests per minute (used `eastus` and `Text-Embedding-Ada-002` as basis for the calculation).
     */
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(1_440);

    private final Integer dimensions;
    private final boolean dimensionsSetByUser;
    private final Integer maxInputTokens;
    private final SimilarityMeasure similarity;

    public static AzureOpenAiEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        var commonSettings = parseCommonSettings(map, validationException, context, DEFAULT_RATE_LIMIT_SETTINGS);
        var dimensions = extractOptionalPositiveInteger(map, DIMENSIONS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var maxInputTokens = extractOptionalPositiveInteger(
            map,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var dimensionsSetByUser = extractOptionalBoolean(map, ServiceFields.DIMENSIONS_SET_BY_USER, validationException);

        switch (context) {
            case REQUEST -> {
                if (dimensionsSetByUser != null) {
                    validationException.addValidationError(
                        ServiceUtils.invalidSettingError(ServiceFields.DIMENSIONS_SET_BY_USER, ModelConfigurations.SERVICE_SETTINGS)
                    );
                }
                dimensionsSetByUser = dimensions != null;
            }
            case PERSISTENT -> {
                if (dimensionsSetByUser == null) {
                    validationException.addValidationError(
                        InferenceUtils.missingSettingErrorMsg(ServiceFields.DIMENSIONS_SET_BY_USER, ModelConfigurations.SERVICE_SETTINGS)
                    );
                }
            }
        }

        validationException.throwIfValidationErrorsExist();

        return new AzureOpenAiEmbeddingsServiceSettings(
            commonSettings,
            dimensions,
            Boolean.TRUE.equals(dimensionsSetByUser),
            maxInputTokens,
            similarity
        );
    }

    public AzureOpenAiEmbeddingsServiceSettings(
        String resourceName,
        String deploymentId,
        String apiVersion,
        @Nullable Integer dimensions,
        boolean dimensionsSetByUser,
        @Nullable Integer maxInputTokens,
        @Nullable SimilarityMeasure similarity,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this(resourceName, deploymentId, apiVersion, dimensions, dimensionsSetByUser, maxInputTokens, similarity, rateLimitSettings, null);
    }

    public AzureOpenAiEmbeddingsServiceSettings(
        String resourceName,
        String deploymentId,
        String apiVersion,
        @Nullable Integer dimensions,
        boolean dimensionsSetByUser,
        @Nullable Integer maxInputTokens,
        @Nullable SimilarityMeasure similarity,
        @Nullable RateLimitSettings rateLimitSettings,
        @Nullable AzureOpenAiOAuth2Settings oAuth2Settings
    ) {
        super(
            resourceName,
            deploymentId,
            apiVersion,
            Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS),
            oAuth2Settings
        );
        this.dimensions = dimensions;
        this.dimensionsSetByUser = dimensionsSetByUser;
        this.maxInputTokens = maxInputTokens;
        this.similarity = similarity;
    }

    private AzureOpenAiEmbeddingsServiceSettings(
        CommonSettings commonSettings,
        @Nullable Integer dimensions,
        boolean dimensionsSetByUser,
        @Nullable Integer maxInputTokens,
        @Nullable SimilarityMeasure similarity
    ) {
        this(
            commonSettings.resourceName(),
            commonSettings.deploymentId(),
            commonSettings.apiVersion(),
            dimensions,
            dimensionsSetByUser,
            maxInputTokens,
            similarity,
            commonSettings.rateLimitSettings(),
            commonSettings.oAuth2Settings()
        );
    }

    public AzureOpenAiEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readString(),
            in.readString(),
            in.readOptionalVInt(),
            in.readBoolean(),
            in.readOptionalVInt(),
            in.readOptionalEnum(SimilarityMeasure.class),
            new RateLimitSettings(in),
            in.getTransportVersion().supports(AZURE_OPENAI_OAUTH_SETTINGS) ? in.readOptionalWriteable(AzureOpenAiOAuth2Settings::new) : null
        );
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
    public SimilarityMeasure similarity() {
        return similarity;
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return DenseVectorFieldMapper.ElementType.FLOAT;
    }

    @Override
    public String modelId() {
        return null;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public AzureOpenAiEmbeddingsServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();

        var updatedCommonSettings = updateCommonSettings(serviceSettings, validationException);
        var extractedMaxInputTokens = extractOptionalPositiveInteger(
            serviceSettings,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        validationException.throwIfValidationErrorsExist();

        return new AzureOpenAiEmbeddingsServiceSettings(
            updatedCommonSettings,
            this.dimensions,
            this.dimensionsSetByUser,
            extractedMaxInputTokens != null ? extractedMaxInputTokens : this.maxInputTokens,
            this.similarity
        );
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
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(resourceName);
        out.writeString(deploymentId);
        out.writeString(apiVersion);
        out.writeOptionalVInt(dimensions);
        out.writeBoolean(dimensionsSetByUser);
        out.writeOptionalVInt(maxInputTokens);
        out.writeOptionalEnum(similarity);
        rateLimitSettings.writeTo(out);
        if (out.getTransportVersion().supports(AZURE_OPENAI_OAUTH_SETTINGS)) {
            out.writeOptionalWriteable(oAuth2Settings);
        } else if (oAuth2Settings != null) {
            throw new ElasticsearchStatusException(
                "Cannot send OAuth2 settings to an older node. Please wait until all nodes are upgraded before using OAuth2.",
                RestStatus.BAD_REQUEST
            );
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        AzureOpenAiEmbeddingsServiceSettings that = (AzureOpenAiEmbeddingsServiceSettings) o;
        return Objects.equals(dimensions, that.dimensions)
            && Objects.equals(dimensionsSetByUser, that.dimensionsSetByUser)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && similarity == that.similarity;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dimensions, dimensionsSetByUser, maxInputTokens, similarity);
    }
}
