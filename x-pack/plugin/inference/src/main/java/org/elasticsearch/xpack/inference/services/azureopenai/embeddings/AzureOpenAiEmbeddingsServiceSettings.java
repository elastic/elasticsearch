/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiService;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.API_VERSION;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.DEPLOYMENT_ID;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.RESOURCE_NAME;

/**
 * Defines the service settings for interacting with OpenAI's text embedding models.
 */
public class AzureOpenAiEmbeddingsServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        AzureOpenAiRateLimitServiceSettings {

    public static final String NAME = "azure_openai_embeddings_service_settings";

    static final String DIMENSIONS_SET_BY_USER = "dimensions_set_by_user";
    /**
     * Rate limit documentation can be found here:
     * Limits per region per model id
     * https://learn.microsoft.com/en-us/azure/ai-services/openai/quotas-limits
     *
     * How to change the limits
     * https://learn.microsoft.com/en-us/azure/ai-services/openai/how-to/quota?tabs=rest
     *
     * Blog giving some examples
     * https://techcommunity.microsoft.com/t5/fasttrack-for-azure/optimizing-azure-openai-a-guide-to-limits-quotas-and-best/ba-p/4076268
     *
     * According to the docs 1000 tokens per minute (TPM) = 6 requests per minute (RPM). The limits change depending on the region
     * and model. The lowest text embedding limit is 240K TPM, so we'll default to that.
     * Calculation: 240K TPM = 240 * 6 = 1440 requests per minute (used `eastus` and `Text-Embedding-Ada-002` as basis for the calculation).
     */
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(1_440);

    public static AzureOpenAiEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        var settings = fromMap(map, validationException, context);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AzureOpenAiEmbeddingsServiceSettings(settings);
    }

    private static CommonFields fromMap(
        Map<String, Object> map,
        ValidationException validationException,
        ConfigurationParseContext context
    ) {
        String resourceName = extractRequiredString(map, RESOURCE_NAME, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String deploymentId = extractRequiredString(map, DEPLOYMENT_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String apiVersion = extractRequiredString(map, API_VERSION, ModelConfigurations.SERVICE_SETTINGS, validationException);
        Integer dims = extractOptionalPositiveInteger(map, DIMENSIONS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        Integer maxTokens = extractOptionalPositiveInteger(
            map,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        SimilarityMeasure similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            AzureOpenAiService.NAME,
            context
        );

        Boolean dimensionsSetByUser = extractOptionalBoolean(map, DIMENSIONS_SET_BY_USER, validationException);

        switch (context) {
            case REQUEST -> {
                if (dimensionsSetByUser != null) {
                    validationException.addValidationError(
                        ServiceUtils.invalidSettingError(DIMENSIONS_SET_BY_USER, ModelConfigurations.SERVICE_SETTINGS)
                    );
                }
                dimensionsSetByUser = dims != null;
            }
            case PERSISTENT -> {
                if (dimensionsSetByUser == null) {
                    validationException.addValidationError(
                        ServiceUtils.missingSettingErrorMsg(DIMENSIONS_SET_BY_USER, ModelConfigurations.SERVICE_SETTINGS)
                    );
                }
            }
        }

        return new CommonFields(
            resourceName,
            deploymentId,
            apiVersion,
            dims,
            Boolean.TRUE.equals(dimensionsSetByUser),
            maxTokens,
            similarity,
            rateLimitSettings
        );
    }

    private record CommonFields(
        String resourceName,
        String deploymentId,
        String apiVersion,
        @Nullable Integer dimensions,
        Boolean dimensionsSetByUser,
        @Nullable Integer maxInputTokens,
        @Nullable SimilarityMeasure similarity,
        RateLimitSettings rateLimitSettings
    ) {}

    private final String resourceName;
    private final String deploymentId;
    private final String apiVersion;
    private final Integer dimensions;
    private final Boolean dimensionsSetByUser;
    private final Integer maxInputTokens;
    private final SimilarityMeasure similarity;
    private final RateLimitSettings rateLimitSettings;

    public AzureOpenAiEmbeddingsServiceSettings(
        String resourceName,
        String deploymentId,
        String apiVersion,
        @Nullable Integer dimensions,
        Boolean dimensionsSetByUser,
        @Nullable Integer maxInputTokens,
        @Nullable SimilarityMeasure similarity,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.resourceName = resourceName;
        this.deploymentId = deploymentId;
        this.apiVersion = apiVersion;
        this.dimensions = dimensions;
        this.dimensionsSetByUser = Objects.requireNonNull(dimensionsSetByUser);
        this.maxInputTokens = maxInputTokens;
        this.similarity = similarity;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public AzureOpenAiEmbeddingsServiceSettings(StreamInput in) throws IOException {
        resourceName = in.readString();
        deploymentId = in.readString();
        apiVersion = in.readString();
        dimensions = in.readOptionalVInt();
        dimensionsSetByUser = in.readBoolean();
        maxInputTokens = in.readOptionalVInt();
        similarity = in.readOptionalEnum(SimilarityMeasure.class);

        if (in.getTransportVersion().onOrAfter(TransportVersions.ML_INFERENCE_RATE_LIMIT_SETTINGS_ADDED)) {
            rateLimitSettings = new RateLimitSettings(in);
        } else {
            rateLimitSettings = DEFAULT_RATE_LIMIT_SETTINGS;
        }
    }

    private AzureOpenAiEmbeddingsServiceSettings(CommonFields fields) {
        this(
            fields.resourceName,
            fields.deploymentId,
            fields.apiVersion,
            fields.dimensions,
            fields.dimensionsSetByUser,
            fields.maxInputTokens,
            fields.similarity,
            fields.rateLimitSettings
        );
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public String resourceName() {
        return resourceName;
    }

    @Override
    public String deploymentId() {
        return deploymentId;
    }

    public String apiVersion() {
        return apiVersion;
    }

    @Override
    public Integer dimensions() {
        return dimensions;
    }

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
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        toXContentFragmentOfExposedFields(builder, params);
        builder.field(DIMENSIONS_SET_BY_USER, dimensionsSetByUser);

        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(RESOURCE_NAME, resourceName);
        builder.field(DEPLOYMENT_ID, deploymentId);
        builder.field(API_VERSION, apiVersion);

        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_14_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(resourceName);
        out.writeString(deploymentId);
        out.writeString(apiVersion);
        out.writeOptionalVInt(dimensions);
        out.writeBoolean(dimensionsSetByUser);
        out.writeOptionalVInt(maxInputTokens);
        out.writeOptionalEnum(SimilarityMeasure.translateSimilarity(similarity, out.getTransportVersion()));

        if (out.getTransportVersion().onOrAfter(TransportVersions.ML_INFERENCE_RATE_LIMIT_SETTINGS_ADDED)) {
            rateLimitSettings.writeTo(out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AzureOpenAiEmbeddingsServiceSettings that = (AzureOpenAiEmbeddingsServiceSettings) o;

        return Objects.equals(resourceName, that.resourceName)
            && Objects.equals(deploymentId, that.deploymentId)
            && Objects.equals(apiVersion, that.apiVersion)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(dimensionsSetByUser, that.dimensionsSetByUser)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            resourceName,
            deploymentId,
            apiVersion,
            dimensions,
            dimensionsSetByUser,
            maxInputTokens,
            similarity,
            rateLimitSettings
        );
    }
}
