/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.InferenceUtils;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiService;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceSettings;
import org.elasticsearch.xpack.inference.services.azureopenai.oauth.AzureOpenAiOAuthSettings;
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
import static org.elasticsearch.xpack.inference.services.azureopenai.oauth.AzureOpenAiOAuthSettings.AZURE_OPENAI_OAUTH_SETTINGS;

/**
 * Defines the service settings for interacting with OpenAI's text embedding models.
 */
public class AzureOpenAiEmbeddingsServiceSettings extends FilteredXContentObject implements ServiceSettings, AzureOpenAiServiceSettings {

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

        Boolean dimensionsSetByUser = extractOptionalBoolean(map, ServiceFields.DIMENSIONS_SET_BY_USER, validationException);

        var oAuthSettings = AzureOpenAiOAuthSettings.fromMap(map, validationException);

        switch (context) {
            case REQUEST -> {
                if (dimensionsSetByUser != null) {
                    validationException.addValidationError(
                        ServiceUtils.invalidSettingError(ServiceFields.DIMENSIONS_SET_BY_USER, ModelConfigurations.SERVICE_SETTINGS)
                    );
                }
                dimensionsSetByUser = dims != null;
            }
            case PERSISTENT -> {
                if (dimensionsSetByUser == null) {
                    validationException.addValidationError(
                        InferenceUtils.missingSettingErrorMsg(ServiceFields.DIMENSIONS_SET_BY_USER, ModelConfigurations.SERVICE_SETTINGS)
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
            rateLimitSettings,
            oAuthSettings
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
        RateLimitSettings rateLimitSettings,
        @Nullable AzureOpenAiOAuthSettings oAuthSettings
    ) {}

    private final CommonFields fields;

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
        this(
            new CommonFields(
                resourceName,
                deploymentId,
                apiVersion,
                dimensions,
                Objects.requireNonNull(dimensionsSetByUser),
                maxInputTokens,
                similarity,
                Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS),
                // TODO remove this constructor
                null
            )
        );
    }

    public AzureOpenAiEmbeddingsServiceSettings(
        String resourceName,
        String deploymentId,
        String apiVersion,
        @Nullable Integer dimensions,
        Boolean dimensionsSetByUser,
        @Nullable Integer maxInputTokens,
        @Nullable SimilarityMeasure similarity,
        @Nullable RateLimitSettings rateLimitSettings,
        @Nullable AzureOpenAiOAuthSettings oAuthSettings
    ) {
        this(
            new CommonFields(
                resourceName,
                deploymentId,
                apiVersion,
                dimensions,
                Objects.requireNonNull(dimensionsSetByUser),
                maxInputTokens,
                similarity,
                Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS),
                oAuthSettings
            )
        );
    }

    public AzureOpenAiEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this(
            new CommonFields(
                in.readString(),
                in.readString(),
                in.readString(),
                in.readOptionalVInt(),
                in.readBoolean(),
                in.readOptionalVInt(),
                in.readOptionalEnum(SimilarityMeasure.class),
                new RateLimitSettings(in),
                in.getTransportVersion().supports(AZURE_OPENAI_OAUTH_SETTINGS)
                    ? in.readOptionalWriteable(AzureOpenAiOAuthSettings::new)
                    : null
            )
        );
    }

    private AzureOpenAiEmbeddingsServiceSettings(CommonFields fields) {
        this.fields = fields;
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return fields.rateLimitSettings();
    }

    @Override
    public String resourceName() {
        return fields.resourceName();
    }

    @Override
    public String deploymentId() {
        return fields.deploymentId();
    }

    public String apiVersion() {
        return fields.apiVersion();
    }

    @Override
    public Integer dimensions() {
        return fields.dimensions();
    }

    public Boolean dimensionsSetByUser() {
        return fields.dimensionsSetByUser();
    }

    public Integer maxInputTokens() {
        return fields.maxInputTokens();
    }

    @Override
    public SimilarityMeasure similarity() {
        return fields.similarity();
    }

    public AzureOpenAiOAuthSettings oAuth2Settings() {
        return fields.oAuthSettings();
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
        builder.field(ServiceFields.DIMENSIONS_SET_BY_USER, fields.dimensionsSetByUser());

        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(RESOURCE_NAME, fields.resourceName());
        builder.field(DEPLOYMENT_ID, fields.deploymentId());
        builder.field(API_VERSION, fields.apiVersion());

        if (fields.dimensions() != null) {
            builder.field(DIMENSIONS, fields.dimensions());
        }
        if (fields.maxInputTokens() != null) {
            builder.field(MAX_INPUT_TOKENS, fields.maxInputTokens());
        }
        if (fields.similarity() != null) {
            builder.field(SIMILARITY, fields.similarity());
        }
        fields.rateLimitSettings().toXContent(builder, params);

        if (fields.oAuthSettings() != null) {
            fields.oAuthSettings().toXContent(builder, params);
        }

        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fields.resourceName());
        out.writeString(fields.deploymentId());
        out.writeString(fields.apiVersion());
        out.writeOptionalVInt(fields.dimensions());
        out.writeBoolean(fields.dimensionsSetByUser());
        out.writeOptionalVInt(fields.maxInputTokens());
        out.writeOptionalEnum(fields.similarity());
        fields.rateLimitSettings().writeTo(out);

        if (out.getTransportVersion().supports(AZURE_OPENAI_OAUTH_SETTINGS)) {
            out.writeOptionalWriteable(fields.oAuthSettings());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AzureOpenAiEmbeddingsServiceSettings that = (AzureOpenAiEmbeddingsServiceSettings) o;
        return Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }
}
