/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockServiceSettings;
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

public class AmazonBedrockEmbeddingsServiceSettings extends AmazonBedrockServiceSettings {
    public static final String NAME = "amazon_bedrock_embeddings_service_settings";
    static final String DIMENSIONS_SET_BY_USER = "dimensions_set_by_user";

    private final Integer dimensions;
    private final Boolean dimensionsSetByUser;
    private final Integer maxInputTokens;
    private final SimilarityMeasure similarity;

    public static AmazonBedrockEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        var settings = embeddingSettingsFromMap(map, validationException, context);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return settings;
    }

    private static AmazonBedrockEmbeddingsServiceSettings embeddingSettingsFromMap(
        Map<String, Object> map,
        ValidationException validationException,
        ConfigurationParseContext context
    ) {
        var baseSettings = AmazonBedrockServiceSettings.fromMap(map, validationException, context);
        SimilarityMeasure similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);

        Integer maxTokens = extractOptionalPositiveInteger(
            map,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        Integer dims = extractOptionalPositiveInteger(map, DIMENSIONS, ModelConfigurations.SERVICE_SETTINGS, validationException);

        Boolean dimensionsSetByUser = extractOptionalBoolean(map, DIMENSIONS_SET_BY_USER, validationException);

        switch (context) {
            case REQUEST -> {
                if (dimensionsSetByUser != null) {
                    validationException.addValidationError(
                        ServiceUtils.invalidSettingError(DIMENSIONS_SET_BY_USER, ModelConfigurations.SERVICE_SETTINGS)
                    );
                }

                if (dims != null) {
                    validationException.addValidationError(
                        ServiceUtils.invalidSettingError(DIMENSIONS, ModelConfigurations.SERVICE_SETTINGS)
                    );
                }
                dimensionsSetByUser = false;
            }
            case PERSISTENT -> {
                if (dimensionsSetByUser == null) {
                    validationException.addValidationError(
                        ServiceUtils.missingSettingErrorMsg(DIMENSIONS_SET_BY_USER, ModelConfigurations.SERVICE_SETTINGS)
                    );
                }
            }
        }
        return new AmazonBedrockEmbeddingsServiceSettings(
            baseSettings.region(),
            baseSettings.model(),
            baseSettings.provider(),
            dims,
            dimensionsSetByUser,
            maxTokens,
            similarity,
            baseSettings.rateLimitSettings()
        );
    }

    public AmazonBedrockEmbeddingsServiceSettings(StreamInput in) throws IOException {
        super(in);
        dimensions = in.readOptionalVInt();
        dimensionsSetByUser = in.readBoolean();
        maxInputTokens = in.readOptionalVInt();
        similarity = in.readOptionalEnum(SimilarityMeasure.class);
    }

    public AmazonBedrockEmbeddingsServiceSettings(
        String region,
        String model,
        AmazonBedrockProvider provider,
        @Nullable Integer dimensions,
        Boolean dimensionsSetByUser,
        @Nullable Integer maxInputTokens,
        @Nullable SimilarityMeasure similarity,
        RateLimitSettings rateLimitSettings
    ) {
        super(region, model, provider, rateLimitSettings);
        this.dimensions = dimensions;
        this.dimensionsSetByUser = dimensionsSetByUser;
        this.maxInputTokens = maxInputTokens;
        this.similarity = similarity;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalVInt(dimensions);
        out.writeBoolean(dimensionsSetByUser);
        out.writeOptionalVInt(maxInputTokens);
        out.writeOptionalEnum(similarity);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        super.addBaseXContent(builder, params);
        builder.field(DIMENSIONS_SET_BY_USER, dimensionsSetByUser);

        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        super.addXContentFragmentOfExposedFields(builder, params);

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
    public SimilarityMeasure similarity() {
        return similarity;
    }

    @Override
    public Integer dimensions() {
        return dimensions;
    }

    @Override
    public Boolean dimensionsSetByUser() {
        return this.dimensionsSetByUser;
    }

    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return DenseVectorFieldMapper.ElementType.FLOAT;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AmazonBedrockEmbeddingsServiceSettings that = (AmazonBedrockEmbeddingsServiceSettings) o;

        return Objects.equals(region, that.region)
            && Objects.equals(provider, that.provider)
            && Objects.equals(model, that.model)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(dimensionsSetByUser, that.dimensionsSetByUser)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(region, model, provider, dimensions, dimensionsSetByUser, maxInputTokens, similarity, rateLimitSettings);
    }

}
