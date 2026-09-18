/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.embeddings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.InferenceUtils;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceSettings;
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
import static org.elasticsearch.xpack.inference.services.SettingsScope.SERVICE_SETTINGS;

/**
 * Service settings of the OCI Generative AI text embedding task. Adds the embedding dimensions, similarity measure and maximum input
 * token count to the common OCI Generative AI settings. When the user sets {@code dimensions}, the value is passed to the service as
 * {@code outputDimensions} (supported by {@code cohere.embed-v4.0}); otherwise the model's native dimensionality is discovered from the
 * first embedding response.
 */
public class OciGenAiEmbeddingsServiceSettings extends OciGenAiServiceSettings {

    public static final String NAME = "oci_genai_embeddings_service_settings";

    public static OciGenAiEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        var common = extractCommonSettings(map, validationException, context);
        var maxInputTokens = extractOptionalPositiveInteger(map, MAX_INPUT_TOKENS, SERVICE_SETTINGS, validationException);
        var similarity = extractSimilarity(map, SERVICE_SETTINGS, validationException);
        var dimensions = extractOptionalPositiveInteger(map, DIMENSIONS, SERVICE_SETTINGS, validationException);
        var dimensionsSetByUser = extractOptionalBoolean(map, ServiceFields.DIMENSIONS_SET_BY_USER, validationException);

        switch (context) {
            case REQUEST -> {
                if (dimensionsSetByUser != null) {
                    validationException.addValidationError(
                        ServiceUtils.invalidSettingError(ServiceFields.DIMENSIONS_SET_BY_USER, SERVICE_SETTINGS)
                    );
                }
                dimensionsSetByUser = dimensions != null;
            }
            case PERSISTENT -> {
                if (dimensionsSetByUser == null) {
                    validationException.addValidationError(
                        InferenceUtils.missingSettingErrorMsg(ServiceFields.DIMENSIONS_SET_BY_USER, SERVICE_SETTINGS.toString())
                    );
                }
            }
        }

        validationException.throwIfValidationErrorsExist();

        return new OciGenAiEmbeddingsServiceSettings(common, dimensionsSetByUser, dimensions, maxInputTokens, similarity);
    }

    private final Boolean dimensionsSetByUser;
    private final Integer dimensions;
    private final Integer maxInputTokens;
    private final SimilarityMeasure similarity;

    public OciGenAiEmbeddingsServiceSettings(
        CommonSettings common,
        Boolean dimensionsSetByUser,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable SimilarityMeasure similarity
    ) {
        super(common);
        this.dimensionsSetByUser = Objects.requireNonNull(dimensionsSetByUser);
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
        this.similarity = similarity;
    }

    public OciGenAiEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this(
            new CommonSettings(in),
            in.readBoolean(),
            in.readOptionalVInt(),
            in.readOptionalVInt(),
            in.readOptionalEnum(SimilarityMeasure.class)
        );
    }

    @Override
    public OciGenAiEmbeddingsServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();
        var extractedMaxInputTokens = extractOptionalPositiveInteger(
            serviceSettings,
            MAX_INPUT_TOKENS,
            SERVICE_SETTINGS,
            validationException
        );
        var extractedRateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            rateLimitSettings(),
            validationException,
            ConfigurationParseContext.REQUEST
        );
        validationException.throwIfValidationErrorsExist();

        // Only max_input_tokens and rate_limit can be updated, other fields remain unchanged.
        return new OciGenAiEmbeddingsServiceSettings(
            common().withRateLimitSettings(extractedRateLimitSettings),
            dimensionsSetByUser,
            dimensions,
            extractedMaxInputTokens != null ? extractedMaxInputTokens : maxInputTokens,
            similarity
        );
    }

    @Override
    public Boolean dimensionsSetByUser() {
        return dimensionsSetByUser;
    }

    @Override
    public Integer dimensions() {
        return dimensions;
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
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        common().writeTo(out);
        out.writeBoolean(dimensionsSetByUser);
        out.writeOptionalVInt(dimensions);
        out.writeOptionalVInt(maxInputTokens);
        out.writeOptionalEnum(similarity);
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
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (super.equals(o) == false) return false;
        OciGenAiEmbeddingsServiceSettings that = (OciGenAiEmbeddingsServiceSettings) o;
        return Objects.equals(dimensionsSetByUser, that.dimensionsSetByUser)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(similarity, that.similarity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dimensionsSetByUser, dimensions, maxInputTokens, similarity);
    }
}
