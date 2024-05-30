/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.TransportVersions.ADD_MISTRAL_EMBEDDINGS_INFERENCE;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeAsType;
import static org.elasticsearch.xpack.inference.services.mistral.MistralConstants.DIMENSIONS_SET_BY_USER;
import static org.elasticsearch.xpack.inference.services.mistral.MistralConstants.MISTRAL_MODEL_FIELD;

public class MistralEmbeddingsServiceSettings extends FilteredXContentObject implements ServiceSettings {
    public static final String NAME = "mistral_embeddings_service_settings";

    private final String model;
    private final Integer dimensions;
    private final Boolean dimensionsSetByUser;
    private final SimilarityMeasure similarity;
    private final Integer maxInputTokens;
    private final RateLimitSettings rateLimitSettings;

    // default for Mistral is 5 requests / sec
    // setting this to 240 (4 requests / sec) is a sane default for us
    protected static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(240);

    public static MistralEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        String model = extractRequiredString(map, MISTRAL_MODEL_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);
        SimilarityMeasure similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        Integer maxInputTokens = extractOptionalPositiveInteger(
            map,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        RateLimitSettings rateLimitSettings = RateLimitSettings.of(map, DEFAULT_RATE_LIMIT_SETTINGS, validationException);

        Integer dims = removeAsType(map, DIMENSIONS, Integer.class);
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

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new MistralEmbeddingsServiceSettings(model, dims, dimensionsSetByUser, maxInputTokens, similarity, rateLimitSettings);
    }

    public MistralEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this.model = in.readString();
        this.dimensions = in.readOptionalVInt();
        this.dimensionsSetByUser = in.readBoolean();
        this.similarity = in.readOptionalEnum(SimilarityMeasure.class);
        this.maxInputTokens = in.readOptionalInt();
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    public MistralEmbeddingsServiceSettings(
        String model,
        @Nullable Integer dimensions,
        Boolean dimensionsSetByUser,
        @Nullable Integer maxInputTokens,
        @Nullable SimilarityMeasure similarity,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.model = model;
        this.dimensions = dimensions;
        this.dimensionsSetByUser = dimensionsSetByUser;
        this.similarity = similarity;
        this.maxInputTokens = maxInputTokens;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ADD_MISTRAL_EMBEDDINGS_INFERENCE;
    }

    public String model() {
        return this.model;
    }

    public Integer dimensions() {
        return this.dimensions;
    }

    public boolean dimensionsSetByUser() {
        return this.dimensionsSetByUser;
    }

    public Integer maxInputTokens() {
        return this.maxInputTokens;
    }

    public SimilarityMeasure similarity() {
        return this.similarity;
    }

    public RateLimitSettings rateLimitSettings() {
        return this.rateLimitSettings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(model);
        out.writeOptionalVInt(dimensions);
        out.writeBoolean(dimensionsSetByUser);
        out.writeOptionalEnum(SimilarityMeasure.translateSimilarity(similarity, out.getTransportVersion()));
        out.writeOptionalInt(maxInputTokens);
        rateLimitSettings.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        this.toXContentFragmentOfExposedFields(builder, params);
        builder.field(DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
        rateLimitSettings.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(MISTRAL_MODEL_FIELD, this.model);

        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        if (this.maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, this.maxInputTokens);
        }

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MistralEmbeddingsServiceSettings that = (MistralEmbeddingsServiceSettings) o;
        return Objects.equals(model, that.model)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(dimensionsSetByUser, that.dimensionsSetByUser)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(similarity, that.similarity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(model, dimensions, dimensionsSetByUser, maxInputTokens, similarity);
    }

}
