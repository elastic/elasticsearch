/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.embeddings;

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
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudCommonServiceSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudRateLimitServiceSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;

public class TencentCloudEmbeddingsServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        TencentCloudRateLimitServiceSettings {

    public static final String NAME = "tencentcloud_embeddings_service_settings";

    public static TencentCloudEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        var commonSettings = TencentCloudCommonServiceSettings.fromMap(map, context, validationException);
        var similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var dimensions = extractOptionalPositiveInteger(map, DIMENSIONS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var maxInputTokens = extractOptionalPositiveInteger(
            map,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        validationException.throwIfValidationErrorsExist();

        return new TencentCloudEmbeddingsServiceSettings(commonSettings, similarity, dimensions, maxInputTokens);
    }

    private final TencentCloudCommonServiceSettings commonSettings;
    @Nullable
    private final SimilarityMeasure similarity;
    @Nullable
    private final Integer dimensions;
    @Nullable
    private final Integer maxInputTokens;

    public TencentCloudEmbeddingsServiceSettings(
        TencentCloudCommonServiceSettings commonSettings,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens
    ) {
        this.commonSettings = Objects.requireNonNull(commonSettings);
        this.similarity = similarity;
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
    }

    public TencentCloudEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this.commonSettings = new TencentCloudCommonServiceSettings(in);
        this.similarity = in.readOptionalEnum(SimilarityMeasure.class);
        this.dimensions = in.readOptionalVInt();
        this.maxInputTokens = in.readOptionalVInt();
    }

    public TencentCloudCommonServiceSettings getCommonSettings() {
        return commonSettings;
    }

    @Override
    public String modelId() {
        return commonSettings.modelId();
    }

    @Override
    public SimilarityMeasure similarity() {
        return similarity;
    }

    @Override
    public Integer dimensions() {
        return dimensions;
    }

    @Nullable
    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return commonSettings.rateLimitSettings();
    }

    @Override
    public TencentCloudEmbeddingsServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();

        var extractedMaxInputTokens = extractOptionalPositiveInteger(
            serviceSettings,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        var updatedCommonServiceSettings = commonSettings.updateCommonServiceSettings(serviceSettings, validationException);

        validationException.throwIfValidationErrorsExist();

        return new TencentCloudEmbeddingsServiceSettings(
            updatedCommonServiceSettings,
            this.similarity,
            this.dimensions,
            extractedMaxInputTokens != null ? extractedMaxInputTokens : this.maxInputTokens
        );
    }

    /**
     * Return an updated copy with the resolved embedding size and similarity measure.
     */
    public TencentCloudEmbeddingsServiceSettings updateEmbeddingDetails(int newDimensions, SimilarityMeasure newSimilarity) {
        return new TencentCloudEmbeddingsServiceSettings(commonSettings, newSimilarity, newDimensions, maxInputTokens);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        commonSettings.toXContentFragment(builder, params);
        toXContentFragmentOfExposedFields(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        commonSettings.writeTo(out);
        out.writeOptionalEnum(similarity);
        out.writeOptionalVInt(dimensions);
        out.writeOptionalVInt(maxInputTokens);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TencentCloudEmbeddingsServiceSettings that = (TencentCloudEmbeddingsServiceSettings) o;
        return Objects.equals(commonSettings, that.commonSettings)
            && similarity == that.similarity
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonSettings, similarity, dimensions, maxInputTokens);
    }
}
