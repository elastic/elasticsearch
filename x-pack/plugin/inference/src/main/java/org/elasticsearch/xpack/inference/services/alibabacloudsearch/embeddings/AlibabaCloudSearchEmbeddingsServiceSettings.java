/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;

public class AlibabaCloudSearchEmbeddingsServiceSettings implements ServiceSettings {
    public static final String NAME = "alibabacloud_search_embeddings_service_settings";

    public static AlibabaCloudSearchEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        var commonServiceSettings = AlibabaCloudSearchServiceSettings.fromMap(map, context, validationException);

        var similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var dimensions = extractOptionalPositiveInteger(map, DIMENSIONS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var maxInputTokens = extractOptionalPositiveInteger(
            map,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        validationException.throwIfValidationErrorsExist();

        return new AlibabaCloudSearchEmbeddingsServiceSettings(commonServiceSettings, similarity, dimensions, maxInputTokens);
    }

    private final AlibabaCloudSearchServiceSettings commonSettings;
    private final SimilarityMeasure similarity;
    private final Integer dimensions;
    private final Integer maxInputTokens;

    public AlibabaCloudSearchEmbeddingsServiceSettings(
        AlibabaCloudSearchServiceSettings commonSettings,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens
    ) {
        this.commonSettings = commonSettings;
        this.similarity = similarity;
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
    }

    public AlibabaCloudSearchEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this.commonSettings = new AlibabaCloudSearchServiceSettings(in);
        this.similarity = in.readOptionalEnum(SimilarityMeasure.class);
        this.dimensions = in.readOptionalVInt();
        this.maxInputTokens = in.readOptionalVInt();
    }

    public AlibabaCloudSearchServiceSettings getCommonSettings() {
        return commonSettings;
    }

    public SimilarityMeasure getSimilarity() {
        return similarity;
    }

    @Override
    public Integer dimensions() {
        return dimensions;
    }

    @Override
    public SimilarityMeasure similarity() {
        return similarity;
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return DenseVectorFieldMapper.ElementType.FLOAT;
    }

    public Integer getMaxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public String modelId() {
        return commonSettings.modelId();
    }

    @Override
    public AlibabaCloudSearchEmbeddingsServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();
        var commonServiceSettings = commonSettings.updateServiceSettings(serviceSettings, validationException);

        var extractedMaxInputTokens = extractOptionalPositiveInteger(
            serviceSettings,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        validationException.throwIfValidationErrorsExist();

        return new AlibabaCloudSearchEmbeddingsServiceSettings(
            commonServiceSettings,
            this.similarity,
            this.dimensions,
            extractedMaxInputTokens != null ? extractedMaxInputTokens : this.maxInputTokens
        );
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        commonSettings.toXContentFragment(builder, params);
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return this;
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
        AlibabaCloudSearchEmbeddingsServiceSettings that = (AlibabaCloudSearchEmbeddingsServiceSettings) o;
        return Objects.equals(commonSettings, that.commonSettings)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonSettings, similarity, dimensions, maxInputTokens);
    }
}
