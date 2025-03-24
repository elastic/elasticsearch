/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.embeddings;

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
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeAsType;

public class JinaAIEmbeddingsServiceSettings extends FilteredXContentObject implements ServiceSettings {
    public static final String NAME = "jinaai_embeddings_service_settings";

    static final String EMBEDDING_TYPE = "embedding_type";

    public static JinaAIEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();
        var commonServiceSettings = JinaAIServiceSettings.fromMap(map, context);
        SimilarityMeasure similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        Integer dims = removeAsType(map, DIMENSIONS, Integer.class);
        Integer maxInputTokens = removeAsType(map, MAX_INPUT_TOKENS, Integer.class);

        JinaAIEmbeddingType embeddingTypes = parseEmbeddingType(map, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new JinaAIEmbeddingsServiceSettings(commonServiceSettings, similarity, dims, maxInputTokens, embeddingTypes);
    }

    static JinaAIEmbeddingType parseEmbeddingType(Map<String, Object> map, ValidationException validationException) {
        return Objects.requireNonNullElse(
            extractOptionalEnum(
                map,
                EMBEDDING_TYPE,
                ModelConfigurations.SERVICE_SETTINGS,
                JinaAIEmbeddingType::fromString,
                EnumSet.allOf(JinaAIEmbeddingType.class),
                validationException
            ),
            JinaAIEmbeddingType.FLOAT
        );
    }

    private final JinaAIServiceSettings commonSettings;
    private final SimilarityMeasure similarity;
    private final Integer dimensions;
    private final Integer maxInputTokens;
    private final JinaAIEmbeddingType embeddingType;

    public JinaAIEmbeddingsServiceSettings(
        JinaAIServiceSettings commonSettings,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable JinaAIEmbeddingType embeddingType
    ) {
        this.commonSettings = commonSettings;
        this.similarity = similarity;
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
        this.embeddingType = embeddingType != null ? embeddingType : JinaAIEmbeddingType.FLOAT;
    }

    public JinaAIEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this.commonSettings = new JinaAIServiceSettings(in);
        this.similarity = in.readOptionalEnum(SimilarityMeasure.class);
        this.dimensions = in.readOptionalVInt();
        this.maxInputTokens = in.readOptionalVInt();

        this.embeddingType = (in.getTransportVersion().onOrAfter(TransportVersions.JINA_AI_EMBEDDING_TYPE_SUPPORT_ADDED)
            || in.getTransportVersion().isPatchFrom(TransportVersions.JINA_AI_EMBEDDING_TYPE_SUPPORT_ADDED_BACKPORT_8_19))
                ? Objects.requireNonNullElse(in.readOptionalEnum(JinaAIEmbeddingType.class), JinaAIEmbeddingType.FLOAT)
                : JinaAIEmbeddingType.FLOAT;
    }

    public JinaAIServiceSettings getCommonSettings() {
        return commonSettings;
    }

    @Override
    public SimilarityMeasure similarity() {
        return similarity;
    }

    @Override
    public Integer dimensions() {
        return dimensions;
    }

    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public String modelId() {
        return commonSettings.modelId();
    }

    public JinaAIEmbeddingType getEmbeddingType() {
        return embeddingType;
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return embeddingType == null ? DenseVectorFieldMapper.ElementType.FLOAT : embeddingType.toElementType();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder = commonSettings.toXContentFragment(builder, params);
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (embeddingType != null) {
            builder.field(EMBEDDING_TYPE, embeddingType);
        }

        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        commonSettings.toXContentFragmentOfExposedFields(builder, params);
        if (embeddingType != null) {
            builder.field(EMBEDDING_TYPE, embeddingType);
        }
        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.JINA_AI_INTEGRATION_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        commonSettings.writeTo(out);
        out.writeOptionalEnum(SimilarityMeasure.translateSimilarity(similarity, out.getTransportVersion()));
        out.writeOptionalVInt(dimensions);
        out.writeOptionalVInt(maxInputTokens);

        if (out.getTransportVersion().onOrAfter(TransportVersions.JINA_AI_EMBEDDING_TYPE_SUPPORT_ADDED)
            || out.getTransportVersion().isPatchFrom(TransportVersions.JINA_AI_EMBEDDING_TYPE_SUPPORT_ADDED_BACKPORT_8_19)) {
            out.writeOptionalEnum(JinaAIEmbeddingType.translateToVersion(embeddingType, out.getTransportVersion()));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JinaAIEmbeddingsServiceSettings that = (JinaAIEmbeddingsServiceSettings) o;
        return Objects.equals(commonSettings, that.commonSettings)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(embeddingType, that.embeddingType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonSettings, similarity, dimensions, maxInputTokens, embeddingType);
    }
}
