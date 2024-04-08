/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;

public class CohereEmbeddingsServiceSettings implements ServiceSettings {
    public static final String NAME = "cohere_embeddings_service_settings";

    static final String EMBEDDING_TYPE = "embedding_type";

    public static CohereEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();
        var commonServiceSettings = CohereServiceSettings.fromMap(map, context);

        CohereEmbeddingType embeddingTypes = parseEmbeddingType(map, context, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new CohereEmbeddingsServiceSettings(commonServiceSettings, embeddingTypes);
    }

    private static CohereEmbeddingType parseEmbeddingType(
        Map<String, Object> map,
        ConfigurationParseContext context,
        ValidationException validationException
    ) {
        if (context == ConfigurationParseContext.REQUEST) {
            return Objects.requireNonNullElse(
                extractOptionalEnum(
                    map,
                    EMBEDDING_TYPE,
                    ModelConfigurations.SERVICE_SETTINGS,
                    CohereEmbeddingType::fromString,
                    EnumSet.allOf(CohereEmbeddingType.class),
                    validationException
                ),
                CohereEmbeddingType.FLOAT
            );
        }

        DenseVectorFieldMapper.ElementType elementType = Objects.requireNonNullElse(
            extractOptionalEnum(
                map,
                EMBEDDING_TYPE,
                ModelConfigurations.SERVICE_SETTINGS,
                DenseVectorFieldMapper.ElementType::fromString,
                CohereEmbeddingType.SUPPORTED_ELEMENT_TYPES,
                validationException
            ),
            DenseVectorFieldMapper.ElementType.FLOAT
        );

        return CohereEmbeddingType.fromElementType(elementType);
    }

    private final CohereServiceSettings commonSettings;
    private final CohereEmbeddingType embeddingType;

    public CohereEmbeddingsServiceSettings(CohereServiceSettings commonSettings, CohereEmbeddingType embeddingType) {
        this.commonSettings = commonSettings;
        this.embeddingType = Objects.requireNonNull(embeddingType);
    }

    public CohereEmbeddingsServiceSettings(StreamInput in) throws IOException {
        commonSettings = new CohereServiceSettings(in);
        embeddingType = Objects.requireNonNullElse(in.readOptionalEnum(CohereEmbeddingType.class), CohereEmbeddingType.FLOAT);
    }

    public CohereServiceSettings getCommonSettings() {
        return commonSettings;
    }

    @Override
    public SimilarityMeasure similarity() {
        return commonSettings.similarity();
    }

    @Override
    public Integer dimensions() {
        return commonSettings.dimensions();
    }

    public CohereEmbeddingType getEmbeddingType() {
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

        commonSettings.toXContentFragment(builder);
        builder.field(EMBEDDING_TYPE, elementType());

        builder.endObject();
        return builder;
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return this;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_COHERE_EMBEDDINGS_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        commonSettings.writeTo(out);
        out.writeOptionalEnum(CohereEmbeddingType.translateToVersion(embeddingType, out.getTransportVersion()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CohereEmbeddingsServiceSettings that = (CohereEmbeddingsServiceSettings) o;
        return Objects.equals(commonSettings, that.commonSettings) && Objects.equals(embeddingType, that.embeddingType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonSettings, embeddingType);
    }
}
