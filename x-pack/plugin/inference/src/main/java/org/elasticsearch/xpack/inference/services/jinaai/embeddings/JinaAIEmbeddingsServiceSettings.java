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
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;

public class JinaAIEmbeddingsServiceSettings extends FilteredXContentObject implements ServiceSettings {
    public static final String NAME = "jinaai_embeddings_service_settings";

    static final String EMBEDDING_TYPE = "embedding_type";

    public static JinaAIEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();
        var commonServiceSettings = JinaAIServiceSettings.fromMap(map, context);

        JinaAIEmbeddingType embeddingTypes = parseEmbeddingType(map, context, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new JinaAIEmbeddingsServiceSettings(commonServiceSettings, embeddingTypes);
    }

    static JinaAIEmbeddingType parseEmbeddingType(
        Map<String, Object> map,
        ConfigurationParseContext context,
        ValidationException validationException
    ) {
        return switch (context) {
            case REQUEST -> Objects.requireNonNullElse(
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
            case PERSISTENT -> {
                var embeddingType = ServiceUtils.extractOptionalString(
                    map,
                    EMBEDDING_TYPE,
                    ModelConfigurations.SERVICE_SETTINGS,
                    validationException
                );
                yield fromJinaAIOrDenseVectorEnumValues(embeddingType, validationException);
            }

        };
    }

    /**
     * Parse either and convert to a JinaAIEmbeddingType
     */
    static JinaAIEmbeddingType fromJinaAIOrDenseVectorEnumValues(String enumString, ValidationException validationException) {
        // TODO(JoanFM): Check this
        if (enumString == null) {
            return JinaAIEmbeddingType.FLOAT;
        }

        try {
            return JinaAIEmbeddingType.fromString(enumString);
        } catch (IllegalArgumentException ae) {
            try {
                return JinaAIEmbeddingType.fromElementType(DenseVectorFieldMapper.ElementType.fromString(enumString));
            } catch (IllegalArgumentException iae) {
                var validValuesAsStrings = JinaAIEmbeddingType.SUPPORTED_ELEMENT_TYPES.stream()
                    .map(value -> value.toString().toLowerCase(Locale.ROOT))
                    .toArray(String[]::new);
                validationException.addValidationError(
                    ServiceUtils.invalidValue(EMBEDDING_TYPE, ModelConfigurations.SERVICE_SETTINGS, enumString, validValuesAsStrings)
                );
                return null;
            }
        }
    }

    private final JinaAIServiceSettings commonSettings;
    private final JinaAIEmbeddingType embeddingType;

    public JinaAIEmbeddingsServiceSettings(JinaAIServiceSettings commonSettings, JinaAIEmbeddingType embeddingType) {
        this.commonSettings = commonSettings;
        this.embeddingType = Objects.requireNonNull(embeddingType);
    }

    public JinaAIEmbeddingsServiceSettings(StreamInput in) throws IOException {
        commonSettings = new JinaAIServiceSettings(in);
        embeddingType = Objects.requireNonNullElse(in.readOptionalEnum(JinaAIEmbeddingType.class), JinaAIEmbeddingType.FLOAT);
    }

    public JinaAIServiceSettings getCommonSettings() {
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

        commonSettings.toXContentFragment(builder, params);
        builder.field(EMBEDDING_TYPE, elementType());

        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        commonSettings.toXContentFragmentOfExposedFields(builder, params);
        builder.field(EMBEDDING_TYPE, elementType());

        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_13_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        commonSettings.writeTo(out);
        out.writeOptionalEnum(JinaAIEmbeddingType.translateToVersion(embeddingType, out.getTransportVersion()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JinaAIEmbeddingsServiceSettings that = (JinaAIEmbeddingsServiceSettings) o;
        return Objects.equals(commonSettings, that.commonSettings) && Objects.equals(embeddingType, that.embeddingType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonSettings, embeddingType);
    }
}
