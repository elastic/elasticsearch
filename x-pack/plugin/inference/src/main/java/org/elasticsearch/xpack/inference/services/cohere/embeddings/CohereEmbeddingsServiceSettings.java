/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;
import static org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettings.ML_INFERENCE_COHERE_API_VERSION;
import static org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettings.ML_INFERENCE_COHERE_SERVICE_SETTINGS_REFACTOR;

/**
 * Settings for the Cohere embeddings service. Wraps {@link CohereCommonServiceSettings} and adds
 * embeddings-specific fields: similarity measure, dimensions, max input tokens, and embedding type.
 */
public class CohereEmbeddingsServiceSettings extends FilteredXContentObject implements ServiceSettings {

    public static final String NAME = "cohere_embeddings_service_settings";

    /**
     * Creates {@link CohereEmbeddingsServiceSettings} from a map of settings.
     * @param map the map to parse
     * @param context the context in which the parsing is done
     * @return the created {@link CohereEmbeddingsServiceSettings}
     * @throws ValidationException If there are validation errors in the provided settings.
     */
    public static CohereEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();
        var commonSettings = CohereCommonServiceSettings.fromMap(map, context);

        var similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var dimensions = extractOptionalPositiveInteger(map, DIMENSIONS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var maxInputTokens = extractOptionalPositiveInteger(
            map,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var embeddingType = parseEmbeddingType(map, context, validationException);

        validationException.throwIfValidationErrorsExist();

        return new CohereEmbeddingsServiceSettings(commonSettings, similarity, dimensions, maxInputTokens, embeddingType);
    }

    private static CohereEmbeddingType parseEmbeddingType(
        Map<String, Object> map,
        ConfigurationParseContext context,
        ValidationException validationException
    ) {
        return switch (context) {
            case REQUEST -> Objects.requireNonNullElse(
                extractOptionalEnum(
                    map,
                    ServiceFields.EMBEDDING_TYPE,
                    ModelConfigurations.SERVICE_SETTINGS,
                    CohereEmbeddingType::fromString,
                    EnumSet.allOf(CohereEmbeddingType.class),
                    validationException
                ),
                CohereEmbeddingType.FLOAT
            );
            case PERSISTENT -> {
                var embeddingType = ServiceUtils.extractOptionalString(
                    map,
                    ServiceFields.EMBEDDING_TYPE,
                    ModelConfigurations.SERVICE_SETTINGS,
                    validationException
                );
                yield fromCohereOrDenseVectorEnumValues(embeddingType, validationException);
            }
        };
    }

    /**
     * Before TransportVersions::ML_INFERENCE_COHERE_EMBEDDINGS_ADDED element
     * type was persisted as a CohereEmbeddingType enum. After
     * DenseVectorFieldMapper.ElementType was used.
     * <p>
     * Parse either and convert to a CohereEmbeddingType
     */
    static CohereEmbeddingType fromCohereOrDenseVectorEnumValues(String enumString, ValidationException validationException) {
        if (enumString == null) {
            return CohereEmbeddingType.FLOAT;
        }

        try {
            return CohereEmbeddingType.fromString(enumString);
        } catch (IllegalArgumentException ae) {
            try {
                return CohereEmbeddingType.fromElementType(DenseVectorFieldMapper.ElementType.fromString(enumString));
            } catch (IllegalArgumentException iae) {
                var validValuesAsStrings = CohereEmbeddingType.SUPPORTED_ELEMENT_TYPES.stream()
                    .map(value -> value.toString().toLowerCase(Locale.ROOT))
                    .toArray(String[]::new);
                validationException.addValidationError(
                    ServiceUtils.invalidValue(
                        ServiceFields.EMBEDDING_TYPE,
                        ModelConfigurations.SERVICE_SETTINGS,
                        enumString,
                        validValuesAsStrings
                    )
                );
                return null;
            }
        }
    }

    private final CohereCommonServiceSettings commonSettings;
    private final SimilarityMeasure similarity;
    private final Integer dimensions;
    private final Integer maxInputTokens;
    private final CohereEmbeddingType embeddingType;

    public CohereEmbeddingsServiceSettings(
        CohereCommonServiceSettings commonSettings,
        SimilarityMeasure similarity,
        Integer dimensions,
        Integer maxInputTokens,
        CohereEmbeddingType embeddingType
    ) {
        this.commonSettings = Objects.requireNonNull(commonSettings);
        this.similarity = similarity;
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
        this.embeddingType = Objects.requireNonNull(embeddingType);
    }

    public CohereEmbeddingsServiceSettings(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(ML_INFERENCE_COHERE_SERVICE_SETTINGS_REFACTOR) == false) {
            // Old format: full CohereServiceSettings wire layout
            // uri, similarity, dimensions, maxInputTokens, modelId, rateLimitSettings, [apiVersion]
            var uri = createOptionalUri(in.readOptionalString());
            this.similarity = in.readOptionalEnum(SimilarityMeasure.class);
            this.dimensions = in.readOptionalVInt();
            this.maxInputTokens = in.readOptionalVInt();
            var modelId = in.readOptionalString();
            var rateLimitSettings = new RateLimitSettings(in);
            var apiVersion = in.getTransportVersion().supports(ML_INFERENCE_COHERE_API_VERSION)
                ? in.readEnum(CohereCommonServiceSettings.CohereApiVersion.class)
                : CohereCommonServiceSettings.CohereApiVersion.V1;
            this.commonSettings = new CohereCommonServiceSettings(uri, modelId, rateLimitSettings, apiVersion);
        } else {
            this.commonSettings = new CohereCommonServiceSettings(in);
            this.similarity = in.readOptionalEnum(SimilarityMeasure.class);
            this.dimensions = in.readOptionalVInt();
            this.maxInputTokens = in.readOptionalVInt();
        }
        this.embeddingType = Objects.requireNonNullElse(in.readOptionalEnum(CohereEmbeddingType.class), CohereEmbeddingType.FLOAT);
    }

    public CohereCommonServiceSettings getCommonSettings() {
        return commonSettings;
    }

    public RateLimitSettings rateLimitSettings() {
        return commonSettings.rateLimitSettings();
    }

    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    public CohereEmbeddingType embeddingType() {
        return embeddingType;
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
    public String modelId() {
        return commonSettings.modelId();
    }

    @Override
    public CohereEmbeddingsServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();

        var updatedCommonSettings = commonSettings.update(serviceSettings, validationException);

        var extractedMaxInputTokens = extractOptionalPositiveInteger(
            serviceSettings,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        validationException.throwIfValidationErrorsExist();

        return new CohereEmbeddingsServiceSettings(
            updatedCommonSettings,
            this.similarity,
            this.dimensions,
            extractedMaxInputTokens != null ? extractedMaxInputTokens : this.maxInputTokens,
            this.embeddingType
        );
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
        commonSettings.toXContent(builder, params);
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        builder.field(ServiceFields.EMBEDDING_TYPE, elementType());
        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        commonSettings.toXContentFragmentOfExposedFields(builder, params);
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        builder.field(ServiceFields.EMBEDDING_TYPE, elementType());
        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(ML_INFERENCE_COHERE_SERVICE_SETTINGS_REFACTOR) == false) {
            // Old CohereServiceSettings wire format
            out.writeOptionalString(commonSettings.uri() != null ? commonSettings.uri().toString() : null);
            out.writeOptionalEnum(similarity);
            out.writeOptionalVInt(dimensions);
            out.writeOptionalVInt(maxInputTokens);
            out.writeOptionalString(commonSettings.modelId());
            commonSettings.rateLimitSettings().writeTo(out);
            if (out.getTransportVersion().supports(ML_INFERENCE_COHERE_API_VERSION)) {
                out.writeEnum(commonSettings.apiVersion());
            }
        } else {
            commonSettings.writeTo(out);
            out.writeOptionalEnum(similarity);
            out.writeOptionalVInt(dimensions);
            out.writeOptionalVInt(maxInputTokens);
        }
        out.writeOptionalEnum(CohereEmbeddingType.translateToVersion(embeddingType, out.getTransportVersion()));
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CohereEmbeddingsServiceSettings that = (CohereEmbeddingsServiceSettings) o;
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
