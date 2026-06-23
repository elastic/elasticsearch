/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.common.parser.EnumParser;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettings.CommonUpdate;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.parser.NumberParser.validatePositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettings.ML_INFERENCE_COHERE_API_VERSION;
import static org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettings.ML_INFERENCE_COHERE_SERVICE_SETTINGS_REFACTOR;

/**
 * Settings for the Cohere embeddings service. Wraps {@link CohereCommonServiceSettings} and adds
 * embeddings-specific fields: similarity measure, dimensions, max input tokens, and embedding type.
 */
public class CohereEmbeddingsServiceSettings extends FilteredXContentObject implements CohereServiceSettings {

    public static final String NAME = "cohere_embeddings_service_settings";

    static class Builder extends CohereCommonServiceSettings.Builder<CohereEmbeddingsServiceSettings> {

        private SimilarityMeasure similarity;
        private Integer dimensions;
        private Integer maxInputTokens;
        private CohereEmbeddingType embeddingType;

        Builder(ConfigurationParseContext context) {
            super(context);
        }

        void setSimilarity(SimilarityMeasure similarity) {
            this.similarity = similarity;
        }

        void setDimensions(Integer dimensions) {
            validatePositiveInteger(dimensions, DIMENSIONS);
            this.dimensions = dimensions;
        }

        void setMaxInputTokens(Integer maxInputTokens) {
            validatePositiveInteger(maxInputTokens, MAX_INPUT_TOKENS);
            this.maxInputTokens = maxInputTokens;
        }

        void setEmbeddingType(CohereEmbeddingType embeddingType) {
            this.embeddingType = embeddingType;
        }

        @Override
        protected CohereEmbeddingsServiceSettings build(CohereCommonServiceSettings commonSettings) {
            var resolvedEmbeddingType = Objects.requireNonNullElse(embeddingType, CohereEmbeddingType.FLOAT);
            return new CohereEmbeddingsServiceSettings(commonSettings, similarity, dimensions, maxInputTokens, resolvedEmbeddingType);
        }
    }

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(
        false,
        ConfigurationParseContext.REQUEST
    );
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(
        true,
        ConfigurationParseContext.PERSISTENT
    );

    static ObjectParser<Builder, ConfigurationParseContext> createParser(boolean ignoreUnknownFields, ConfigurationParseContext context) {
        ObjectParser<Builder, ConfigurationParseContext> parser = new ObjectParser<>(
            ModelConfigurations.SERVICE_SETTINGS,
            ignoreUnknownFields,
            () -> new Builder(context)
        );
        CohereCommonServiceSettings.declareCommonFields(parser, context);
        parser.declareString(Builder::setSimilarity, EnumParser::parseSimilarity, new ParseField(SIMILARITY));
        parser.declareInt(Builder::setDimensions, new ParseField(DIMENSIONS));
        parser.declareInt(Builder::setMaxInputTokens, new ParseField(MAX_INPUT_TOKENS));
        parser.declareString(
            Builder::setEmbeddingType,
            CohereEmbeddingType::fromCohereOrElementType,
            new ParseField(ServiceFields.EMBEDDING_TYPE)
        );
        return parser;
    }

    /**
     * Creates {@link CohereEmbeddingsServiceSettings} from a map of settings.
     *
     * @param map     the map to parse
     * @param context the context in which the parsing is done
     * @return the created {@link CohereEmbeddingsServiceSettings}
     */
    public static CohereEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        return CohereCommonServiceSettings.fromMap(map, context, parser);
    }

    private static class Update extends CommonUpdate {

        private static final ObjectParser<Update, Void> PARSER = new ObjectParser<>(ModelConfigurations.SERVICE_SETTINGS, Update::new);

        static {
            CohereCommonServiceSettings.declareCommonUpdatableFields(PARSER);
            PARSER.declareInt(Update::setMaxInputTokens, new ParseField(MAX_INPUT_TOKENS));
        }

        private Integer maxInputTokens;

        private void setMaxInputTokens(Integer maxInputTokens) {
            validatePositiveInteger(maxInputTokens, MAX_INPUT_TOKENS);
            this.maxInputTokens = maxInputTokens;
        }

        public CohereEmbeddingsServiceSettings mergeInto(CohereEmbeddingsServiceSettings existing) {
            Integer updatedMaxInputTokens = maxInputTokens != null ? maxInputTokens : existing.maxInputTokens();
            return new CohereEmbeddingsServiceSettings(
                existing.commonSettings().update(this),
                existing.similarity(),
                existing.dimensions(),
                updatedMaxInputTokens,
                existing.embeddingType()
            );
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
        this.embeddingType = Objects.requireNonNull(embeddingType).normalize();
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
        this.embeddingType = Objects.requireNonNullElse(in.readOptionalEnum(CohereEmbeddingType.class), CohereEmbeddingType.FLOAT)
            .normalize();
    }

    @Override
    public CohereCommonServiceSettings commonSettings() {
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
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            Update update = Update.PARSER.apply(xParser, null);
            return update.mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse Cohere embeddings service settings update", e);
        }
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return embeddingType.toElementType();
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
