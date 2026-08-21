/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.inference.common.parser.EnumParser;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.parser.EnumParser.parseFromStringInObjectParserContext;
import static org.elasticsearch.xpack.inference.common.parser.NumberParser.validatePositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.ELEMENT_TYPE;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;

public class ElasticsearchInternalTextEmbeddingServiceSettings extends ElasticsearchInternalServiceSettings {

    public static final String NAME = "custom_eland_model_internal_text_embedding_service_settings";

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createTextEmbeddingParser(
        false,
        ConfigurationParseContext.REQUEST
    );
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createTextEmbeddingParser(
        true,
        ConfigurationParseContext.PERSISTENT
    );
    private static final ObjectParser<Builder, Void> SIMILARITY_AND_ELEMENT_TYPE_PARSER = createSimilarityAndElementTypeParser();

    /**
     * Creates a parser for the text embedding service settings. The {@code dimensions} field is declared only in the
     * {@link ConfigurationParseContext#PERSISTENT} context: dimensions are not accepted from user requests because validation
     * determines them after performing a request to the model, whereas persisted configurations store the determined value.
     *
     * @param ignoreUnknownFields whether unknown fields are tolerated; {@code false} for user requests, {@code true} for persisted config
     * @param context the parse context the returned parser is intended for
     */
    private static ObjectParser<Builder, ConfigurationParseContext> createTextEmbeddingParser(
        boolean ignoreUnknownFields,
        ConfigurationParseContext context
    ) {
        var parser = ElasticsearchInternalServiceSettings.createParser(ignoreUnknownFields, Builder::new);
        declareSimilarityAndElementType(parser);
        if (context == ConfigurationParseContext.PERSISTENT) {
            parser.declareField(Builder::setDimensions, p -> {
                int value = p.intValue();
                validatePositiveInteger(value, DIMENSIONS);
                return value;
            }, new ParseField(DIMENSIONS), ObjectParser.ValueType.INT);
        }
        return parser;
    }

    private static ObjectParser<Builder, Void> createSimilarityAndElementTypeParser() {
        ObjectParser<Builder, Void> parser = new ObjectParser<>(ModelConfigurations.SERVICE_SETTINGS, true, Builder::new);
        declareSimilarityAndElementType(parser);
        return parser;
    }

    private static <C> void declareSimilarityAndElementType(ObjectParser<Builder, C> parser) {
        parser.declareString(Builder::setSimilarity, new ParseField(SIMILARITY));
        parser.declareString(Builder::setElementType, new ParseField(ELEMENT_TYPE));
    }

    /**
     * Parse the text embedding service settings from map and validate the setting values.
     *
     * This method does not verify the model variant
     *
     * If required setting are missing or the values are invalid an
     * {@link ValidationException} is thrown.
     *
     * @param map Source map containing the config
     * @param context The parser context, whether it is from an HTTP request or from persistent storage
     * @return The parsed and validated service settings
     */
    public static ElasticsearchInternalTextEmbeddingServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;

        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            var builder = parser.apply(xParser, context);
            // TODO: remove once all elasticsearch internal service settings are parser-based and ElasticsearchInternalService no
            // longer checks for unconsumed map entries. The object parser reads the map through an XContent view without consuming
            // its entries, so the parsed fields must be removed explicitly to satisfy the caller's check that no unknown settings
            // remain in the map.
            map.remove(NUM_ALLOCATIONS);
            map.remove(NUM_THREADS);
            map.remove(MODEL_ID);
            map.remove(DEPLOYMENT_ID);
            map.remove(ADAPTIVE_ALLOCATIONS);
            map.remove(SIMILARITY);
            map.remove(ELEMENT_TYPE);
            map.remove(DIMENSIONS);

            ElasticsearchInternalServiceSettings.validateRequiredFields(builder);
            return builder.build();
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, ModelConfigurations.SERVICE_SETTINGS);
        }
    }

    /**
     * Parse the similarity and element type from the request map, layering them onto a common settings
     * builder populated from another source (e.g. an existing ML deployment's assignment stats rather
     * than the map itself). Dimensions are left null; validation determines them after performing a
     * request to the model. Only the two parsed fields are consumed from the map so that any remaining
     * entries still fail the caller's check for unknown settings.
     */
    public static ElasticsearchInternalTextEmbeddingServiceSettings fromMap(
        Map<String, Object> map,
        ElasticsearchInternalServiceSettings.Builder commonSettingsBuilder
    ) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            var builder = SIMILARITY_AND_ELEMENT_TYPE_PARSER.apply(xParser, null);
            map.remove(SIMILARITY);
            map.remove(ELEMENT_TYPE);
            return new ElasticsearchInternalTextEmbeddingServiceSettings(
                commonSettingsBuilder.build(),
                null,
                builder.similarityOrDefault(),
                builder.elementTypeOrDefault()
            );
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, ModelConfigurations.SERVICE_SETTINGS);
        }
    }

    /**
     * Builder for the text embedding settings: extends the base builder with the similarity, element type and dimensions fields
     * declared by {@link #createTextEmbeddingParser}. Similarity and element type default to cosine and float when absent.
     */
    public static class Builder extends ElasticsearchInternalServiceSettings.Builder {
        private SimilarityMeasure similarityMeasure;
        private DenseVectorFieldMapper.ElementType elementType;
        private Integer dimensions;

        public void setSimilarity(String similarity) {
            this.similarityMeasure = EnumParser.parseSimilarity(similarity);
        }

        public void setElementType(String elementType) {
            this.elementType = parseFromStringInObjectParserContext(
                elementType,
                DenseVectorFieldMapper.ElementType::fromString,
                EnumSet.of(DenseVectorFieldMapper.ElementType.BYTE, DenseVectorFieldMapper.ElementType.FLOAT),
                EnumSet.noneOf(DenseVectorFieldMapper.ElementType.class)
            );
        }

        public void setDimensions(Integer dimensions) {
            this.dimensions = dimensions;
        }

        SimilarityMeasure similarityOrDefault() {
            return Objects.requireNonNullElse(similarityMeasure, SimilarityMeasure.COSINE);
        }

        DenseVectorFieldMapper.ElementType elementTypeOrDefault() {
            return Objects.requireNonNullElse(elementType, DenseVectorFieldMapper.ElementType.FLOAT);
        }

        @Override
        public ElasticsearchInternalTextEmbeddingServiceSettings build() {
            return new ElasticsearchInternalTextEmbeddingServiceSettings(
                super.build(),
                dimensions,
                similarityOrDefault(),
                elementTypeOrDefault()
            );
        }
    }

    private final Integer dimensions;
    private final SimilarityMeasure similarityMeasure;
    private final DenseVectorFieldMapper.ElementType elementType;

    ElasticsearchInternalTextEmbeddingServiceSettings(
        @Nullable Integer numAllocations,
        int numThreads,
        String modelId,
        @Nullable AdaptiveAllocationsSettings adaptiveAllocationsSettings,
        @Nullable String deploymentId,
        @Nullable Integer dimensions,
        SimilarityMeasure similarityMeasure,
        DenseVectorFieldMapper.ElementType elementType
    ) {
        super(numAllocations, numThreads, modelId, adaptiveAllocationsSettings, deploymentId);
        this.dimensions = dimensions;
        this.similarityMeasure = Objects.requireNonNull(similarityMeasure);
        this.elementType = Objects.requireNonNull(elementType);
    }

    public ElasticsearchInternalTextEmbeddingServiceSettings(StreamInput in) throws IOException {
        super(in);
        dimensions = in.readOptionalVInt();
        similarityMeasure = in.readEnum(SimilarityMeasure.class);
        elementType = in.readEnum(DenseVectorFieldMapper.ElementType.class);
    }

    ElasticsearchInternalTextEmbeddingServiceSettings(
        ElasticsearchInternalServiceSettings internalServiceSettings,
        @Nullable Integer dimensions,
        SimilarityMeasure similarityMeasure,
        DenseVectorFieldMapper.ElementType elementType
    ) {
        super(internalServiceSettings);
        this.dimensions = dimensions;
        this.similarityMeasure = Objects.requireNonNull(similarityMeasure);
        this.elementType = Objects.requireNonNull(elementType);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        addInternalSettingsToXContent(builder, params);

        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }

        if (similarityMeasure != null) {
            builder.field(SIMILARITY, similarityMeasure);
        }

        if (elementType != null) {
            builder.field(ELEMENT_TYPE, elementType);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return ElasticsearchInternalTextEmbeddingServiceSettings.NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalVInt(dimensions);
        out.writeEnum(similarityMeasure);
        out.writeEnum(elementType);
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return elementType;
    }

    @Override
    public SimilarityMeasure similarity() {
        return similarityMeasure;
    }

    @Override
    public Integer dimensions() {
        return dimensions;
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticsearchInternalTextEmbeddingServiceSettings that = (ElasticsearchInternalTextEmbeddingServiceSettings) o;
        return super.equals(that)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(similarityMeasure, that.similarityMeasure)
            && Objects.equals(elementType, that.elementType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dimensions, similarityMeasure, elementType);
    }

    @Override
    public ServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        ServiceSettings updated = super.updateServiceSettings(serviceSettings);
        if (updated instanceof ElasticsearchInternalServiceSettings esSettings) {
            return new ElasticsearchInternalTextEmbeddingServiceSettings(esSettings, dimensions, similarityMeasure, elementType);
        } else {
            throw new IllegalStateException("Unexpected service settings type [" + updated.getClass().getName() + "]");
        }
    }
}
