/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.inference.common.parser.EnumParser.parseFromStringInObjectParserContext;
import static org.elasticsearch.xpack.inference.common.parser.NumberParser.validatePositiveInteger;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService.RERANKER_ID;

public class ElasticRerankerServiceSettings extends ElasticsearchInternalServiceSettings {

    public static final String NAME = "elastic_reranker_service_settings";

    public static final String LONG_DOCUMENT_STRATEGY = "long_document_strategy";
    public static final String MAX_CHUNKS_PER_DOC = "max_chunks_per_doc";

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createRerankerParser(false);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createRerankerParser(true);

    private static final TransportVersion ELASTIC_RERANKER_CHUNKING_CONFIGURATION = TransportVersion.fromName(
        "elastic_reranker_chunking_configuration"
    );

    private static ObjectParser<Builder, ConfigurationParseContext> createRerankerParser(boolean ignoreUnknownFields) {
        var parser = ElasticsearchInternalServiceSettings.createParser(ignoreUnknownFields, Builder::new);
        parser.declareString(Builder::setLongDocumentStrategy, new ParseField(LONG_DOCUMENT_STRATEGY));
        parser.declareField(Builder::setMaxChunksPerDoc, p -> {
            int value = p.intValue();
            validatePositiveInteger(value, MAX_CHUNKS_PER_DOC);
            return value;
        }, new ParseField(MAX_CHUNKS_PER_DOC), ObjectParser.ValueType.INT);
        return parser;
    }

    private final LongDocumentStrategy longDocumentStrategy;
    private final Integer maxChunksPerDoc;

    public static ElasticRerankerServiceSettings defaultEndpointSettings() {
        return new ElasticRerankerServiceSettings(null, 1, RERANKER_ID, new AdaptiveAllocationsSettings(Boolean.TRUE, 0, 32));
    }

    public ElasticRerankerServiceSettings(
        ElasticsearchInternalServiceSettings other,
        LongDocumentStrategy longDocumentStrategy,
        Integer maxChunksPerDoc
    ) {
        super(other);
        this.longDocumentStrategy = longDocumentStrategy;
        this.maxChunksPerDoc = maxChunksPerDoc;

    }

    private ElasticRerankerServiceSettings(
        Integer numAllocations,
        int numThreads,
        String modelId,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings
    ) {
        super(numAllocations, numThreads, modelId, adaptiveAllocationsSettings, null);
        this.longDocumentStrategy = null;
        this.maxChunksPerDoc = null;
    }

    protected ElasticRerankerServiceSettings(
        Integer numAllocations,
        int numThreads,
        String modelId,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings,
        LongDocumentStrategy longDocumentStrategy,
        Integer maxChunksPerDoc
    ) {
        super(numAllocations, numThreads, modelId, adaptiveAllocationsSettings, null);
        this.longDocumentStrategy = longDocumentStrategy;
        this.maxChunksPerDoc = maxChunksPerDoc;
    }

    public ElasticRerankerServiceSettings(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().supports(ELASTIC_RERANKER_CHUNKING_CONFIGURATION)) {
            this.longDocumentStrategy = in.readOptionalEnum(LongDocumentStrategy.class);
            this.maxChunksPerDoc = in.readOptionalInt();
        } else {
            this.longDocumentStrategy = null;
            this.maxChunksPerDoc = null;
        }
    }

    /**
     * Parse the ElasticRerankerServiceSettings from map and validate the setting values.
     *
     * If required setting are missing or the values are invalid an
     * {@link ValidationException} is thrown.
     *
     * @param map Source map containing the config
     * @param context The parser context, whether it is from an HTTP request or from persistent storage
     * @return Parsed and validated service settings
     */
    public static ElasticRerankerServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
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
            map.remove(LONG_DOCUMENT_STRATEGY);
            map.remove(MAX_CHUNKS_PER_DOC);

            ElasticsearchInternalServiceSettings.validateRequiredFields(builder);
            validateChunkingRule(builder);
            return builder.build();
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, ModelConfigurations.SERVICE_SETTINGS);
        }
    }

    private static void validateChunkingRule(Builder builder) {
        if (builder.maxChunksPerDoc != null
            && (builder.longDocumentStrategy == null || builder.longDocumentStrategy == LongDocumentStrategy.TRUNCATE)) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationError(
                "The [" + MAX_CHUNKS_PER_DOC + "] setting requires [" + LONG_DOCUMENT_STRATEGY + "] to be set to [chunk]"
            );
            throw validationException;
        }
    }

    /**
     * Builder for the reranker settings: extends the base builder with the chunking configuration fields declared by
     * {@link #createRerankerParser}.
     */
    public static class Builder extends ElasticsearchInternalServiceSettings.Builder {
        private LongDocumentStrategy longDocumentStrategy;
        private Integer maxChunksPerDoc;

        public void setLongDocumentStrategy(String longDocumentStrategy) {
            this.longDocumentStrategy = parseFromStringInObjectParserContext(
                longDocumentStrategy,
                LongDocumentStrategy::fromString,
                EnumSet.allOf(LongDocumentStrategy.class),
                EnumSet.noneOf(LongDocumentStrategy.class)
            );
        }

        public void setMaxChunksPerDoc(Integer maxChunksPerDoc) {
            this.maxChunksPerDoc = maxChunksPerDoc;
        }

        @Override
        public ElasticRerankerServiceSettings build() {
            return new ElasticRerankerServiceSettings(super.build(), longDocumentStrategy, maxChunksPerDoc);
        }
    }

    public LongDocumentStrategy getLongDocumentStrategy() {
        return longDocumentStrategy;
    }

    public Integer getMaxChunksPerDoc() {
        return maxChunksPerDoc;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().supports(ELASTIC_RERANKER_CHUNKING_CONFIGURATION)) {
            out.writeOptionalEnum(longDocumentStrategy);
            out.writeOptionalInt(maxChunksPerDoc);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        addInternalSettingsToXContent(builder, params);
        if (longDocumentStrategy != null) {
            builder.field(LONG_DOCUMENT_STRATEGY, longDocumentStrategy.strategyName);
        }
        if (maxChunksPerDoc != null) {
            builder.field(MAX_CHUNKS_PER_DOC, maxChunksPerDoc);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return ElasticRerankerServiceSettings.NAME;
    }

    public enum LongDocumentStrategy {
        CHUNK("chunk"),
        TRUNCATE("truncate");

        public final String strategyName;

        LongDocumentStrategy(String strategyName) {
            this.strategyName = strategyName;
        }

        public static LongDocumentStrategy fromString(String name) {
            return valueOf(name.trim().toUpperCase(Locale.ROOT));
        }
    }

    @Override
    public ServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        ServiceSettings updated = super.updateServiceSettings(serviceSettings);
        if (updated instanceof ElasticsearchInternalServiceSettings esSettings) {
            return new ElasticRerankerServiceSettings(esSettings, longDocumentStrategy, maxChunksPerDoc);
        } else {
            throw new IllegalStateException("Unexpected service settings type [" + updated.getClass().getName() + "]");
        }
    }
}
