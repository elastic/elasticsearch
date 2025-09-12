/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService.RERANKER_ID;

public class ElasticRerankerServiceSettings extends ElasticsearchInternalServiceSettings {

    public static final String NAME = "elastic_reranker_service_settings";

    private static final String LONG_DOCUMENT_HANDLING_STRATEGY = "long_document_handling_strategy";
    private static final String MAX_CHUNKS_PER_DOC = "max_chunks_per_doc";

    private final LongDocumentHandlingStrategy longDocumentHandlingStrategy;
    private final Integer maxChunksPerDoc;

    public static ElasticRerankerServiceSettings defaultEndpointSettings() {
        return new ElasticRerankerServiceSettings(null, 1, RERANKER_ID, new AdaptiveAllocationsSettings(Boolean.TRUE, 0, 32));
    }

    public ElasticRerankerServiceSettings(ElasticsearchInternalServiceSettings other) {
        super(other);
        this.longDocumentHandlingStrategy = null;
        this.maxChunksPerDoc = null;
    }

    public ElasticRerankerServiceSettings(
        ElasticsearchInternalServiceSettings other,
        LongDocumentHandlingStrategy longDocumentHandlingStrategy,
        Integer maxChunksPerDoc
    ) {
        super(other);
        this.longDocumentHandlingStrategy = longDocumentHandlingStrategy;
        this.maxChunksPerDoc = maxChunksPerDoc;

    }

    private ElasticRerankerServiceSettings(
        Integer numAllocations,
        int numThreads,
        String modelId,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings
    ) {
        super(numAllocations, numThreads, modelId, adaptiveAllocationsSettings, null);
        this.longDocumentHandlingStrategy = null;
        this.maxChunksPerDoc = null;
    }

    public ElasticRerankerServiceSettings(StreamInput in) throws IOException {
        super(in);
        // TODO: Add transport version here
        this.longDocumentHandlingStrategy = in.readOptionalEnum(LongDocumentHandlingStrategy.class);
        this.maxChunksPerDoc = in.readOptionalInt();
    }

    /**
     * Parse the ElasticRerankerServiceSettings from map and validate the setting values.
     *
     * If required setting are missing or the values are invalid an
     * {@link ValidationException} is thrown.
     *
     * @param map Source map containing the config
     * @return Parsed and validated service settings
     */
    public static ElasticRerankerServiceSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();
        var baseSettings = ElasticsearchInternalServiceSettings.fromMap(map, validationException);

        LongDocumentHandlingStrategy longDocumentHandlingStrategy = extractOptionalEnum(
            map,
            LONG_DOCUMENT_HANDLING_STRATEGY,
            ModelConfigurations.SERVICE_SETTINGS,
            LongDocumentHandlingStrategy::fromString,
            EnumSet.allOf(LongDocumentHandlingStrategy.class),
            validationException
        );

        Integer maxChunksPerDoc = extractOptionalPositiveInteger(
            map,
            MAX_CHUNKS_PER_DOC,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        if (maxChunksPerDoc != null
            && (longDocumentHandlingStrategy == null || longDocumentHandlingStrategy == LongDocumentHandlingStrategy.TRUNCATE)) {
            validationException.addValidationError(
                "The [" + MAX_CHUNKS_PER_DOC + "] setting requires [" + LONG_DOCUMENT_HANDLING_STRATEGY + "] to be set to [chunk]"
            );
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new ElasticRerankerServiceSettings(baseSettings.build(), longDocumentHandlingStrategy, maxChunksPerDoc);
    }

    public LongDocumentHandlingStrategy getLongDocumentHandlingStrategy() {
        return longDocumentHandlingStrategy;
    }

    public Integer getMaxChunksPerDoc() {
        return maxChunksPerDoc;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        // TODO: Add transport version here
        out.writeOptionalEnum(longDocumentHandlingStrategy);
        out.writeOptionalInt(maxChunksPerDoc);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        addInternalSettingsToXContent(builder, params);
        if (longDocumentHandlingStrategy != null) {
            builder.field(LONG_DOCUMENT_HANDLING_STRATEGY, longDocumentHandlingStrategy.strategyName);
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

    public enum LongDocumentHandlingStrategy {
        CHUNK("chunk"),
        TRUNCATE("truncate");

        public final String strategyName;

        LongDocumentHandlingStrategy(String strategyName) {
            this.strategyName = strategyName;
        }

        public static LongDocumentHandlingStrategy fromString(String name) {
            return valueOf(name.trim().toUpperCase(Locale.ROOT));
        }
    }
}
