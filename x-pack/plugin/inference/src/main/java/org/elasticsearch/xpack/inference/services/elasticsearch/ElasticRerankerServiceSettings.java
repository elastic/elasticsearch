/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService.RERANKER_ID;

public class ElasticRerankerServiceSettings extends ElasticsearchInternalServiceSettings {

    public static final String NAME = "elastic_reranker_service_settings";

    public static final String LONG_DOCUMENT_STRATEGY = "long_document_strategy";
    public static final String MAX_CHUNKS_PER_DOC = "max_chunks_per_doc";

    private static final TransportVersion ELASTIC_RERANKER_CHUNKING_CONFIGURATION = TransportVersion.fromName(
        "elastic_reranker_chunking_configuration"
    );

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
     * @return Parsed and validated service settings
     */
    public static ElasticRerankerServiceSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();
        var baseSettings = ElasticsearchInternalServiceSettings.fromMap(map, validationException);

        LongDocumentStrategy longDocumentStrategy = extractOptionalEnum(
            map,
            LONG_DOCUMENT_STRATEGY,
            ModelConfigurations.SERVICE_SETTINGS,
            LongDocumentStrategy::fromString,
            EnumSet.allOf(LongDocumentStrategy.class),
            validationException
        );

        Integer maxChunksPerDoc = extractOptionalPositiveInteger(
            map,
            MAX_CHUNKS_PER_DOC,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        if (maxChunksPerDoc != null && (longDocumentStrategy == null || longDocumentStrategy == LongDocumentStrategy.TRUNCATE)) {
            validationException.addValidationError(
                "The [" + MAX_CHUNKS_PER_DOC + "] setting requires [" + LONG_DOCUMENT_STRATEGY + "] to be set to [chunk]"
            );
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new ElasticRerankerServiceSettings(baseSettings.build(), longDocumentStrategy, maxChunksPerDoc);
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
        serviceSettings = new HashMap<>(serviceSettings);
        ServiceSettings updated = super.updateServiceSettings(serviceSettings);
        if (updated instanceof ElasticsearchInternalServiceSettings esSettings) {
            return new ElasticRerankerServiceSettings(esSettings, longDocumentStrategy, maxChunksPerDoc);
        } else {
            throw new IllegalStateException("Unexpected service settings type [" + updated.getClass().getName() + "]");
        }
    }
}
