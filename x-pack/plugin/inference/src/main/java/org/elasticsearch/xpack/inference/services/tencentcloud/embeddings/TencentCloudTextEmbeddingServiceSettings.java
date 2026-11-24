/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.embeddings;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudCommonServiceSettings;

import java.io.IOException;
import java.util.Map;

/**
 * Service settings for the TencentCloud {@code text_embedding} task, which calls {@code /v1/embeddings}.
 */
public class TencentCloudTextEmbeddingServiceSettings extends BaseTencentCloudEmbeddingsServiceSettings {

    public static final String NAME = "tencentcloud_embeddings_service_settings";

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(false);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(true);

    static ObjectParser<Builder, ConfigurationParseContext> createParser(boolean isPersistentParser) {
        ObjectParser<Builder, ConfigurationParseContext> parser = new ObjectParser<>(
            ModelConfigurations.SERVICE_SETTINGS,
            isPersistentParser,
            Builder::new
        );
        declareEmbeddingsFields(parser, isPersistentParser);
        return parser;
    }

    public static TencentCloudTextEmbeddingServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        return TencentCloudCommonServiceSettings.fromMap(map, context, parser);
    }

    public TencentCloudTextEmbeddingServiceSettings(
        String modelId,
        String region,
        @Nullable RateLimitSettings rateLimitSettings,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens
    ) {
        super(modelId, region, rateLimitSettings, similarity, dimensions, maxInputTokens);
    }

    public TencentCloudTextEmbeddingServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected TencentCloudTextEmbeddingServiceSettings create(
        String modelId,
        String region,
        @Nullable RateLimitSettings rateLimitSettings,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens
    ) {
        return new TencentCloudTextEmbeddingServiceSettings(modelId, region, rateLimitSettings, similarity, dimensions, maxInputTokens);
    }

    @Override
    public TencentCloudTextEmbeddingServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        return (TencentCloudTextEmbeddingServiceSettings) super.updateServiceSettings(serviceSettings);
    }

    @Override
    public TencentCloudTextEmbeddingServiceSettings updateEmbeddingDetails(int newDimensions, SimilarityMeasure newSimilarity) {
        return (TencentCloudTextEmbeddingServiceSettings) super.updateEmbeddingDetails(newDimensions, newSimilarity);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public static class Builder extends BaseTencentCloudEmbeddingsServiceSettings.Builder<TencentCloudTextEmbeddingServiceSettings> {
        @Override
        protected TencentCloudTextEmbeddingServiceSettings build(
            String modelId,
            String region,
            RateLimitSettings rateLimitSettings,
            SimilarityMeasure similarity,
            Integer dimensions,
            Integer maxInputTokens
        ) {
            return new TencentCloudTextEmbeddingServiceSettings(modelId, region, rateLimitSettings, similarity, dimensions, maxInputTokens);
        }
    }
}
