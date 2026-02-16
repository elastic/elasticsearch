/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.embeddings;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceSettings;

import java.io.IOException;
import java.util.Map;

/**
 * Service settings for the VoyageAI TEXT_EMBEDDING task type.
 * This name is a holdover from before the introduction of {@link VoyageAIEmbeddingServiceSettings} to support multimodal embeddings.
 * This name cannot be changed due to backwards compatibility, but it should be 'voyageai_text_embedding_service_settings'.
 */
public class VoyageAIEmbeddingsServiceSettings extends BaseVoyageAIEmbeddingsServiceSettings {
    public static final String NAME = "voyageai_embeddings_service_settings";
    public static final boolean DEFAULT_MULTIMODAL_MODEL = false;

    public static VoyageAIEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return BaseVoyageAIEmbeddingsServiceSettings.fromMap(
            map,
            context,
            (m, v) -> DEFAULT_MULTIMODAL_MODEL,
            VoyageAIEmbeddingsServiceSettings::new
        );
    }

    public VoyageAIEmbeddingsServiceSettings(
        VoyageAIServiceSettings commonSettings,
        @Nullable VoyageAIEmbeddingType embeddingType,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        boolean dimensionsSetByUser,
        boolean multimodalModel
    ) {
        super(commonSettings, embeddingType, similarity, dimensions, maxInputTokens, dimensionsSetByUser, DEFAULT_MULTIMODAL_MODEL);
    }

    public VoyageAIEmbeddingsServiceSettings(
        VoyageAIServiceSettings commonSettings,
        @Nullable VoyageAIEmbeddingType embeddingType,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        boolean dimensionsSetByUser
    ) {
        this(commonSettings, embeddingType, similarity, dimensions, maxInputTokens, dimensionsSetByUser, DEFAULT_MULTIMODAL_MODEL);
    }

    public VoyageAIEmbeddingsServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public BaseVoyageAIEmbeddingsServiceSettings update(SimilarityMeasure similarity, Integer dimensions) {
        return new VoyageAIEmbeddingsServiceSettings(
            getCommonSettings(),
            getEmbeddingType(),
            similarity,
            dimensions,
            maxInputTokens(),
            dimensionsSetByUser()
        );
    }

    @Override
    protected void optionallyWriteMultimodalField(XContentBuilder builder) {
        // Do not include the multimodal_model field for text_embedding, because it is always false
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
