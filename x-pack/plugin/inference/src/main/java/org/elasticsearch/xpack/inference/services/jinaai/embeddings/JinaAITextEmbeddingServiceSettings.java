/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.embeddings;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings;

import java.io.IOException;
import java.util.Map;

public class JinaAITextEmbeddingServiceSettings extends BaseJinaAIEmbeddingsServiceSettings {
    /**
     * This name is a holdover from before the introduction of {@link JinaAIEmbeddingServiceSettings} to support multimodal embeddings
     */
    public static final String NAME = "jinaai_embeddings_service_settings";

    public static JinaAITextEmbeddingServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return (JinaAITextEmbeddingServiceSettings) BaseJinaAIEmbeddingsServiceSettings.fromMap(map, TaskType.TEXT_EMBEDDING, context);
    }

    public JinaAITextEmbeddingServiceSettings(
        JinaAIServiceSettings commonServiceSettings,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dims,
        @Nullable Integer maxInputTokens,
        @Nullable JinaAIEmbeddingType embeddingTypes,
        boolean dimensionsSetByUser
    ) {
        super(commonServiceSettings, similarity, dims, maxInputTokens, embeddingTypes, dimensionsSetByUser, null);
    }

    public JinaAITextEmbeddingServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public boolean getDefaultMultimodal() {
        return false;
    }

    @Override
    public BaseJinaAIEmbeddingsServiceSettings update(SimilarityMeasure similarity, Integer dimensions) {
        return new JinaAITextEmbeddingServiceSettings(
            getCommonSettings(),
            similarity,
            dimensions,
            maxInputTokens(),
            getEmbeddingType(),
            dimensionsSetByUser()
        );
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
