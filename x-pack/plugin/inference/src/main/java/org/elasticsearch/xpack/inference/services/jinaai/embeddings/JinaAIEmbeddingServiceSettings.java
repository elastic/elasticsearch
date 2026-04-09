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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MULTIMODAL_MODEL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeAsType;

public class JinaAIEmbeddingServiceSettings extends BaseJinaAIEmbeddingsServiceSettings {
    public static final String NAME = "jinaai_multimodal_embedding_service_settings";
    public static final boolean DEFAULT_MULTIMODAL_MODEL = true;

    public static JinaAIEmbeddingServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return BaseJinaAIEmbeddingsServiceSettings.fromMap(
            map,
            context,
            (m, v) -> Objects.requireNonNullElse(removeAsType(m, MULTIMODAL_MODEL, Boolean.class, v), DEFAULT_MULTIMODAL_MODEL),
            JinaAIEmbeddingServiceSettings::new
        );
    }

    public JinaAIEmbeddingServiceSettings(
        JinaAIServiceSettings commonSettings,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable JinaAIEmbeddingType embeddingType,
        boolean dimensionsSetByUser,
        boolean multimodalModel
    ) {
        super(commonSettings, similarity, dimensions, maxInputTokens, embeddingType, dimensionsSetByUser, multimodalModel);
    }

    public JinaAIEmbeddingServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public BaseJinaAIEmbeddingsServiceSettings update(SimilarityMeasure similarity, Integer dimensions) {
        return new JinaAIEmbeddingServiceSettings(
            getCommonSettings(),
            similarity,
            dimensions,
            maxInputTokens(),
            getEmbeddingType(),
            dimensionsSetByUser(),
            isMultimodal()
        );
    }

    @Override
    protected void optionallyWriteMultimodalField(XContentBuilder builder) throws IOException {
        builder.field(MULTIMODAL_MODEL, isMultimodal());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
