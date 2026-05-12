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
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MULTIMODAL_MODEL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeAsType;

/**
 * Service settings for the VoyageAI EMBEDDING task type.
 * This class supports multimodal embeddings (text + images).
 */
public class VoyageAIEmbeddingServiceSettings extends BaseVoyageAIEmbeddingsServiceSettings {
    public static final String NAME = "voyageai_multimodal_embedding_service_settings";
    public static final boolean DEFAULT_MULTIMODAL_MODEL = true;

    public static VoyageAIEmbeddingServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return BaseVoyageAIEmbeddingsServiceSettings.fromMap(
            map,
            context,
            (m, v) -> Objects.requireNonNullElse(removeAsType(m, MULTIMODAL_MODEL, Boolean.class, v), DEFAULT_MULTIMODAL_MODEL),
            VoyageAIEmbeddingServiceSettings::new
        );
    }

    public VoyageAIEmbeddingServiceSettings(
        VoyageAIServiceSettings commonSettings,
        @Nullable VoyageAIEmbeddingType embeddingType,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        boolean dimensionsSetByUser,
        boolean multimodalModel
    ) {
        super(commonSettings, embeddingType, similarity, dimensions, maxInputTokens, dimensionsSetByUser, multimodalModel);
    }

    public VoyageAIEmbeddingServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public BaseVoyageAIEmbeddingsServiceSettings update(SimilarityMeasure similarity, Integer dimensions) {
        return new VoyageAIEmbeddingServiceSettings(
            getCommonSettings(),
            getEmbeddingType(),
            similarity,
            dimensions,
            maxInputTokens(),
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
