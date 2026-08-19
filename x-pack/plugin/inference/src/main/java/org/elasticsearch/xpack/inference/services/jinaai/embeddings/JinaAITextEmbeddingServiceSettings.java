/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.embeddings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAICommonServiceSettings;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;

public class JinaAITextEmbeddingServiceSettings extends BaseJinaAIEmbeddingsServiceSettings {
    /**
     * This name is a holdover from before the introduction of {@link JinaAIEmbeddingServiceSettings} to support multimodal embeddings
     * This name cannot be changed due to backwards compatibility, but it should be 'jinaai_text_embedding_service_settings'
     */
    public static final String NAME = "jinaai_embeddings_service_settings";
    public static final boolean DEFAULT_MULTIMODAL_MODEL = false;

    public static JinaAITextEmbeddingServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return BaseJinaAIEmbeddingsServiceSettings.fromMap(
            map,
            context,
            (m, v) -> DEFAULT_MULTIMODAL_MODEL,
            JinaAITextEmbeddingServiceSettings::new
        );
    }

    private JinaAITextEmbeddingServiceSettings(
        JinaAICommonServiceSettings commonServiceSettings,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable JinaAIEmbeddingType embeddingType,
        boolean dimensionsSetByUser,
        boolean multimodalModel
    ) {
        super(commonServiceSettings, similarity, dimensions, maxInputTokens, embeddingType, dimensionsSetByUser, DEFAULT_MULTIMODAL_MODEL);
    }

    public JinaAITextEmbeddingServiceSettings(
        JinaAICommonServiceSettings commonServiceSettings,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable JinaAIEmbeddingType embeddingType,
        boolean dimensionsSetByUser
    ) {
        this(commonServiceSettings, similarity, dimensions, maxInputTokens, embeddingType, dimensionsSetByUser, DEFAULT_MULTIMODAL_MODEL);
    }

    public JinaAITextEmbeddingServiceSettings(StreamInput in) throws IOException {
        super(in);
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
    public JinaAITextEmbeddingServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();

        var extractedMaxInputTokens = extractOptionalPositiveInteger(
            serviceSettings,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        var updatedCommonServiceSettings = getCommonSettings().updateCommonServiceSettings(serviceSettings, validationException);

        validationException.throwIfValidationErrorsExist();

        return new JinaAITextEmbeddingServiceSettings(
            updatedCommonServiceSettings,
            this.similarity(),
            this.dimensions(),
            extractedMaxInputTokens != null ? extractedMaxInputTokens : this.maxInputTokens(),
            this.getEmbeddingType(),
            this.dimensionsSetByUser()
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
