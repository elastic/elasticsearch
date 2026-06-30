/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.embeddings;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAICommonServiceSettings;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.common.parser.StatefulValue.applyUpdate;

public class JinaAITextEmbeddingServiceSettings extends BaseJinaAIEmbeddingsServiceSettings {
    /**
     * This name is a holdover from before the introduction of {@link JinaAIEmbeddingServiceSettings} to support multimodal embeddings
     * This name cannot be changed due to backwards compatibility, but it should be 'jinaai_text_embedding_service_settings'
     */
    public static final String NAME = "jinaai_embeddings_service_settings";
    public static final boolean DEFAULT_MULTIMODAL_MODEL = false;

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(
        false,
        ConfigurationParseContext.REQUEST
    );
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(
        true,
        ConfigurationParseContext.PERSISTENT
    );

    static ObjectParser<Builder, ConfigurationParseContext> createParser(boolean ignoreUnknownFields, ConfigurationParseContext context) {
        ObjectParser<Builder, ConfigurationParseContext> parser = new ObjectParser<>(
            ModelConfigurations.SERVICE_SETTINGS,
            ignoreUnknownFields,
            () -> new Builder(context)
        );
        JinaAICommonServiceSettings.declareCommonFields(parser);
        BaseJinaAIEmbeddingsServiceSettings.declareEmbeddingFields(parser, context);
        // text_embedding is always non-multimodal, so multimodal_model is not a valid field: the strict REQUEST parser rejects it, and
        // the lenient PERSISTENT parser ignores it (a persisted text embedding config never carries this field).
        return parser;
    }

    public static JinaAITextEmbeddingServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        return JinaAICommonServiceSettings.fromMap(map, context, parser);
    }

    public JinaAITextEmbeddingServiceSettings(
        JinaAICommonServiceSettings commonServiceSettings,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable JinaAIEmbeddingType embeddingType,
        boolean dimensionsSetByUser
    ) {
        super(commonServiceSettings, similarity, dimensions, maxInputTokens, embeddingType, dimensionsSetByUser, DEFAULT_MULTIMODAL_MODEL);
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
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            return Update.PARSER.apply(xParser, null).mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse JinaAI text embeddings service settings update", e);
        }
    }

    @Override
    protected void optionallyWriteMultimodalField(XContentBuilder builder) {
        // Do not include the multimodal_model field for text_embedding, because it is always false
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Accumulates the embeddings fields and builds a {@link JinaAITextEmbeddingServiceSettings}, which is always non-multimodal.
     */
    static class Builder extends BaseJinaAIEmbeddingsServiceSettings.Builder<JinaAITextEmbeddingServiceSettings> {

        Builder(ConfigurationParseContext context) {
            super(context);
        }

        @Override
        protected JinaAITextEmbeddingServiceSettings construct(
            JinaAICommonServiceSettings commonSettings,
            SimilarityMeasure similarity,
            Integer dimensions,
            Integer maxInputTokens,
            JinaAIEmbeddingType embeddingType,
            boolean dimensionsSetByUser
        ) {
            return new JinaAITextEmbeddingServiceSettings(
                commonSettings,
                similarity,
                dimensions,
                maxInputTokens,
                embeddingType,
                dimensionsSetByUser
            );
        }
    }

    /**
     * Parses an update request, which may only contain the mutable {@code max_input_tokens} and {@code rate_limit} fields. Including
     * any immutable field causes the strict parser to reject the request.
     */
    private static class Update extends EmbeddingsUpdate {

        private static final ObjectParser<Update, Void> PARSER = new ObjectParser<>(ModelConfigurations.SERVICE_SETTINGS, Update::new);

        static {
            declareEmbeddingsUpdatableFields(PARSER);
        }

        public JinaAITextEmbeddingServiceSettings mergeInto(JinaAITextEmbeddingServiceSettings existing) {
            return new JinaAITextEmbeddingServiceSettings(
                existing.getCommonSettings().update(this),
                existing.similarity(),
                existing.dimensions(),
                applyUpdate(maxInputTokens, existing.maxInputTokens()),
                existing.getEmbeddingType(),
                existing.dimensionsSetByUser()
            );
        }
    }
}
