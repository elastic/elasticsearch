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
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAICommonServiceSettings;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.common.parser.StatefulValue.applyUpdate;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MULTIMODAL_MODEL;

public class JinaAIEmbeddingServiceSettings extends BaseJinaAIEmbeddingsServiceSettings {
    public static final String NAME = "jinaai_multimodal_embedding_service_settings";
    public static final boolean DEFAULT_MULTIMODAL_MODEL = true;

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
        parser.declareBoolean(Builder::setMultimodalModel, new ParseField(MULTIMODAL_MODEL));
        return parser;
    }

    public static JinaAIEmbeddingServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        return JinaAICommonServiceSettings.fromMap(map, context, parser);
    }

    public JinaAIEmbeddingServiceSettings(
        JinaAICommonServiceSettings commonSettings,
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
    public JinaAIEmbeddingServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            return Update.PARSER.apply(xParser, null).mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse JinaAI multimodal embeddings service settings update", e);
        }
    }

    @Override
    protected void optionallyWriteMultimodalField(XContentBuilder builder) throws IOException {
        builder.field(MULTIMODAL_MODEL, isMultimodal());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Accumulates the embeddings fields and builds a {@link JinaAIEmbeddingServiceSettings}. The {@code multimodal_model} flag
     * defaults to {@code true} and may be overridden by the request.
     */
    static class Builder extends BaseJinaAIEmbeddingsServiceSettings.Builder<JinaAIEmbeddingServiceSettings> {

        Builder(ConfigurationParseContext context) {
            super(context);
            this.multimodalModel = DEFAULT_MULTIMODAL_MODEL;
        }

        @Override
        protected JinaAIEmbeddingServiceSettings construct(
            JinaAICommonServiceSettings commonSettings,
            SimilarityMeasure similarity,
            Integer dimensions,
            Integer maxInputTokens,
            JinaAIEmbeddingType embeddingType,
            boolean dimensionsSetByUser
        ) {
            return new JinaAIEmbeddingServiceSettings(
                commonSettings,
                similarity,
                dimensions,
                maxInputTokens,
                embeddingType,
                dimensionsSetByUser,
                multimodalModel
            );
        }
    }

    /**
     * Parses an update request, which may only contain the mutable {@code max_input_tokens} and {@code rate_limit} fields. Including
     * any immutable field (including {@code multimodal_model}) causes the strict parser to reject the request.
     */
    private static class Update extends EmbeddingsUpdate {

        private static final ObjectParser<Update, Void> PARSER = new ObjectParser<>(ModelConfigurations.SERVICE_SETTINGS, Update::new);

        static {
            declareEmbeddingsUpdatableFields(PARSER);
        }

        public JinaAIEmbeddingServiceSettings mergeInto(JinaAIEmbeddingServiceSettings existing) {
            return new JinaAIEmbeddingServiceSettings(
                existing.getCommonSettings().update(this),
                existing.similarity(),
                existing.dimensions(),
                applyUpdate(maxInputTokens, existing.maxInputTokens()),
                existing.getEmbeddingType(),
                existing.dimensionsSetByUser(),
                existing.isMultimodal()
            );
        }
    }
}
