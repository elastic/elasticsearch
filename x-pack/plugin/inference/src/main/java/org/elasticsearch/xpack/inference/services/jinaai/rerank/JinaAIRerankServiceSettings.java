/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.rerank;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;

/**
 * Service settings for the JinaAI rerank task. Rerank adds no settings of its own beyond the common {@link JinaAIServiceSettings}
 * fields (model identity and rate limiting).
 */
public class JinaAIRerankServiceSettings extends JinaAIServiceSettings {
    public static final String NAME = "jinaai_rerank_service_settings";

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
        JinaAIServiceSettings.declareCommonFields(parser);
        return parser;
    }

    public static JinaAIRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        return JinaAIServiceSettings.fromMap(map, context, parser);
    }

    public JinaAIRerankServiceSettings(String modelId, @Nullable RateLimitSettings rateLimitSettings) {
        super(modelId, rateLimitSettings);
    }

    public JinaAIRerankServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public JinaAIRerankServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            return Update.PARSER.apply(xParser, null).mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse JinaAI rerank service settings update", e);
        }
    }

    /**
     * Builds a {@link JinaAIRerankServiceSettings} from the common JinaAI fields. Rerank adds no settings of its own.
     */
    static class Builder extends JinaAIServiceSettings.Builder<JinaAIRerankServiceSettings> {

        Builder(ConfigurationParseContext context) {
            super(context);
        }

        @Override
        protected JinaAIRerankServiceSettings build(String modelId, RateLimitSettings rateLimitSettings) {
            return new JinaAIRerankServiceSettings(modelId, rateLimitSettings);
        }
    }

    /**
     * Parses an update request, which may only contain the mutable {@code rate_limit} field. Including any immutable field (such as
     * {@code model_id}) causes the strict parser to reject the request.
     */
    private static class Update extends JinaAIServiceSettings.CommonUpdate {

        private static final ObjectParser<Update, Void> PARSER = new ObjectParser<>(ModelConfigurations.SERVICE_SETTINGS, Update::new);

        static {
            JinaAIServiceSettings.declareCommonUpdatableFields(PARSER);
        }

        public JinaAIRerankServiceSettings mergeInto(JinaAIRerankServiceSettings existing) {
            return new JinaAIRerankServiceSettings(existing.modelId(), mergedRateLimitSettings(existing));
        }
    }
}
