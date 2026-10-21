/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.rerank;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudCommonServiceSettings;

import java.io.IOException;
import java.util.Map;

/**
 * Settings for the TencentCloud rerank service. Holds only the fields common to every TencentCloud task
 * (model id, region, rate limit).
 */
public class TencentCloudRerankServiceSettings extends TencentCloudCommonServiceSettings {

    public static final String NAME = "tencentcloud_rerank_service_settings";

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(false);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(true);

    static ObjectParser<Builder, ConfigurationParseContext> createParser(boolean ignoreUnknownFields) {
        ObjectParser<Builder, ConfigurationParseContext> parser = new ObjectParser<>(
            ModelConfigurations.SERVICE_SETTINGS,
            ignoreUnknownFields,
            Builder::new
        );
        TencentCloudCommonServiceSettings.declareCommonFields(parser, TencentCloudCommonServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS);
        return parser;
    }

    public static TencentCloudRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        return TencentCloudCommonServiceSettings.fromMap(map, context, parser);
    }

    public TencentCloudRerankServiceSettings(String modelId, String region, @Nullable RateLimitSettings rateLimitSettings) {
        super(modelId, region, rateLimitSettings);
    }

    public TencentCloudRerankServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TencentCloudRerankServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            return Update.PARSER.apply(xParser, null).mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse TencentCloud rerank service settings update", e);
        }
    }

    public static class Builder extends TencentCloudCommonServiceSettings.Builder<TencentCloudRerankServiceSettings> {
        @Override
        protected TencentCloudRerankServiceSettings build(String modelId, String region, RateLimitSettings rateLimitSettings) {
            return new TencentCloudRerankServiceSettings(modelId, region, rateLimitSettings);
        }
    }

    /**
     * Parses an update request, which may only contain the mutable {@code rate_limit} field. Including any immutable field (such as
     * {@code model_id} or {@code region}) causes the strict parser to reject the request.
     */
    private static class Update extends TencentCloudCommonServiceSettings.CommonUpdate {

        private static final ObjectParser<Update, Void> PARSER = new ObjectParser<>(ModelConfigurations.SERVICE_SETTINGS, Update::new);

        static {
            TencentCloudCommonServiceSettings.declareCommonUpdatableFields(PARSER);
        }

        public TencentCloudRerankServiceSettings mergeInto(TencentCloudRerankServiceSettings existing) {
            return new TencentCloudRerankServiceSettings(existing.modelId(), existing.region(), mergedRateLimitSettings(existing));
        }
    }
}
