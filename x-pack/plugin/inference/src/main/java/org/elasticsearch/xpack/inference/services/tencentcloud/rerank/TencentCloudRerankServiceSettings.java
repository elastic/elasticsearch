/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.rerank;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.ObjectParser;
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
        var validationException = new ValidationException();
        var settings = TencentCloudCommonServiceSettings.fromMap(map, context, parser, validationException);
        validationException.throwIfValidationErrorsExist();
        return settings;
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
        var validationException = new ValidationException();
        var extractedRateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            this.rateLimitSettings(),
            validationException,
            ConfigurationParseContext.REQUEST
        );
        validationException.throwIfValidationErrorsExist();
        return new TencentCloudRerankServiceSettings(this.modelId(), this.region(), extractedRateLimitSettings);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    public static class Builder extends TencentCloudCommonServiceSettings.Builder<TencentCloudRerankServiceSettings> {
        @Override
        protected TencentCloudRerankServiceSettings build() {
            return new TencentCloudRerankServiceSettings(modelId, region, rateLimitSettings);
        }
    }
}
