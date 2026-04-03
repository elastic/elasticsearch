/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.rerank;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.contextualai.ContextualAiServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class ContextualAiRerankServiceSettings extends ContextualAiServiceSettings {

    public static final String NAME = "contextualai_rerank_service_settings";

    private static final URI DEFAULT_RERANK_URI = ServiceUtils.createUri("https://api.contextual.ai/v1/rerank");

    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(1000);

    public static ContextualAiRerankServiceSettings fromMap(Map<String, Object> serviceSettingsMap, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        var commonSettings = ContextualAiServiceSettings.fromMap(
            serviceSettingsMap,
            context,
            DEFAULT_RERANK_URI,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException
        );

        validationException.throwIfValidationErrorsExist();

        return new ContextualAiRerankServiceSettings(commonSettings);
    }

    @Override
    public ServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();

        var commonSettings = updateCommonSettings(serviceSettings, validationException);

        validationException.throwIfValidationErrorsExist();

        return new ContextualAiRerankServiceSettings(commonSettings);
    }

    public ContextualAiRerankServiceSettings(CommonSettings commonSettings) {
        super(commonSettings);
    }

    public ContextualAiRerankServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
