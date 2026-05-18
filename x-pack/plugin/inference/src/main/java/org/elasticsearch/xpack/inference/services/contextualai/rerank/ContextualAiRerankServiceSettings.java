/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.contextualai.ContextualAiServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiUtils.INFERENCE_CONTEXTUAL_AI_ADDED;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiUtils.INFERENCE_CONTEXTUAL_AI_URL_SERVICE_SETTING_REMOVED;

public class ContextualAiRerankServiceSettings extends ContextualAiServiceSettings {

    public static final String NAME = "contextualai_rerank_service_settings";

    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(1000);

    public static ContextualAiRerankServiceSettings fromMap(Map<String, Object> serviceSettingsMap, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        var commonSettings = ContextualAiServiceSettings.fromMap(
            serviceSettingsMap,
            context,
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

    /**
     * For versions that still support the URL service setting, we need to write out a value to avoid serialization errors.
     * This value will be set as URI to send ContextualAI rerank request to.
     * @param out the output stream to write to
     * @throws IOException if an I/O error occurs during writing
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(INFERENCE_CONTEXTUAL_AI_URL_SERVICE_SETTING_REMOVED) == false) {
            out.writeString(ContextualAiRerankModel.DEFAULT_RERANK_URI.toString());
        }
        super.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return INFERENCE_CONTEXTUAL_AI_ADDED;
    }
}
