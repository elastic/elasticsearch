/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.completion;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;

/**
 * Service settings of the OCI Generative AI {@code completion} and {@code chat_completion} tasks. Chat completion adds no settings
 * of its own to the common OCI Generative AI settings.
 */
public class OciGenAiChatCompletionServiceSettings extends OciGenAiServiceSettings {

    public static final String NAME = "oci_genai_chat_completion_service_settings";

    public static OciGenAiChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();
        var common = extractCommonSettings(map, validationException, context);
        validationException.throwIfValidationErrorsExist();
        return new OciGenAiChatCompletionServiceSettings(common);
    }

    public OciGenAiChatCompletionServiceSettings(CommonSettings common) {
        super(common);
    }

    public OciGenAiChatCompletionServiceSettings(StreamInput in) throws IOException {
        this(new CommonSettings(in));
    }

    @Override
    public OciGenAiChatCompletionServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();
        var extractedRateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            rateLimitSettings(),
            validationException,
            ConfigurationParseContext.REQUEST
        );
        validationException.throwIfValidationErrorsExist();
        return new OciGenAiChatCompletionServiceSettings(common().withRateLimitSettings(extractedRateLimitSettings));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        common().writeTo(out);
    }
}
