/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiOAuth2Settings;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiOAuth2Settings.AZURE_OPENAI_OAUTH_SETTINGS;

public class AzureOpenAiCompletionServiceSettings extends AzureOpenAiServiceSettings {

    public static final String NAME = "azure_openai_completions_service_settings";

    /**
     * Rate limit documentation can be found here:
     * <p>
     * Limits per region per model id
     * https://learn.microsoft.com/en-us/azure/ai-services/openai/quotas-limits
     * <p>
     * How to change the limits
     * https://learn.microsoft.com/en-us/azure/ai-services/openai/how-to/quota?tabs=rest
     * <p>
     * Blog giving some examples
     * https://techcommunity.microsoft.com/t5/fasttrack-for-azure/optimizing-azure-openai-a-guide-to-limits-quotas-and-best/ba-p/4076268
     * <p>
     * According to the docs 1000 tokens per minute (TPM) = 6 requests per minute (RPM). The limits change depending on the region
     * and model. The lowest chat completions limit is 20k TPM, so we'll default to that.
     * Calculation: 20K TPM = 20 * 6 = 120 requests per minute (used `francecentral` and `gpt-4` as basis for the calculation).
     */
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(120);

    public static AzureOpenAiCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        var commonSettings = parseCommonSettings(map, validationException, context, DEFAULT_RATE_LIMIT_SETTINGS);

        validationException.throwIfValidationErrorsExist();
        return new AzureOpenAiCompletionServiceSettings(commonSettings);
    }

    public AzureOpenAiCompletionServiceSettings(
        String resourceName,
        String deploymentId,
        String apiVersion,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this(resourceName, deploymentId, apiVersion, rateLimitSettings, null);
    }

    public AzureOpenAiCompletionServiceSettings(
        String resourceName,
        String deploymentId,
        String apiVersion,
        @Nullable RateLimitSettings rateLimitSettings,
        @Nullable AzureOpenAiOAuth2Settings oAuth2Settings
    ) {
        super(
            resourceName,
            deploymentId,
            apiVersion,
            Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS),
            oAuth2Settings
        );
    }

    public AzureOpenAiCompletionServiceSettings(StreamInput in) throws IOException {
        super(
            in.readString(),
            in.readString(),
            in.readString(),
            new RateLimitSettings(in),
            in.getTransportVersion().supports(AZURE_OPENAI_OAUTH_SETTINGS) ? in.readOptionalWriteable(AzureOpenAiOAuth2Settings::new) : null
        );
    }

    private AzureOpenAiCompletionServiceSettings(CommonSettings commonSettings) {
        super(commonSettings);
    }

    @Override
    public String modelId() {
        return null;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        toXContentFragmentOfExposedFields(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public AzureOpenAiCompletionServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();

        var updatedCommonSettings = updateCommonSettings(serviceSettings, validationException);

        validationException.throwIfValidationErrorsExist();
        return new AzureOpenAiCompletionServiceSettings(updatedCommonSettings);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(resourceName);
        out.writeString(deploymentId);
        out.writeString(apiVersion);
        rateLimitSettings.writeTo(out);
        if (out.getTransportVersion().supports(AZURE_OPENAI_OAUTH_SETTINGS)) {
            out.writeOptionalWriteable(oAuth2Settings);
        }
    }
}
