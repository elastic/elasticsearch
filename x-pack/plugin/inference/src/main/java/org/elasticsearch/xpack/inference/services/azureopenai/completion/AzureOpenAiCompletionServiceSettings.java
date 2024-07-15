/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiService;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.API_VERSION;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.DEPLOYMENT_ID;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.RESOURCE_NAME;

public class AzureOpenAiCompletionServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        AzureOpenAiRateLimitServiceSettings {

    public static final String NAME = "azure_openai_completions_service_settings";

    /**
     * Rate limit documentation can be found here:
     *
     * Limits per region per model id
     * https://learn.microsoft.com/en-us/azure/ai-services/openai/quotas-limits
     *
     * How to change the limits
     * https://learn.microsoft.com/en-us/azure/ai-services/openai/how-to/quota?tabs=rest
     *
     * Blog giving some examples
     * https://techcommunity.microsoft.com/t5/fasttrack-for-azure/optimizing-azure-openai-a-guide-to-limits-quotas-and-best/ba-p/4076268
     *
     * According to the docs 1000 tokens per minute (TPM) = 6 requests per minute (RPM). The limits change depending on the region
     * and model. The lowest chat completions limit is 20k TPM, so we'll default to that.
     * Calculation: 20K TPM = 20 * 6 = 120 requests per minute (used `francecentral` and `gpt-4` as basis for the calculation).
     */
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(120);

    public static AzureOpenAiCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        var settings = fromMap(map, validationException, context);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AzureOpenAiCompletionServiceSettings(settings);
    }

    private static AzureOpenAiCompletionServiceSettings.CommonFields fromMap(
        Map<String, Object> map,
        ValidationException validationException,
        ConfigurationParseContext context
    ) {
        String resourceName = extractRequiredString(map, RESOURCE_NAME, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String deploymentId = extractRequiredString(map, DEPLOYMENT_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String apiVersion = extractRequiredString(map, API_VERSION, ModelConfigurations.SERVICE_SETTINGS, validationException);
        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            AzureOpenAiService.NAME,
            context
        );

        return new AzureOpenAiCompletionServiceSettings.CommonFields(resourceName, deploymentId, apiVersion, rateLimitSettings);
    }

    private record CommonFields(String resourceName, String deploymentId, String apiVersion, RateLimitSettings rateLimitSettings) {}

    private final String resourceName;
    private final String deploymentId;
    private final String apiVersion;

    private final RateLimitSettings rateLimitSettings;

    public AzureOpenAiCompletionServiceSettings(
        String resourceName,
        String deploymentId,
        String apiVersion,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.resourceName = resourceName;
        this.deploymentId = deploymentId;
        this.apiVersion = apiVersion;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public AzureOpenAiCompletionServiceSettings(StreamInput in) throws IOException {
        resourceName = in.readString();
        deploymentId = in.readString();
        apiVersion = in.readString();
        rateLimitSettings = new RateLimitSettings(in);
    }

    private AzureOpenAiCompletionServiceSettings(AzureOpenAiCompletionServiceSettings.CommonFields fields) {
        this(fields.resourceName, fields.deploymentId, fields.apiVersion, fields.rateLimitSettings);
    }

    public String resourceName() {
        return resourceName;
    }

    public String deploymentId() {
        return deploymentId;
    }

    @Override
    public String modelId() {
        return null;
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    public String apiVersion() {
        return apiVersion;
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
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(RESOURCE_NAME, resourceName);
        builder.field(DEPLOYMENT_ID, deploymentId);
        builder.field(API_VERSION, apiVersion);
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_AZURE_OPENAI_COMPLETIONS;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(resourceName);
        out.writeString(deploymentId);
        out.writeString(apiVersion);
        rateLimitSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        AzureOpenAiCompletionServiceSettings that = (AzureOpenAiCompletionServiceSettings) object;
        return Objects.equals(resourceName, that.resourceName)
            && Objects.equals(deploymentId, that.deploymentId)
            && Objects.equals(apiVersion, that.apiVersion)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceName, deploymentId, apiVersion, rateLimitSettings);
    }
}
