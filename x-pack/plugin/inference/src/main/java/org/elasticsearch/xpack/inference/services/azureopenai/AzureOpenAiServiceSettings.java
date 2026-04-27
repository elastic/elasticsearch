/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.API_VERSION;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.DEPLOYMENT_ID;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.RESOURCE_NAME;

/**
 * Base class for Azure OpenAI service settings, containing fields common to all
 * Azure OpenAI service setting types (e.g. completion, embeddings).
 */
public abstract class AzureOpenAiServiceSettings extends FilteredXContentObject implements ServiceSettings {

    private static final String OAUTH2_SETTINGS_NOT_CONFIGURED_ERROR =
        "Cannot update OAuth2 fields as the service was not configured with OAuth2 settings. "
            + "Please create a new Inference Endpoint with the OAuth2 settings instead.";

    /**
     * Common settings parsed from a map, shared by all Azure OpenAI service setting types.
     */
    protected record CommonSettings(
        String resourceName,
        String deploymentId,
        String apiVersion,
        RateLimitSettings rateLimitSettings,
        @Nullable AzureOpenAiOAuth2Settings oAuth2Settings
    ) {}

    protected final String resourceName;
    protected final String deploymentId;
    protected final String apiVersion;
    protected final RateLimitSettings rateLimitSettings;
    protected final AzureOpenAiOAuth2Settings oAuth2Settings;

    protected AzureOpenAiServiceSettings(
        String resourceName,
        String deploymentId,
        String apiVersion,
        RateLimitSettings rateLimitSettings,
        @Nullable AzureOpenAiOAuth2Settings oAuth2Settings
    ) {
        this.resourceName = resourceName;
        this.deploymentId = deploymentId;
        this.apiVersion = apiVersion;
        this.rateLimitSettings = rateLimitSettings;
        this.oAuth2Settings = oAuth2Settings;
    }

    protected AzureOpenAiServiceSettings(CommonSettings commonSettings) {
        this(
            commonSettings.resourceName(),
            commonSettings.deploymentId(),
            commonSettings.apiVersion(),
            commonSettings.rateLimitSettings(),
            commonSettings.oAuth2Settings()
        );
    }

    /**
     * Parses the common Azure OpenAI service settings from a map. Subclasses may use this
     * when implementing their own {@code fromMap} and then parse additional fields.
     */
    protected static CommonSettings parseCommonSettings(
        Map<String, Object> map,
        ValidationException validationException,
        ConfigurationParseContext context,
        RateLimitSettings defaultRateLimitSettings
    ) {
        var resourceName = extractRequiredString(map, RESOURCE_NAME, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var deploymentId = extractRequiredString(map, DEPLOYMENT_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var apiVersion = extractRequiredString(map, API_VERSION, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var rateLimitSettings = RateLimitSettings.of(map, defaultRateLimitSettings, validationException, AzureOpenAiService.NAME, context);
        var oAuth2Settings = AzureOpenAiOAuth2Settings.fromMap(map, validationException);
        return new CommonSettings(resourceName, deploymentId, apiVersion, rateLimitSettings, oAuth2Settings);
    }

    public String resourceName() {
        return resourceName;
    }

    public String deploymentId() {
        return deploymentId;
    }

    public String apiVersion() {
        return apiVersion;
    }

    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    public AzureOpenAiOAuth2Settings oAuth2Settings() {
        return oAuth2Settings;
    }

    protected CommonSettings updateCommonSettings(Map<String, Object> serviceSettings, ValidationException validationException) {
        // If the endpoint was not initially configured with OAuth2 settings,
        // we do not allow OAuth2 fields in request map as that would lead to an invalid configuration and fail early.
        if (oAuth2Settings == null && AzureOpenAiOAuth2Settings.hasAnyOAuth2Fields(serviceSettings)) {
            throw validationException.addValidationError(OAUTH2_SETTINGS_NOT_CONFIGURED_ERROR);
        }

        var extractedOAuth2Settings = oAuth2Settings != null
            ? oAuth2Settings.updateServiceSettings(serviceSettings, validationException)
            : null;
        var extractedRateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            this.rateLimitSettings,
            validationException,
            AzureOpenAiService.NAME,
            ConfigurationParseContext.REQUEST
        );

        return new CommonSettings(
            this.resourceName,
            this.deploymentId,
            this.apiVersion,
            extractedRateLimitSettings,
            extractedOAuth2Settings
        );
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(RESOURCE_NAME, resourceName);
        builder.field(DEPLOYMENT_ID, deploymentId);
        builder.field(API_VERSION, apiVersion);
        rateLimitSettings.toXContent(builder, params);
        if (oAuth2Settings != null) {
            oAuth2Settings.toXContent(builder, params);
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        AzureOpenAiServiceSettings that = (AzureOpenAiServiceSettings) o;
        return Objects.equals(resourceName, that.resourceName)
            && Objects.equals(deploymentId, that.deploymentId)
            && Objects.equals(apiVersion, that.apiVersion)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings)
            && Objects.equals(oAuth2Settings, that.oAuth2Settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceName, deploymentId, apiVersion, rateLimitSettings, oAuth2Settings);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
