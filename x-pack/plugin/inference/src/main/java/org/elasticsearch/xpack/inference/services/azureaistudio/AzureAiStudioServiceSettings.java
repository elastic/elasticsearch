/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredEnum;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.ENDPOINT_TYPE_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.PROVIDER_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TARGET_FIELD;

public abstract class AzureAiStudioServiceSettings extends FilteredXContentObject implements ServiceSettings {

    protected final String target;
    protected final AzureAiStudioProvider provider;
    protected final AzureAiStudioEndpointType endpointType;
    protected final RateLimitSettings rateLimitSettings;

    protected static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(240);

    protected static BaseAzureAiStudioCommonFields fromMap(
        Map<String, Object> map,
        ValidationException validationException,
        ConfigurationParseContext context
    ) {
        String target = extractRequiredString(map, TARGET_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);
        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            AzureAiStudioService.NAME,
            context
        );
        AzureAiStudioEndpointType endpointType = extractRequiredEnum(
            map,
            ENDPOINT_TYPE_FIELD,
            ModelConfigurations.SERVICE_SETTINGS,
            AzureAiStudioEndpointType::fromString,
            EnumSet.allOf(AzureAiStudioEndpointType.class),
            validationException
        );

        AzureAiStudioProvider provider = extractRequiredEnum(
            map,
            PROVIDER_FIELD,
            ModelConfigurations.SERVICE_SETTINGS,
            AzureAiStudioProvider::fromString,
            EnumSet.allOf(AzureAiStudioProvider.class),
            validationException
        );

        return new BaseAzureAiStudioCommonFields(target, provider, endpointType, rateLimitSettings);
    }

    protected AzureAiStudioServiceSettings(StreamInput in) throws IOException {
        this.target = in.readString();
        this.provider = in.readEnum(AzureAiStudioProvider.class);
        this.endpointType = in.readEnum(AzureAiStudioEndpointType.class);
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    protected AzureAiStudioServiceSettings(
        String target,
        AzureAiStudioProvider provider,
        AzureAiStudioEndpointType endpointType,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.target = target;
        this.provider = provider;
        this.endpointType = endpointType;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    protected record BaseAzureAiStudioCommonFields(
        String target,
        AzureAiStudioProvider provider,
        AzureAiStudioEndpointType endpointType,
        RateLimitSettings rateLimitSettings
    ) {}

    public String target() {
        return this.target;
    }

    public AzureAiStudioProvider provider() {
        return this.provider;
    }

    public AzureAiStudioEndpointType endpointType() {
        return this.endpointType;
    }

    public RateLimitSettings rateLimitSettings() {
        return this.rateLimitSettings;
    }

    @Override
    public String modelId() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(target);
        out.writeEnum(provider);
        out.writeEnum(endpointType);
        rateLimitSettings.writeTo(out);
    }

    protected void addXContentFields(XContentBuilder builder, Params params) throws IOException {
        this.addExposedXContentFields(builder, params);
    }

    protected void addExposedXContentFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(TARGET_FIELD, this.target);
        builder.field(PROVIDER_FIELD, this.provider);
        builder.field(ENDPOINT_TYPE_FIELD, this.endpointType);
        rateLimitSettings.toXContent(builder, params);
    }

}
