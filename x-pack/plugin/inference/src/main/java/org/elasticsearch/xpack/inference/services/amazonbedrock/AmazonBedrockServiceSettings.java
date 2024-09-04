/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock;

import org.elasticsearch.TransportVersion;
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

import static org.elasticsearch.TransportVersions.ML_INFERENCE_AMAZON_BEDROCK_ADDED;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredEnum;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.MODEL_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.PROVIDER_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.REGION_FIELD;

public abstract class AmazonBedrockServiceSettings extends FilteredXContentObject implements ServiceSettings {

    protected static final String AMAZON_BEDROCK_BASE_NAME = "amazon_bedrock";

    protected final String region;
    protected final String model;
    protected final AmazonBedrockProvider provider;
    protected final RateLimitSettings rateLimitSettings;

    // the default requests per minute are defined as per-model in the "Runtime quotas" on AWS
    // see: https://docs.aws.amazon.com/bedrock/latest/userguide/quotas.html
    // setting this to 240 requests per minute (4 requests / sec) is a sane default for us as it should be enough for
    // decent throughput without exceeding the minimal for _most_ items. The user should consult
    // the table above if using a model that might have a lesser limit (e.g. Anthropic Claude 3.5)
    protected static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(240);

    protected static AmazonBedrockServiceSettings.BaseAmazonBedrockCommonSettings fromMap(
        Map<String, Object> map,
        ValidationException validationException,
        ConfigurationParseContext context
    ) {
        String model = extractRequiredString(map, MODEL_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String region = extractRequiredString(map, REGION_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);
        AmazonBedrockProvider provider = extractRequiredEnum(
            map,
            PROVIDER_FIELD,
            ModelConfigurations.SERVICE_SETTINGS,
            AmazonBedrockProvider::fromString,
            EnumSet.allOf(AmazonBedrockProvider.class),
            validationException
        );
        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            AMAZON_BEDROCK_BASE_NAME,
            context
        );

        return new BaseAmazonBedrockCommonSettings(region, model, provider, rateLimitSettings);
    }

    protected record BaseAmazonBedrockCommonSettings(
        String region,
        String model,
        AmazonBedrockProvider provider,
        @Nullable RateLimitSettings rateLimitSettings
    ) {}

    protected AmazonBedrockServiceSettings(StreamInput in) throws IOException {
        this.region = in.readString();
        this.model = in.readString();
        this.provider = in.readEnum(AmazonBedrockProvider.class);
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    protected AmazonBedrockServiceSettings(
        String region,
        String model,
        AmazonBedrockProvider provider,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.region = Objects.requireNonNull(region);
        this.model = Objects.requireNonNull(model);
        this.provider = Objects.requireNonNull(provider);
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ML_INFERENCE_AMAZON_BEDROCK_ADDED;
    }

    public String region() {
        return region;
    }

    @Override
    public String modelId() {
        return model;
    }

    public AmazonBedrockProvider provider() {
        return provider;
    }

    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(region);
        out.writeString(model);
        out.writeEnum(provider);
        rateLimitSettings.writeTo(out);
    }

    public void addBaseXContent(XContentBuilder builder, Params params) throws IOException {
        toXContentFragmentOfExposedFields(builder, params);
    }

    protected void addXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(REGION_FIELD, region);
        builder.field(MODEL_FIELD, model);
        builder.field(PROVIDER_FIELD, provider.name());
        rateLimitSettings.toXContent(builder, params);
    }
}
