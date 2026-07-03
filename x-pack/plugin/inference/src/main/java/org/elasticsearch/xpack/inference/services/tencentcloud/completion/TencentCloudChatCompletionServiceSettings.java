/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudCommonServiceSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudRateLimitServiceSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class TencentCloudChatCompletionServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        TencentCloudRateLimitServiceSettings {

    public static final String NAME = "tencentcloud_chat_completion_service_settings";
    // Chat completion default rate limit is 5 rpm per the AI Gateway docs.
    public static final RateLimitSettings DEFAULT_CHAT_COMPLETION_RATE_LIMIT = new RateLimitSettings(5);

    public static TencentCloudChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        // Chat completion has a lower default rate limit than embeddings/rerank; if user does not specify one we override the default.
        if (map != null && map.containsKey(RateLimitSettings.FIELD_NAME) == false) {
            // Override common default before parsing.
        }
        var commonSettings = TencentCloudCommonServiceSettings.fromMap(map, context, validationException);
        // If the user did not provide a rate_limit override, replace the common default (20 rpm) with 5 rpm.
        if (commonSettings != null && commonSettings.rateLimitSettings() == TencentCloudCommonServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS) {
            commonSettings = new TencentCloudCommonServiceSettings(
                commonSettings.modelId(),
                commonSettings.uri(),
                DEFAULT_CHAT_COMPLETION_RATE_LIMIT
            );
        }

        validationException.throwIfValidationErrorsExist();

        return new TencentCloudChatCompletionServiceSettings(commonSettings);
    }

    private final TencentCloudCommonServiceSettings commonSettings;

    public TencentCloudChatCompletionServiceSettings(TencentCloudCommonServiceSettings commonSettings) {
        this.commonSettings = Objects.requireNonNull(commonSettings);
    }

    public TencentCloudChatCompletionServiceSettings(StreamInput in) throws IOException {
        this.commonSettings = new TencentCloudCommonServiceSettings(in);
    }

    public TencentCloudCommonServiceSettings getCommonSettings() {
        return commonSettings;
    }

    @Override
    public String modelId() {
        return commonSettings.modelId();
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return commonSettings.rateLimitSettings();
    }

    @Override
    public TencentCloudChatCompletionServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();
        var updatedCommonServiceSettings = commonSettings.updateCommonServiceSettings(serviceSettings, validationException);
        validationException.throwIfValidationErrorsExist();
        return new TencentCloudChatCompletionServiceSettings(updatedCommonServiceSettings);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        commonSettings.toXContentFragment(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        return commonSettings.toXContentFragmentOfExposedFields(builder, params);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        commonSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TencentCloudChatCompletionServiceSettings that = (TencentCloudChatCompletionServiceSettings) o;
        return Objects.equals(commonSettings, that.commonSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonSettings);
    }
}
