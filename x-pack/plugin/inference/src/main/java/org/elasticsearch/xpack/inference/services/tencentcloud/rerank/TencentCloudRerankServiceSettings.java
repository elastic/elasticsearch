/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.rerank;

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

public class TencentCloudRerankServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        TencentCloudRateLimitServiceSettings {

    public static final String NAME = "tencentcloud_rerank_service_settings";

    public static TencentCloudRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();
        var commonSettings = TencentCloudCommonServiceSettings.fromMap(map, context, validationException);
        validationException.throwIfValidationErrorsExist();
        return new TencentCloudRerankServiceSettings(commonSettings);
    }

    private final TencentCloudCommonServiceSettings commonSettings;

    public TencentCloudRerankServiceSettings(TencentCloudCommonServiceSettings commonSettings) {
        this.commonSettings = Objects.requireNonNull(commonSettings);
    }

    public TencentCloudRerankServiceSettings(StreamInput in) throws IOException {
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
    public TencentCloudRerankServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();
        var updatedCommonServiceSettings = commonSettings.updateCommonServiceSettings(serviceSettings, validationException);
        validationException.throwIfValidationErrorsExist();
        return new TencentCloudRerankServiceSettings(updatedCommonServiceSettings);
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
        TencentCloudRerankServiceSettings that = (TencentCloudRerankServiceSettings) o;
        return Objects.equals(commonSettings, that.commonSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonSettings);
    }
}
