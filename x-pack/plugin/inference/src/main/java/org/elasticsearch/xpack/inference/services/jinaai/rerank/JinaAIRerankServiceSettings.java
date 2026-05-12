/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAICommonServiceSettings;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class JinaAIRerankServiceSettings extends FilteredXContentObject implements ServiceSettings, JinaAIRateLimitServiceSettings {
    public static final String NAME = "jinaai_rerank_service_settings";

    public static JinaAIRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        var commonServiceSettings = JinaAICommonServiceSettings.fromMap(map, context, validationException);

        validationException.throwIfValidationErrorsExist();
        return new JinaAIRerankServiceSettings(commonServiceSettings);
    }

    private final JinaAICommonServiceSettings commonSettings;

    public JinaAIRerankServiceSettings(JinaAICommonServiceSettings commonSettings) {
        this.commonSettings = commonSettings;
    }

    public JinaAIRerankServiceSettings(StreamInput in) throws IOException {
        this.commonSettings = new JinaAICommonServiceSettings(in);
    }

    public JinaAICommonServiceSettings getCommonSettings() {
        return commonSettings;
    }

    @Override
    public JinaAIRerankServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();

        var updatedCommonServiceSettings = commonSettings.updateCommonServiceSettings(serviceSettings, validationException);

        validationException.throwIfValidationErrorsExist();

        return new JinaAIRerankServiceSettings(updatedCommonServiceSettings);
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
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder = commonSettings.toXContentFragment(builder, params);

        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        commonSettings.toXContentFragmentOfExposedFields(builder, params);
        return builder;
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
        JinaAIRerankServiceSettings that = (JinaAIRerankServiceSettings) o;
        return Objects.equals(commonSettings, that.commonSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonSettings);
    }
}
