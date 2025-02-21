/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.rerank;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class VoyageAIRerankServiceSettings extends FilteredXContentObject implements ServiceSettings, VoyageAIRateLimitServiceSettings {
    public static final String NAME = "voyageai_rerank_service_settings";

    private static final Logger logger = LogManager.getLogger(VoyageAIRerankServiceSettings.class);

    public static VoyageAIRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        var commonServiceSettings = VoyageAIServiceSettings.fromMap(map, context);

        return new VoyageAIRerankServiceSettings(commonServiceSettings);
    }

    private final VoyageAIServiceSettings commonSettings;

    public VoyageAIRerankServiceSettings(VoyageAIServiceSettings commonSettings) {
        this.commonSettings = commonSettings;
    }

    public VoyageAIRerankServiceSettings(StreamInput in) throws IOException {
        this.commonSettings = new VoyageAIServiceSettings(in);
    }

    public VoyageAIServiceSettings getCommonSettings() {
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
        return TransportVersions.VOYAGE_AI_INTEGRATION_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        commonSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VoyageAIRerankServiceSettings that = (VoyageAIRerankServiceSettings) o;
        return Objects.equals(commonSettings, that.commonSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonSettings);
    }
}
