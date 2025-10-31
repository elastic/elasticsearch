/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;

public class ElasticInferenceServiceCompletionServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        ElasticInferenceServiceRateLimitServiceSettings {

    public static final String NAME = "elastic_inference_service_completion_service_settings";

    private static final TransportVersion INFERENCE_API_DISABLE_EIS_RATE_LIMITING = TransportVersion.fromName(
        "inference_api_disable_eis_rate_limiting"
    );

    public static ElasticInferenceServiceCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        String modelId = extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);

        RateLimitSettings.rejectRateLimitFieldForRequestContext(
            map,
            ModelConfigurations.SERVICE_SETTINGS,
            ElasticInferenceService.NAME,
            TaskType.CHAT_COMPLETION,
            context,
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new ElasticInferenceServiceCompletionServiceSettings(modelId);
    }

    private final String modelId;
    private final RateLimitSettings rateLimitSettings;

    public ElasticInferenceServiceCompletionServiceSettings(String modelId) {
        this.modelId = Objects.requireNonNull(modelId);
        this.rateLimitSettings = RateLimitSettings.DISABLED_INSTANCE;
    }

    public ElasticInferenceServiceCompletionServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.rateLimitSettings = RateLimitSettings.DISABLED_INSTANCE;
        if (in.getTransportVersion().supports(INFERENCE_API_DISABLE_EIS_RATE_LIMITING) == false) {
            new RateLimitSettings(in);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public String modelId() {
        return modelId;
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_18_0;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        toXContentFragmentOfExposedFields(builder, params);

        builder.endObject();

        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(MODEL_ID, modelId);
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        if (out.getTransportVersion().supports(INFERENCE_API_DISABLE_EIS_RATE_LIMITING) == false) {
            rateLimitSettings.writeTo(out);
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        ElasticInferenceServiceCompletionServiceSettings that = (ElasticInferenceServiceCompletionServiceSettings) object;
        return Objects.equals(modelId, that.modelId) && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, rateLimitSettings);
    }
}
