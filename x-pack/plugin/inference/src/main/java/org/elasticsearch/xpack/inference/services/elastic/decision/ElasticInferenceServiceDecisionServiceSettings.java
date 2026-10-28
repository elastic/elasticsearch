/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.decision;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
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
import static org.elasticsearch.xpack.inference.services.SettingsScope.SERVICE_SETTINGS;

/**
 * Service settings for the Elastic Inference Service {@code decision} task type. Rate limiting is handled by EIS itself, so only the
 * model is configurable.
 */
public class ElasticInferenceServiceDecisionServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        ElasticInferenceServiceRateLimitServiceSettings {

    public static final String NAME = "elastic_decision_service_settings";

    static final TransportVersion INFERENCE_API_EIS_DECISION = TransportVersion.fromName("inference_api_eis_decision");

    public static ElasticInferenceServiceDecisionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        String modelId = extractRequiredString(map, MODEL_ID, SERVICE_SETTINGS, validationException);

        RateLimitSettings.rejectRateLimitFieldForRequestContext(
            map,
            SERVICE_SETTINGS,
            ElasticInferenceService.NAME,
            TaskType.DECISION,
            context,
            validationException
        );

        validationException.throwIfValidationErrorsExist();

        return new ElasticInferenceServiceDecisionServiceSettings(modelId);
    }

    private final String modelId;

    public ElasticInferenceServiceDecisionServiceSettings(String modelId) {
        this.modelId = Objects.requireNonNull(modelId);
    }

    public ElasticInferenceServiceDecisionServiceSettings(StreamInput in) throws IOException {
        this(in.readString());
    }

    @Override
    public String modelId() {
        return modelId;
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return RateLimitSettings.DISABLED_INSTANCE;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return INFERENCE_API_EIS_DECISION;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(MODEL_ID, modelId);
        return builder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragmentOfExposedFields(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticInferenceServiceDecisionServiceSettings that = (ElasticInferenceServiceDecisionServiceSettings) o;
        return Objects.equals(modelId, that.modelId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(modelId);
    }
}
