/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxService;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.convertToUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxServiceFields.API_VERSION;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxServiceFields.PROJECT_ID;

public class IbmWatsonxRerankServiceSettings extends FilteredXContentObject implements ServiceSettings, IbmWatsonxRateLimitServiceSettings {
    public static final String NAME = "ibm_watsonx_rerank_service_settings";

    /**
     * Rate limits are defined at
     * <a href="https://www.ibm.com/docs/en/watsonx/saas?topic=learning-watson-machine-plans">Watson Machine Learning plans</a>.
     * For Lite plan, you've 120 requests per minute.
     */
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(120);

    public static IbmWatsonxRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        String url = extractRequiredString(map, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        URI uri = convertToUri(url, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String apiVersion = extractRequiredString(map, API_VERSION, ModelConfigurations.SERVICE_SETTINGS, validationException);

        String modelId = extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String projectId = extractRequiredString(map, PROJECT_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);

        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            IbmWatsonxService.NAME,
            context
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new IbmWatsonxRerankServiceSettings(uri, apiVersion, modelId, projectId, rateLimitSettings);
    }

    private final URI uri;

    private final String apiVersion;

    private final String modelId;

    private final String projectId;

    private final RateLimitSettings rateLimitSettings;

    public IbmWatsonxRerankServiceSettings(
        URI uri,
        String apiVersion,
        String modelId,
        String projectId,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.uri = uri;
        this.apiVersion = apiVersion;
        this.projectId = projectId;
        this.modelId = modelId;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public IbmWatsonxRerankServiceSettings(StreamInput in) throws IOException {
        this.uri = createUri(in.readString());
        this.apiVersion = in.readString();
        this.modelId = in.readString();
        this.projectId = in.readString();
        this.rateLimitSettings = new RateLimitSettings(in);

    }

    public URI uri() {
        return uri;
    }

    public String apiVersion() {
        return apiVersion;
    }

    @Override
    public String modelId() {
        return modelId;
    }

    public String projectId() {
        return projectId;
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public String getWriteableName() {
        return NAME;
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
        builder.field(URL, uri.toString());

        builder.field(API_VERSION, apiVersion);

        builder.field(MODEL_ID, modelId);

        builder.field(PROJECT_ID, projectId);

        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_IBM_WATSONX_RERANK_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(uri.toString());
        out.writeString(apiVersion);

        out.writeString(modelId);
        out.writeString(projectId);

        rateLimitSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        IbmWatsonxRerankServiceSettings that = (IbmWatsonxRerankServiceSettings) object;
        return Objects.equals(uri, that.uri)
            && Objects.equals(apiVersion, that.apiVersion)
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(projectId, that.projectId)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, apiVersion, modelId, projectId, rateLimitSettings);
    }
}
