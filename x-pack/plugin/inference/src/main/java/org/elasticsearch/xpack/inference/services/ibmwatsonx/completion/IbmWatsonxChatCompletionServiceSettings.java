/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskType;
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
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractUri;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxServiceFields.API_VERSION;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxServiceFields.PROJECT_ID;

public class IbmWatsonxChatCompletionServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        IbmWatsonxRateLimitServiceSettings {
    public static final String NAME = "ibm_watsonx_completion_service_settings";
    private static final TransportVersion ML_INFERENCE_IBM_WATSONX_COMPLETION_ADDED = TransportVersion.fromName(
        "ml_inference_ibm_watsonx_completion_added"
    );

    /**
     * Rate limits are defined at
     * <a href="https://www.ibm.com/docs/en/watsonx/saas?topic=learning-watson-machine-plans">Watson Machine Learning plans</a>.
     * For the Lite plan, the limit is 120 requests per minute.
     */
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(120);

    /**
     * Creates an instance of {@link IbmWatsonxChatCompletionServiceSettings} from the given map.
     * Validates the presence and format of required fields, and applies default values for optional fields if they are not present.
     * @param map the map containing the settings
     * @param context the context of the configuration parsing, used to determine how to parse the settings
     * @return an instance of {@link IbmWatsonxChatCompletionServiceSettings}
     */
    public static IbmWatsonxChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        var uri = extractUri(map, URL, validationException);
        var apiVersion = extractRequiredString(map, API_VERSION, ModelConfigurations.SERVICE_SETTINGS, validationException);

        var modelId = extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var projectId = extractRequiredString(map, PROJECT_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);

        var rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            IbmWatsonxService.NAME,
            context
        );

        validationException.throwIfValidationErrorsExist();

        return new IbmWatsonxChatCompletionServiceSettings(uri, apiVersion, modelId, projectId, rateLimitSettings);
    }

    private final URI uri;

    private final String apiVersion;

    private final String modelId;

    private final String projectId;

    private final RateLimitSettings rateLimitSettings;

    public IbmWatsonxChatCompletionServiceSettings(
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

    public IbmWatsonxChatCompletionServiceSettings(StreamInput in) throws IOException {
        this.uri = createUri(in.readString());
        this.apiVersion = in.readString();
        this.modelId = in.readString();
        this.projectId = in.readString();
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    @Override
    public ServiceSettings updateServiceSettings(Map<String, Object> serviceSettings, TaskType taskType) {
        var validationException = new ValidationException();

        var extracteduri = extractUri(serviceSettings, URL, validationException);
        var extractedApiVersion = extractRequiredString(
            serviceSettings,
            API_VERSION,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        var extractedModelId = extractRequiredString(serviceSettings, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var extractedProjectId = extractRequiredString(
            serviceSettings,
            PROJECT_ID,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        var extractedRateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            this.rateLimitSettings,
            validationException,
            IbmWatsonxService.NAME,
            ConfigurationParseContext.REQUEST
        );

        validationException.throwIfValidationErrorsExist();

        return new IbmWatsonxChatCompletionServiceSettings(
            extracteduri != null ? extracteduri : this.uri,
            extractedApiVersion != null ? extractedApiVersion : this.apiVersion,
            extractedModelId != null ? extractedModelId : this.modelId,
            extractedProjectId != null ? extractedProjectId : this.projectId,
            extractedRateLimitSettings
        );
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
        return ML_INFERENCE_IBM_WATSONX_COMPLETION_ADDED;
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
        IbmWatsonxChatCompletionServiceSettings that = (IbmWatsonxChatCompletionServiceSettings) object;
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
