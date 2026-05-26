/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalUri;

/**
 * Common service settings shared across all Cohere inference tasks.
 * Holds model identity, rate limiting, and API version — fields that apply
 * regardless of whether the task is embeddings, completion, or reranking.
 * <p>
 * This class is not a named writeable: it is only serialized as an embedded
 * component inside the task-specific settings classes.
 */
public class CohereCommonServiceSettings extends FilteredXContentObject implements ToXContentFragment, Writeable {

    public static final TransportVersion ML_INFERENCE_COHERE_API_VERSION = TransportVersion.fromName("ml_inference_cohere_api_version");
    public static final TransportVersion ML_INFERENCE_COHERE_SERVICE_SETTINGS_REFACTOR = TransportVersion.fromName(
        "ml_inference_cohere_service_settings_refactor"
    );

    public static final String OLD_MODEL_ID_FIELD = "model";
    public static final String API_VERSION = "api_version";
    public static final String MODEL_REQUIRED_FOR_V2_API = "The [service_settings.model_id] field is required for the Cohere V2 API.";

    /**
     * The API versions supported by the Cohere service.
     */
    public enum CohereApiVersion {
        V1,
        V2;

        public static CohereApiVersion fromString(String name) {
            return valueOf(name.trim().toUpperCase(Locale.ROOT));
        }
    }

    private static final Logger logger = LogManager.getLogger(CohereCommonServiceSettings.class);
    // Production key rate limits for all endpoints: https://docs.cohere.com/docs/going-live#production-key-specifications
    // 10K requests a minute
    public static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(10_000);

    /**
     * Creates {@link CohereCommonServiceSettings} from a map, parsing only the fields
     * that are common across all Cohere tasks.
     */
    public static CohereCommonServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        var uri = extractOptionalUri(map, URL, validationException);
        var modelId = extractModelId(map, validationException, context);
        var apiVersion = apiVersionFromMap(map, context, validationException);
        if (apiVersion == CohereApiVersion.V2 && modelId == null) {
            validationException.addValidationError(MODEL_REQUIRED_FOR_V2_API);
        }
        var rateLimitSettings = RateLimitSettings.of(map, DEFAULT_RATE_LIMIT_SETTINGS, validationException, context);

        validationException.throwIfValidationErrorsExist();

        return new CohereCommonServiceSettings(uri, modelId, rateLimitSettings, apiVersion);
    }

    /**
     * Extracts the Cohere API version from the provided map based on the given context.
     */
    public static CohereApiVersion apiVersionFromMap(
        Map<String, Object> map,
        ConfigurationParseContext context,
        ValidationException validationException
    ) {
        return switch (context) {
            case REQUEST -> CohereApiVersion.V2;
            case PERSISTENT -> {
                var apiVersion = ServiceUtils.extractOptionalEnum(
                    map,
                    API_VERSION,
                    ModelConfigurations.SERVICE_SETTINGS,
                    CohereApiVersion::fromString,
                    EnumSet.allOf(CohereApiVersion.class),
                    validationException
                );
                yield apiVersion == null ? CohereApiVersion.V1 : apiVersion;
            }
        };
    }

    private static String extractModelId(
        Map<String, Object> serviceSettings,
        ValidationException validationException,
        ConfigurationParseContext context
    ) {
        var extractedOldModelId = extractOptionalString(
            serviceSettings,
            OLD_MODEL_ID_FIELD,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        if (context == ConfigurationParseContext.REQUEST && extractedOldModelId != null) {
            logger.info("The cohere [service_settings.model] field is deprecated. Please use [service_settings.model_id] instead.");
        }
        var extractedModelId = extractOptionalString(
            serviceSettings,
            ServiceFields.MODEL_ID,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        return extractedModelId != null ? extractedModelId : extractedOldModelId;
    }

    @Nullable
    private final URI uri;
    private final String modelId;
    private final RateLimitSettings rateLimitSettings;
    private final CohereApiVersion apiVersion;

    public CohereCommonServiceSettings(
        @Nullable String modelId,
        @Nullable RateLimitSettings rateLimitSettings,
        CohereApiVersion apiVersion
    ) {
        this(null, modelId, rateLimitSettings, apiVersion);
    }

    public CohereCommonServiceSettings(
        @Nullable URI uri,
        @Nullable String modelId,
        @Nullable RateLimitSettings rateLimitSettings,
        CohereApiVersion apiVersion
    ) {
        this.uri = uri;
        this.modelId = modelId;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
        this.apiVersion = apiVersion;
    }

    /**
     * Deserializes from a stream (new format only — this class is not a named writeable;
     * backward-compat deserialization of old formats is handled by each task-specific class).
     */
    public CohereCommonServiceSettings(StreamInput in) throws IOException {
        this.uri = createOptionalUri(in.readOptionalString());
        this.modelId = in.readOptionalString();
        this.rateLimitSettings = new RateLimitSettings(in);
        this.apiVersion = in.readEnum(CohereApiVersion.class);
    }

    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    public CohereApiVersion apiVersion() {
        return apiVersion;
    }

    public URI uri() {
        return uri;
    }

    public String modelId() {
        return modelId;
    }

    public CohereCommonServiceSettings update(Map<String, Object> serviceSettings, ValidationException validationException) {
        var extractedRateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            this.rateLimitSettings,
            validationException,
            ConfigurationParseContext.REQUEST
        );
        return new CohereCommonServiceSettings(this.uri, this.modelId, extractedRateLimitSettings, this.apiVersion);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        toXContentFragmentOfExposedFields(builder, params);
        if (uri != null) {
            builder.field(URL, uri.toString());
        }
        builder.field(API_VERSION, apiVersion);
        return builder;
    }

    @Override
    public XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (modelId != null) {
            builder.field(ServiceFields.MODEL_ID, modelId);
        }
        rateLimitSettings.toXContent(builder, params);
        return builder;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(uri != null ? uri.toString() : null);
        out.writeOptionalString(modelId);
        rateLimitSettings.writeTo(out);
        out.writeEnum(apiVersion);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CohereCommonServiceSettings that = (CohereCommonServiceSettings) o;
        return Objects.equals(uri, that.uri)
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings)
            && apiVersion == that.apiVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, modelId, rateLimitSettings, apiVersion);
    }
}
