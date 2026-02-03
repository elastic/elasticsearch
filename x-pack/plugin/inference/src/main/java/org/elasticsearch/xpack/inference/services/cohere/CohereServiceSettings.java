/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
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

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;

/**
 * Settings for the Cohere service.
 * This class encapsulates the configuration settings required to use Cohere models.
 */
public class CohereServiceSettings extends FilteredXContentObject implements ServiceSettings, CohereRateLimitServiceSettings {

    public static final String NAME = "cohere_service_settings";
    public static final String OLD_MODEL_ID_FIELD = "model";
    public static final String API_VERSION = "api_version";
    public static final String MODEL_REQUIRED_FOR_V2_API = "The [service_settings.model_id] field is required for the Cohere V2 API.";

    private static final TransportVersion ML_INFERENCE_COHERE_API_VERSION = TransportVersion.fromName("ml_inference_cohere_api_version");

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

    private static final Logger logger = LogManager.getLogger(CohereServiceSettings.class);
    // Production key rate limits for all endpoints: https://docs.cohere.com/docs/going-live#production-key-specifications
    // 10K requests a minute
    public static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(10_000);

    /**
     * Creates {@link CohereServiceSettings} from a map
     * @param map the map to parse
     * @param context the context in which the parsing is done
     * @return the created {@link CohereServiceSettings}
     * @throws ValidationException If there are validation errors in the provided settings.
     */
    public static CohereServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        var uri = extractOptionalUri(map, URL, validationException);
        var similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var dimensions = extractOptionalPositiveInteger(map, DIMENSIONS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var maxInputTokens = extractOptionalPositiveInteger(
            map,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var modelId = extractModelId(map, validationException, context);
        var apiVersion = apiVersionFromMap(map, context, validationException);
        if (apiVersion == CohereApiVersion.V2 && modelId == null) {
            validationException.addValidationError(MODEL_REQUIRED_FOR_V2_API);
        }

        var rateLimitSettings = RateLimitSettings.of(map, DEFAULT_RATE_LIMIT_SETTINGS, validationException, CohereService.NAME, context);

        validationException.throwIfValidationErrorsExist();

        return new CohereServiceSettings(uri, similarity, dimensions, maxInputTokens, modelId, rateLimitSettings, apiVersion);
    }

    /**
     * Extracts the Cohere API version from the provided map based on the given context.
     *
     * @param map the map containing the settings
     * @param context the context for parsing configuration settings
     * @param validationException the validation exception to collect errors
     * @return the extracted Cohere API version
     */
    public static CohereApiVersion apiVersionFromMap(
        Map<String, Object> map,
        ConfigurationParseContext context,
        ValidationException validationException
    ) {
        return switch (context) {
            case REQUEST -> CohereApiVersion.V2; // new endpoints all use the V2 API.
            case PERSISTENT -> {
                var apiVersion = ServiceUtils.extractOptionalEnum(
                    map,
                    API_VERSION,
                    ModelConfigurations.SERVICE_SETTINGS,
                    CohereApiVersion::fromString,
                    EnumSet.allOf(CohereApiVersion.class),
                    validationException
                );

                if (apiVersion == null) {
                    yield CohereApiVersion.V1; // If the API version is not persisted then it must be V1
                } else {
                    yield apiVersion;
                }
            }
        };
    }

    private static String selectModelId(@Nullable String oldModelId, @Nullable String newModelId) {
        return newModelId != null ? newModelId : oldModelId;
    }

    private final URI uri;
    private final SimilarityMeasure similarity;
    private final Integer dimensions;
    private final Integer maxInputTokens;
    private final String modelId;
    private final RateLimitSettings rateLimitSettings;
    private final CohereApiVersion apiVersion;

    /**
     * Constructs a new {@link CohereServiceSettings} instance.
     *
     * @param uri the URI of the Cohere service
     * @param similarity the similarity measure to use
     * @param dimensions the number of dimensions for embeddings
     * @param maxInputTokens the maximum number of input tokens
     * @param modelId the model identifier
     * @param rateLimitSettings the rate limit settings
     * @param apiVersion the Cohere API version
     */
    public CohereServiceSettings(
        @Nullable URI uri,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable String modelId,
        @Nullable RateLimitSettings rateLimitSettings,
        CohereApiVersion apiVersion
    ) {
        this.uri = uri;
        this.similarity = similarity;
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
        this.modelId = modelId;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
        this.apiVersion = apiVersion;
    }

    public CohereServiceSettings(
        @Nullable String url,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable String modelId,
        @Nullable RateLimitSettings rateLimitSettings,
        CohereApiVersion apiVersion
    ) {
        this(createOptionalUri(url), similarity, dimensions, maxInputTokens, modelId, rateLimitSettings, apiVersion);
    }

    /**
     * Constructs a new {@link CohereServiceSettings} instance from a {@link StreamInput}.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs
     */
    public CohereServiceSettings(StreamInput in) throws IOException {
        uri = createOptionalUri(in.readOptionalString());
        similarity = in.readOptionalEnum(SimilarityMeasure.class);
        dimensions = in.readOptionalVInt();
        maxInputTokens = in.readOptionalVInt();
        modelId = in.readOptionalString();
        rateLimitSettings = new RateLimitSettings(in);
        if (in.getTransportVersion().supports(ML_INFERENCE_COHERE_API_VERSION)) {
            this.apiVersion = in.readEnum(CohereApiVersion.class);
        } else {
            this.apiVersion = CohereApiVersion.V1;
        }
    }

    // should only be used for testing, public because it's accessed outside the package
    public CohereServiceSettings(CohereApiVersion apiVersion) {
        this((URI) null, null, null, null, null, null, apiVersion);
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public CohereApiVersion apiVersion() {
        return apiVersion;
    }

    public URI uri() {
        return uri;
    }

    @Override
    public SimilarityMeasure similarity() {
        return similarity;
    }

    @Override
    public Integer dimensions() {
        return dimensions;
    }

    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public String modelId() {
        return modelId;
    }

    @Override
    public CohereServiceSettings updateServiceSettings(Map<String, Object> serviceSettings, TaskType taskType) {
        var validationException = new ValidationException();

        var extractedUri = extractOptionalUri(serviceSettings, URL, validationException);
        var extractedSimilarity = extractSimilarity(serviceSettings, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var extractedDimensions = extractOptionalPositiveInteger(
            serviceSettings,
            DIMENSIONS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var extractedMaxInputTokens = extractOptionalPositiveInteger(
            serviceSettings,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var extractedModelId = extractModelId(serviceSettings, validationException, ConfigurationParseContext.REQUEST);
        var extractedApiVersion = ServiceUtils.extractOptionalEnum(
            serviceSettings,
            API_VERSION,
            ModelConfigurations.SERVICE_SETTINGS,
            CohereServiceSettings.CohereApiVersion::fromString,
            EnumSet.allOf(CohereServiceSettings.CohereApiVersion.class),
            validationException
        );
        if (extractedApiVersion == CohereServiceSettings.CohereApiVersion.V2 && extractedModelId == null && this.modelId == null) {
            validationException.addValidationError(MODEL_REQUIRED_FOR_V2_API);
        }

        var extractedRateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            this.rateLimitSettings,
            validationException,
            CohereService.NAME,
            ConfigurationParseContext.PERSISTENT
        );

        validationException.throwIfValidationErrorsExist();

        return new CohereServiceSettings(
            extractedUri != null ? extractedUri : this.uri,
            extractedSimilarity != null ? extractedSimilarity : this.similarity,
            extractedDimensions != null ? extractedDimensions : this.dimensions,
            extractedMaxInputTokens != null ? extractedMaxInputTokens : this.maxInputTokens,
            extractedModelId != null ? extractedModelId : this.modelId,
            extractedRateLimitSettings,
            extractedApiVersion != null ? extractedApiVersion : this.apiVersion
        );
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
        return selectModelId(extractedOldModelId, extractedModelId);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragment(builder, params);
        builder.endObject();
        return builder;
    }

    public XContentBuilder toXContentFragment(XContentBuilder builder, Params params) throws IOException {
        toXContentFragmentOfExposedFields(builder, params);
        return builder.field(API_VERSION, apiVersion); // API version is persisted but not exposed to the user
    }

    @Override
    public XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        if (uri != null) {
            builder.field(URL, uri.toString());
        }
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (modelId != null) {
            builder.field(ServiceFields.MODEL_ID, modelId);
        }
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        var uriToWrite = uri != null ? uri.toString() : null;
        out.writeOptionalString(uriToWrite);
        out.writeOptionalEnum(SimilarityMeasure.translateSimilarity(similarity, out.getTransportVersion()));
        out.writeOptionalVInt(dimensions);
        out.writeOptionalVInt(maxInputTokens);
        out.writeOptionalString(modelId);

        rateLimitSettings.writeTo(out);
        if (out.getTransportVersion().supports(ML_INFERENCE_COHERE_API_VERSION)) {
            out.writeEnum(apiVersion);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CohereServiceSettings that = (CohereServiceSettings) o;
        return Objects.equals(uri, that.uri)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings)
            && apiVersion == that.apiVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, similarity, dimensions, maxInputTokens, modelId, rateLimitSettings, apiVersion);
    }
}
