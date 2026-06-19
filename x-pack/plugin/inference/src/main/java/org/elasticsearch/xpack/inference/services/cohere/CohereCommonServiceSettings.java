/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;

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
     * Registers the common Cohere service-settings fields (model_id, url, api_version, rate_limit)
     * onto the given parser. The deprecated {@code model} alias is also registered and emits a
     * log warning when encountered in request context.
     */
    public static <B extends Builder<? extends CohereServiceSettings>> void declareCommonFields(
        AbstractObjectParser<B, ConfigurationParseContext> parser,
        ConfigurationParseContext context
    ) {
        parser.declareString(Builder::setDeprecatedModelId, new ParseField(OLD_MODEL_ID_FIELD));
        parser.declareString(Builder::setModelId, new ParseField(ServiceFields.MODEL_ID));
        parser.declareString(Builder::setUrl, new ParseField(URL));
        if (context == ConfigurationParseContext.PERSISTENT) {
            parser.declareString(Builder::setApiVersion, new ParseField(API_VERSION));
        }
        parser.declareObject(
            Builder::setRateLimitSettings,
            (p, c) -> RateLimitSettings.createParser(c == ConfigurationParseContext.PERSISTENT, DEFAULT_RATE_LIMIT_SETTINGS).apply(p, null),
            new ParseField(RateLimitSettings.FIELD_NAME)
        );
        // api_key appears in the same JSON block as service settings in REST requests; DefaultSecretSettings extracts it separately.
        // Declare it here as a no-op so the strict REQUEST parser does not reject it as an unknown field.
        parser.declareString((b, v) -> {}, new ParseField(DefaultSecretSettings.API_KEY));
    }

    /**
     * Resolves the API version from a raw string captured during parsing.
     * REQUEST context always returns V2; PERSISTENT reads the stored value and defaults to V1.
     */
    public static CohereApiVersion resolveApiVersion(String rawApiVersion, boolean isRequest) {
        if (isRequest) {
            return CohereApiVersion.V2;
        }
        return rawApiVersion != null ? CohereApiVersion.fromString(rawApiVersion) : CohereApiVersion.V1;
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

    public CohereCommonServiceSettings update(CommonUpdate update) {
        RateLimitSettings updatedRateLimitSettings = Objects.requireNonNullElse(update.rateLimitSettings, this.rateLimitSettings);
        return new CohereCommonServiceSettings(this.uri, this.modelId, updatedRateLimitSettings, this.apiVersion);
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

    @Override
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

    public abstract static class Builder<T> {
        protected final ConfigurationParseContext context;

        private String url;
        private String modelId;
        private String deprecatedModelId;
        private String apiVersionRaw;
        protected RateLimitSettings rateLimitSettings;

        protected Builder(ConfigurationParseContext context) {
            this.context = Objects.requireNonNull(context);
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        public void setDeprecatedModelId(String deprecatedModelId) {
            this.deprecatedModelId = deprecatedModelId;
            if (context == ConfigurationParseContext.REQUEST) {
                logger.info("The cohere [service_settings.model] field is deprecated. Please use [service_settings.model_id] instead.");
            }
        }

        public void setApiVersion(String apiVersion) {
            this.apiVersionRaw = apiVersion;
        }

        public void setRateLimitSettings(RateLimitSettings rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }

        protected abstract T build(CohereCommonServiceSettings commonSettings);

        public final T build() {
            return build(buildCommonSettings());
        }

        private CohereCommonServiceSettings buildCommonSettings() {
            boolean isRequest = context == ConfigurationParseContext.REQUEST;
            var apiVersion = CohereCommonServiceSettings.resolveApiVersion(apiVersionRaw, isRequest);
            String resolvedModelId = modelId != null ? modelId : deprecatedModelId;
            if (apiVersion == CohereCommonServiceSettings.CohereApiVersion.V2 && resolvedModelId == null) {
                throw new IllegalArgumentException(CohereCommonServiceSettings.MODEL_REQUIRED_FOR_V2_API);
            }
            var uri = createOptionalUri(url);
            return new CohereCommonServiceSettings(uri, resolvedModelId, rateLimitSettings, apiVersion);
        }
    }

    /**
     * Creates a {@link CohereServiceSettings} from a map of settings using the given parser.
     *
     * @param map     the map to parse
     * @param context the context in which the parsing is done
     * @param parser  the parser to use for parsing the settings.
     * @return the created {@link CohereServiceSettings}
     */
    public static <T extends CohereServiceSettings> T fromMap(
        Map<String, Object> map,
        ConfigurationParseContext context,
        ObjectParser<? extends Builder<T>, ConfigurationParseContext> parser
    ) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            return parser.apply(xParser, context).build();
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, ModelConfigurations.SERVICE_SETTINGS);
        }
    }

    public static void declareCommonUpdatableFields(AbstractObjectParser<? extends CommonUpdate, Void> parser) {
        parser.declareObject(
            CommonUpdate::setRateLimitSettings,
            (p, c) -> RateLimitSettings.createParser(false, null).apply(p, null),
            new ParseField(RateLimitSettings.FIELD_NAME)
        );
    }

    public static class CommonUpdate {

        protected RateLimitSettings rateLimitSettings;

        private void setRateLimitSettings(RateLimitSettings rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }
    }
}
