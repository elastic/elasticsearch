/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.parser.StringParser.validateStringIsNotNullOrEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;

/**
 * Abstract base for all Llama task-specific service settings. Holds the fields shared across every Llama task
 * (model identity, endpoint URL, and rate limiting) together with the parsing, serialization, and update machinery
 * that would otherwise be duplicated. Task-specific subclasses contribute only their own additional fields.
 */
public abstract class LlamaServiceSettings extends FilteredXContentObject implements ServiceSettings {

    protected static final TransportVersion ML_INFERENCE_LLAMA_ADDED = TransportVersion.fromName("ml_inference_llama_added");
    // There is no default rate limit for Llama, so we set a reasonable default of 3000 requests per minute
    protected static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(3000);

    /**
     * Registers the common Llama service-settings fields (model_id, url, rate_limit) onto the given parser.
     */
    public static <B extends Builder<? extends LlamaServiceSettings>> void declareCommonFields(
        AbstractObjectParser<B, ConfigurationParseContext> parser
    ) {
        parser.declareString(Builder::setModelId, new ParseField(MODEL_ID));
        parser.declareString(Builder::setUrl, new ParseField(URL));
        parser.declareObject(
            Builder::setRateLimitSettings,
            // An explicitly empty rate_limit object ({}) resolves to the default rate limit rather than null, so the setter is never
            // invoked with null.
            (p, c) -> RateLimitSettings.createParser(c == ConfigurationParseContext.PERSISTENT, DEFAULT_RATE_LIMIT_SETTINGS).apply(p, null),
            new ParseField(RateLimitSettings.FIELD_NAME)
        );
        // api_key appears in the same JSON block as service settings in REST requests; DefaultSecretSettings extracts it separately.
        // Declare it here as a no-op so the strict REQUEST parser does not reject it as an unknown field.
        parser.declareString((b, v) -> {}, new ParseField(DefaultSecretSettings.API_KEY));
    }

    private final String modelId;
    private final URI uri;
    private final RateLimitSettings rateLimitSettings;

    protected LlamaServiceSettings(String modelId, URI uri, @Nullable RateLimitSettings rateLimitSettings) {
        this.modelId = modelId;
        this.uri = uri;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    @Override
    public String modelId() {
        return modelId;
    }

    public URI uri() {
        return uri;
    }

    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ML_INFERENCE_LLAMA_ADDED;
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
        builder.field(URL, uri.toString());
        rateLimitSettings.toXContent(builder, params);
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LlamaServiceSettings that = (LlamaServiceSettings) o;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(uri, that.uri)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, uri, rateLimitSettings);
    }

    /**
     * Accumulates the parsed common fields and assembles a {@link LlamaServiceSettings}, enforcing that the required
     * {@code model_id} and {@code url} fields are present. Task-specific builders extend this and contribute their own fields.
     *
     * @param <T> the task-specific settings type produced by {@link #build(String, URI, RateLimitSettings)}
     */
    public abstract static class Builder<T extends LlamaServiceSettings> {

        private String modelId;
        private String url;
        protected RateLimitSettings rateLimitSettings;

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public void setRateLimitSettings(RateLimitSettings rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }

        protected abstract T build(String modelId, URI uri, RateLimitSettings rateLimitSettings);

        public final T build() {
            validateStringIsNotNullOrEmpty(modelId, MODEL_ID);
            validateStringIsNotNullOrEmpty(url, URL);
            return build(modelId, createUri(url), rateLimitSettings);
        }
    }

    /**
     * Creates a {@link LlamaServiceSettings} from a map of settings using the given parser.
     *
     * @param map     the map to parse
     * @param context the context in which the parsing is done
     * @param parser  the parser to use for parsing the settings
     * @return the created {@link LlamaServiceSettings}
     */
    public static <T extends LlamaServiceSettings> T fromMap(
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

    /**
     * Registers the common Llama fields that may be changed by an update request. Only {@code rate_limit} is mutable; the
     * immutable fields (such as {@code model_id} and {@code url}) are intentionally not declared so that a strict update parser
     * rejects attempts to change them.
     */
    public static void declareCommonUpdatableFields(AbstractObjectParser<? extends CommonUpdate, Void> parser) {
        parser.declareObject(
            CommonUpdate::setRateLimitSettings,
            // A null default preserves "no change" semantics for updates: an empty or value-less rate_limit object leaves the existing
            // rate limit untouched in CommonUpdate#mergedRateLimitSettings.
            (p, c) -> RateLimitSettings.createParser(false, null).apply(p, null),
            new ParseField(RateLimitSettings.FIELD_NAME)
        );
    }

    /**
     * Common fields parsed from an update request. Because settings are immutable, each subclass builds the new instance itself,
     * calling {@link #mergedRateLimitSettings(LlamaServiceSettings)} to resolve the shared fields.
     */
    public static class CommonUpdate {

        protected RateLimitSettings rateLimitSettings;

        private void setRateLimitSettings(@Nullable RateLimitSettings rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }

        /**
         * Resolves the rate limit settings to use after applying this update: the value supplied by the update if present, otherwise
         * the value carried by the existing settings.
         */
        protected RateLimitSettings mergedRateLimitSettings(LlamaServiceSettings existing) {
            return Objects.requireNonNullElse(this.rateLimitSettings, existing.rateLimitSettings());
        }
    }
}
