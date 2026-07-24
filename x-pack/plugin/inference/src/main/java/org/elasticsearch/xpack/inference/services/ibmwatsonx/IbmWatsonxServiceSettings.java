/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx;

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
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.parser.StatefulValue.applyUpdate;
import static org.elasticsearch.xpack.inference.common.parser.StringParser.validateStringIsNotNullOrEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxServiceFields.API_VERSION;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxServiceFields.PROJECT_ID;

/**
 * Abstract base for all IBM watsonx task-specific service settings. Holds the fields shared across every IBM watsonx task
 * (model identity, endpoint URL, API version, project, and rate limiting) together with the parsing, serialization, and update
 * machinery that would otherwise be duplicated. Task-specific subclasses contribute only their own additional fields.
 */
public abstract class IbmWatsonxServiceSettings extends FilteredXContentObject implements ServiceSettings {

    /**
     * Rate limits are defined at
     * <a href="https://www.ibm.com/docs/en/watsonx/saas?topic=learning-watson-machine-plans">Watson Machine Learning plans</a>.
     * For the Lite plan, the limit is 120 requests per minute.
     */
    protected static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(120);

    /**
     * Registers the common IBM watsonx service-settings fields (url, api_version, model_id, project_id, rate_limit) onto the given
     * parser.
     */
    public static <B extends Builder<? extends IbmWatsonxServiceSettings>> void declareCommonFields(
        AbstractObjectParser<B, ConfigurationParseContext> parser
    ) {
        parser.declareString(Builder::setUrl, new ParseField(URL));
        parser.declareString(Builder::setApiVersion, new ParseField(API_VERSION));
        parser.declareString(Builder::setModelId, new ParseField(MODEL_ID));
        parser.declareString(Builder::setProjectId, new ParseField(PROJECT_ID));
        RateLimitSettings.declareRateLimitSettings(parser, Builder::setRateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
        // api_key appears in the same JSON block as service settings in REST requests; DefaultSecretSettings extracts it separately.
        // Declare it here as a no-op so the strict REQUEST parser does not reject it as an unknown field.
        parser.declareString((b, v) -> {}, new ParseField(DefaultSecretSettings.API_KEY));
    }

    private final URI uri;
    private final String apiVersion;
    private final String modelId;
    private final String projectId;
    private final RateLimitSettings rateLimitSettings;

    protected IbmWatsonxServiceSettings(
        URI uri,
        String apiVersion,
        String modelId,
        String projectId,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.uri = uri;
        this.apiVersion = apiVersion;
        this.modelId = modelId;
        this.projectId = projectId;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
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

    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
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
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IbmWatsonxServiceSettings that = (IbmWatsonxServiceSettings) o;
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

    /**
     * Accumulates the parsed common fields and assembles an {@link IbmWatsonxServiceSettings}, enforcing that the required
     * {@code url}, {@code api_version}, {@code model_id}, and {@code project_id} fields are present. Task-specific builders extend this
     * and contribute their own fields.
     *
     * @param <T> the task-specific settings type produced by {@link #build(URI, String, String, String, RateLimitSettings)}
     */
    public abstract static class Builder<T extends IbmWatsonxServiceSettings> {

        private String url;
        private String apiVersion;
        private String modelId;
        private String projectId;
        protected RateLimitSettings rateLimitSettings;

        public void setUrl(String url) {
            this.url = url;
        }

        public void setApiVersion(String apiVersion) {
            this.apiVersion = apiVersion;
        }

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        public void setProjectId(String projectId) {
            this.projectId = projectId;
        }

        public void setRateLimitSettings(RateLimitSettings rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }

        protected abstract T build(URI uri, String apiVersion, String modelId, String projectId, RateLimitSettings rateLimitSettings);

        public final T build() {
            validateStringIsNotNullOrEmpty(url, URL);
            validateStringIsNotNullOrEmpty(apiVersion, API_VERSION);
            validateStringIsNotNullOrEmpty(modelId, MODEL_ID);
            validateStringIsNotNullOrEmpty(projectId, PROJECT_ID);
            return build(createUri(url), apiVersion, modelId, projectId, rateLimitSettings);
        }
    }

    /**
     * Creates an {@link IbmWatsonxServiceSettings} from a map of settings using the given parser.
     *
     * @param map     the map to parse
     * @param context the context in which the parsing is done
     * @param parser  the parser to use for parsing the settings
     * @return the created {@link IbmWatsonxServiceSettings}
     */
    public static <T extends IbmWatsonxServiceSettings> T fromMap(
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
     * Registers the common IBM watsonx fields that may be changed by an update request. Only {@code rate_limit} is mutable; the
     * immutable fields (such as {@code model_id}, {@code url}, {@code api_version}, and {@code project_id}) are intentionally not
     * declared so that a strict update parser rejects attempts to change them.
     */
    public static void declareCommonUpdatableFields(AbstractObjectParser<? extends CommonUpdate, Void> parser) {
        RateLimitSettings.declareUpdatableRateLimitSettings(parser, CommonUpdate::setRateLimitSettings);
    }

    /**
     * Common fields parsed from an update request. Because settings are immutable, each subclass builds the new instance itself,
     * calling {@link #mergedRateLimitSettings(IbmWatsonxServiceSettings)} to resolve the shared fields.
     */
    public static class CommonUpdate {

        protected StatefulValue<RateLimitSettings> rateLimitSettings = StatefulValue.undefined();

        private void setRateLimitSettings(StatefulValue<RateLimitSettings> rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }

        /**
         * Resolves the rate limit settings to use after applying the update following the tri-state convention: an omitted field keeps
         * the current value, an explicit null resets the field to the default rate limit, and a present value replaces the current one.
         */
        protected RateLimitSettings mergedRateLimitSettings(IbmWatsonxServiceSettings existing) {
            return applyUpdate(rateLimitSettings, existing.rateLimitSettings(), DEFAULT_RATE_LIMIT_SETTINGS);
        }
    }
}
