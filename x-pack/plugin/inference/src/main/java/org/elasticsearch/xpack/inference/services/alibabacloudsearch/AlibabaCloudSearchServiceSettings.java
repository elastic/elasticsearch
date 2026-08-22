/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.inference.common.parser.StatefulValue.applyUpdate;
import static org.elasticsearch.xpack.inference.common.parser.StringParser.validateStringIsNotNullOrEmpty;

/**
 * Holds the settings common to every AlibabaCloud AI Search task (service id, host, workspace, HTTP schema, and rate limiting)
 * together with the parsing, serialization, and update machinery that would otherwise be duplicated. Unlike other providers whose
 * task-specific settings extend a common superclass, the task-specific AlibabaCloud AI Search settings <em>wrap</em> an instance of
 * this class, which is also serialized on its own; it therefore stays concrete.
 */
public class AlibabaCloudSearchServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        AlibabaCloudSearchRateLimitServiceSettings {

    public static final String NAME = "alibabacloud_search_service_settings";
    public static final String SERVICE_ID = "service_id";
    public static final String HOST = "host";
    public static final String WORKSPACE_NAME = "workspace";
    public static final String HTTP_SCHEMA_NAME = "http_schema";
    private static final Set<String> VALID_SCHEMAS = Set.of("https", "http");

    static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(1_000);

    /**
     * Registers the common AlibabaCloud AI Search service-settings fields (service_id, host, workspace, http_schema, rate_limit) onto
     * the given parser.
     */
    public static <B extends Builder<? extends ServiceSettings>> void declareCommonFields(
        AbstractObjectParser<B, ConfigurationParseContext> parser
    ) {
        parser.declareString(Builder::setServiceId, new ParseField(SERVICE_ID));
        parser.declareString(Builder::setHost, new ParseField(HOST));
        parser.declareString(Builder::setWorkspaceName, new ParseField(WORKSPACE_NAME));
        parser.declareString(Builder::setHttpSchema, new ParseField(HTTP_SCHEMA_NAME));
        RateLimitSettings.declareRateLimitSettings(parser, Builder::setRateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
        // api_key appears in the same JSON block as service settings in REST requests; DefaultSecretSettings extracts it separately.
        // Declare it here as a no-op so the strict REQUEST parser does not reject it as an unknown field.
        parser.declareString((b, v) -> {}, new ParseField(DefaultSecretSettings.API_KEY));
    }

    /**
     * Validates that {@code http_schema}, when present, is one of the supported schemas, throwing an {@link IllegalArgumentException}
     * otherwise.
     */
    static void validateHttpSchema(@Nullable String httpSchema) {
        if (httpSchema != null && VALID_SCHEMAS.contains(httpSchema) == false) {
            throw new IllegalArgumentException("Invalid value for [" + HTTP_SCHEMA_NAME + "]. Must be one of [https, http]");
        }
    }

    /**
     * Accumulates the parsed common fields and assembles an {@link AlibabaCloudSearchServiceSettings}, enforcing that the required
     * {@code service_id}, {@code host} and {@code workspace} fields are present and that {@code http_schema} is valid. Task-specific
     * builders extend this and contribute their own fields.
     *
     * @param <T> the task-specific settings type produced by {@link #build(AlibabaCloudSearchServiceSettings)}
     */
    public abstract static class Builder<T extends ServiceSettings> {

        private String serviceId;
        private String host;
        private String workspaceName;
        private String httpSchema;
        private RateLimitSettings rateLimitSettings;

        public void setServiceId(String serviceId) {
            this.serviceId = serviceId;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public void setWorkspaceName(String workspaceName) {
            this.workspaceName = workspaceName;
        }

        public void setHttpSchema(String httpSchema) {
            this.httpSchema = httpSchema;
        }

        public void setRateLimitSettings(RateLimitSettings rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }

        protected abstract T build(AlibabaCloudSearchServiceSettings commonSettings);

        public final T build() {
            validateStringIsNotNullOrEmpty(serviceId, SERVICE_ID);
            validateStringIsNotNullOrEmpty(host, HOST);
            validateStringIsNotNullOrEmpty(workspaceName, WORKSPACE_NAME);
            validateHttpSchema(httpSchema);
            return build(new AlibabaCloudSearchServiceSettings(serviceId, host, workspaceName, httpSchema, rateLimitSettings));
        }
    }

    /**
     * Creates a task-specific settings instance from a map of settings using the given parser.
     *
     * @param map     the map to parse
     * @param context the context in which the parsing is done
     * @param parser  the parser to use for parsing the settings
     * @return the created settings
     */
    public static <T extends ServiceSettings> T fromMap(
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
     * Registers the common AlibabaCloud AI Search fields that may be changed by an update request: {@code http_schema} and
     * {@code rate_limit}. The immutable fields ({@code service_id}, {@code host} and {@code workspace}) are intentionally not declared
     * so that a strict update parser rejects attempts to change them.
     */
    public static void declareCommonUpdatableFields(AbstractObjectParser<? extends CommonUpdate, Void> parser) {
        StatefulValue.declareNullable(parser, (update, value) -> update.httpSchema = value, p -> {
            String value = p.text();
            validateHttpSchema(value);
            return value;
        }, new ParseField(HTTP_SCHEMA_NAME), ObjectParser.ValueType.STRING_OR_NULL);
        RateLimitSettings.declareUpdatableRateLimitSettings(parser, (update, value) -> update.rateLimitSettings = value);
        // api_key appears in the same JSON block as service settings in update requests; DefaultSecretSettings extracts it separately.
        // Declare it here as a no-op so the strict update parser does not reject it as an unknown field.
        parser.declareString((u, v) -> {}, new ParseField(DefaultSecretSettings.API_KEY));
    }

    /**
     * Common fields parsed from an update request. Because settings are immutable, each task-specific update builds the new instance
     * itself, calling {@link #mergedCommonSettings(AlibabaCloudSearchServiceSettings)} to resolve the shared fields.
     */
    public static class CommonUpdate {

        protected StatefulValue<String> httpSchema = StatefulValue.undefined();
        protected StatefulValue<RateLimitSettings> rateLimitSettings = StatefulValue.undefined();

        /**
         * Resolves the common settings to use after applying the update following the tri-state convention: an omitted field keeps
         * the current value, an explicit null resets the field to its default ({@code null} for {@code http_schema}, the default rate
         * limit for {@code rate_limit}), and a present value replaces the current one.
         */
        protected AlibabaCloudSearchServiceSettings mergedCommonSettings(AlibabaCloudSearchServiceSettings existing) {
            return new AlibabaCloudSearchServiceSettings(
                existing.modelId(),
                existing.getHost(),
                existing.getWorkspaceName(),
                applyUpdate(httpSchema, existing.getHttpSchema()),
                applyUpdate(rateLimitSettings, existing.rateLimitSettings(), DEFAULT_RATE_LIMIT_SETTINGS)
            );
        }
    }

    private final String serviceId;
    private final String host;
    private final String workspaceName;
    private final String httpSchema;
    private final RateLimitSettings rateLimitSettings;

    public AlibabaCloudSearchServiceSettings(
        String serviceId,
        String host,
        String workspaceName,
        @Nullable String httpSchema,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.serviceId = serviceId;
        this.host = host;
        this.workspaceName = workspaceName;
        this.httpSchema = httpSchema;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public AlibabaCloudSearchServiceSettings(StreamInput in) throws IOException {
        this.serviceId = in.readString();
        this.host = in.readString();
        this.workspaceName = in.readString();
        this.httpSchema = in.readOptionalString();
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    @Override
    public String modelId() {
        return serviceId;
    }

    public String getHost() {
        return host;
    }

    public String getWorkspaceName() {
        return workspaceName;
    }

    public String getHttpSchema() {
        return httpSchema;
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

        toXContentFragment(builder, params);

        builder.endObject();
        return builder;
    }

    public XContentBuilder toXContentFragment(XContentBuilder builder, Params params) throws IOException {
        return toXContentFragmentOfExposedFields(builder, params);
    }

    @Override
    public XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        if (serviceId != null) {
            builder.field(SERVICE_ID, serviceId);
        }
        builder.field(HOST, host);
        builder.field(WORKSPACE_NAME, workspaceName);
        if (httpSchema != null) {
            builder.field(HTTP_SCHEMA_NAME, httpSchema);
        }
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return this;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(serviceId);
        out.writeString(host);
        out.writeString(workspaceName);
        out.writeOptionalString(httpSchema);
        rateLimitSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlibabaCloudSearchServiceSettings that = (AlibabaCloudSearchServiceSettings) o;
        return Objects.equals(serviceId, that.serviceId)
            && Objects.equals(host, that.host)
            && Objects.equals(workspaceName, that.workspaceName)
            && Objects.equals(httpSchema, that.httpSchema)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceId, host, workspaceName, httpSchema, rateLimitSettings);
    }
}
