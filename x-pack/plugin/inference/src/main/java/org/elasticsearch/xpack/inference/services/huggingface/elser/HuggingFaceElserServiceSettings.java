/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.elser;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.common.parser.ServiceSettingsOPBuilder;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.common.parser.UpdateServiceSettingsOPBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.parser.StatefulValue.applyUpdate;
import static org.elasticsearch.xpack.inference.common.parser.StringParser.validateStringIsNotNullOrEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;

/**
 * Service settings for the Hugging Face ELSER service. The only user-supplied fields are the {@code url} (required, immutable) and the
 * {@code rate_limit} (optional, updatable). The {@code max_input_tokens} value is a fixed constant driven by the ELSER model itself and
 * appears in the exposed XContent so clients can observe it, but it is not accepted on write.
 */
public class HuggingFaceElserServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        HuggingFaceRateLimitServiceSettings {

    public static final String NAME = "hugging_face_elser_service_settings";
    private static final int ELSER_TOKEN_LIMIT = 512;
    // At the time of writing HuggingFace hasn't posted the default rate limit for inference endpoints so this value is only a guess.
    // 3000 requests per minute.
    static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(3000);

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(false);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(true);

    /**
     * Creates an {@link ObjectParser} for the Hugging Face ELSER service settings.
     *
     * @param ignoreUnknownFields whether the parser should tolerate unknown fields. This is {@code false} for request parsing (so that
     *                            unexpected fields are rejected) and {@code true} for persisted configuration (so that fields written by
     *                            other versions - including the exposed {@code max_input_tokens} - are tolerated).
     * @return the parser
     */
    static ObjectParser<Builder, ConfigurationParseContext> createParser(boolean ignoreUnknownFields) {
        var parser = ServiceSettingsOPBuilder.of(
            ignoreUnknownFields,
            Builder::new,
            DEFAULT_RATE_LIMIT_SETTINGS,
            Builder::setRateLimitSettings
        ).build();
        parser.declareString(Builder::setUrl, new ParseField(ServiceFields.URL));
        return parser;
    }

    /**
     * Creates a new instance from a map of settings.
     *
     * @param map     the map containing the service settings
     * @param context the context for parsing configuration settings
     * @return a new instance of {@link HuggingFaceElserServiceSettings}
     */
    public static HuggingFaceElserServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            return parser.apply(xParser, context).build();
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, ModelConfigurations.SERVICE_SETTINGS);
        }
    }

    private final URI uri;
    private final RateLimitSettings rateLimitSettings;

    public HuggingFaceElserServiceSettings(String url) {
        this.uri = createUri(url);
        this.rateLimitSettings = DEFAULT_RATE_LIMIT_SETTINGS;
    }

    // default for testing
    HuggingFaceElserServiceSettings(URI uri, @Nullable RateLimitSettings rateLimitSettings) {
        this.uri = Objects.requireNonNull(uri);
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public HuggingFaceElserServiceSettings(StreamInput in) throws IOException {
        this.uri = createUri(in.readString());
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    @Override
    public HuggingFaceElserServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            return Update.PARSER.apply(xParser, null).mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse Hugging Face ELSER service settings update", e);
        }
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public URI uri() {
        return uri;
    }

    public int maxInputTokens() {
        return ELSER_TOKEN_LIMIT;
    }

    @Override
    public String modelId() {
        return null;
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
        builder.field(ServiceFields.URL, uri.toString());
        builder.field(ServiceFields.MAX_INPUT_TOKENS, ELSER_TOKEN_LIMIT);
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(uri.toString());
        rateLimitSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HuggingFaceElserServiceSettings that = (HuggingFaceElserServiceSettings) o;
        return Objects.equals(uri, that.uri) && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, rateLimitSettings);
    }

    /**
     * Accumulates the parsed fields and assembles a {@link HuggingFaceElserServiceSettings}, enforcing that the required
     * {@code url} field is present and a valid URI.
     */
    public static class Builder {

        private String url;
        private RateLimitSettings rateLimitSettings;

        public void setUrl(String url) {
            this.url = url;
        }

        public void setRateLimitSettings(RateLimitSettings rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }

        public HuggingFaceElserServiceSettings build() {
            validateStringIsNotNullOrEmpty(url, ServiceFields.URL);
            return new HuggingFaceElserServiceSettings(createUri(url), rateLimitSettings);
        }
    }

    /**
     * Parses an update request, which may only contain the mutable {@code rate_limit} field. Including any immutable field (such as
     * {@code url} or the read-only {@code max_input_tokens}) causes the strict parser to reject the request.
     */
    private static class Update {

        private static final ObjectParser<Update, Void> PARSER = UpdateServiceSettingsOPBuilder.of(
            Update::new,
            Update::setRateLimitSettings
        ).build();

        private StatefulValue<RateLimitSettings> rateLimitSettings = StatefulValue.undefined();

        private void setRateLimitSettings(StatefulValue<RateLimitSettings> rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }

        public HuggingFaceElserServiceSettings mergeInto(HuggingFaceElserServiceSettings existing) {
            return new HuggingFaceElserServiceSettings(
                existing.uri,
                applyUpdate(rateLimitSettings, existing.rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS)
            );
        }
    }
}
