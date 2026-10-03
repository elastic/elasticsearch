/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.rerank;

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
 * Service settings for the Hugging Face rerank service. The only user-supplied fields are the {@code url} (required, immutable) and the
 * {@code rate_limit} (optional, updatable). Hugging Face requires that the model be chosen when initializing a deployment within their
 * service, so {@code model_id} is intentionally not part of the service settings.
 */
public class HuggingFaceRerankServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        HuggingFaceRateLimitServiceSettings {

    public static final String NAME = "hugging_face_rerank_service_settings";

    // At the time of writing HuggingFace hasn't posted the default rate limit for inference endpoints so this value is only a guess.
    // 3000 requests per minute.
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(3000);

    private static final TransportVersion ML_INFERENCE_HUGGING_FACE_RERANK_ADDED = TransportVersion.fromName(
        "ml_inference_sagemaker_chat_completion"
    );

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(false);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(true);

    /**
     * Creates an {@link ObjectParser} for the Hugging Face rerank service settings.
     *
     * @param ignoreUnknownFields whether the parser should tolerate unknown fields. This is {@code false} for request parsing (so that
     *                            unexpected fields are rejected) and {@code true} for persisted configuration (so that fields written by
     *                            other versions are tolerated).
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
     * @return a new instance of {@link HuggingFaceRerankServiceSettings}
     */
    public static HuggingFaceRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            return parser.apply(xParser, context).build();
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, ModelConfigurations.SERVICE_SETTINGS);
        }
    }

    @Override
    public HuggingFaceRerankServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            var update = Update.PARSER.apply(xParser, null);
            return update.mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse Hugging Face rerank service settings update", e);
        }
    }

    private final URI uri;
    private final RateLimitSettings rateLimitSettings;

    public HuggingFaceRerankServiceSettings(String url) {
        uri = createUri(url);
        rateLimitSettings = DEFAULT_RATE_LIMIT_SETTINGS;
    }

    HuggingFaceRerankServiceSettings(URI uri, @Nullable RateLimitSettings rateLimitSettings) {
        this.uri = Objects.requireNonNull(uri);
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public HuggingFaceRerankServiceSettings(StreamInput in) throws IOException {
        uri = createUri(in.readString());
        rateLimitSettings = new RateLimitSettings(in);
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public URI uri() {
        return uri;
    }

    // model is not defined in the service settings.
    // since hugging face requires that the model be chosen when initializing a deployment within their service.
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
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        assert false : "should never be called when supportsVersion is used";
        return ML_INFERENCE_HUGGING_FACE_RERANK_ADDED;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return version.supports(ML_INFERENCE_HUGGING_FACE_RERANK_ADDED);
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
        HuggingFaceRerankServiceSettings that = (HuggingFaceRerankServiceSettings) o;
        return Objects.equals(uri, that.uri) && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, rateLimitSettings);
    }

    /**
     * Accumulates the parsed fields and assembles a {@link HuggingFaceRerankServiceSettings}, enforcing that the required
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

        public HuggingFaceRerankServiceSettings build() {
            validateStringIsNotNullOrEmpty(url, ServiceFields.URL);
            return new HuggingFaceRerankServiceSettings(createUri(url, ServiceFields.URL), rateLimitSettings);
        }
    }

    /**
     * Parses an update request, which may only contain the mutable {@code rate_limit} field. Including the immutable {@code url} field
     * causes the strict parser to reject the request.
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

        public HuggingFaceRerankServiceSettings mergeInto(HuggingFaceRerankServiceSettings existing) {
            return new HuggingFaceRerankServiceSettings(
                existing.uri,
                applyUpdate(rateLimitSettings, existing.rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS)
            );
        }
    }
}
