/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.completion;

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
 * Settings for the Hugging Face chat completion service. The user-supplied fields are {@code url} (required, immutable),
 * {@code model_id} (optional, immutable) and {@code rate_limit} (optional, updatable).
 */
public class HuggingFaceChatCompletionServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        HuggingFaceRateLimitServiceSettings {

    public static final String NAME = "hugging_face_completion_service_settings";
    // At the time of writing HuggingFace hasn't posted the default rate limit for inference endpoints so this value is only a guess.
    // 3000 requests per minute.
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(3000);

    private static final TransportVersion ML_INFERENCE_HUGGING_FACE_CHAT_COMPLETION_ADDED = TransportVersion.fromName(
        "ml_inference_hugging_face_chat_completion_added"
    );

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(false);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(true);

    /**
     * Creates an {@link ObjectParser} for the Hugging Face chat completion service settings.
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
        parser.declareString(Builder::setModelId, new ParseField(ServiceFields.MODEL_ID));
        parser.declareString(Builder::setUrl, new ParseField(ServiceFields.URL));
        return parser;
    }

    /**
     * Creates a new instance from a map of settings.
     *
     * @param map     the map containing the service settings
     * @param context the context for parsing configuration settings
     * @return a new instance of {@link HuggingFaceChatCompletionServiceSettings}
     */
    public static HuggingFaceChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            var builder = parser.apply(xParser, context);
            // TODO: remove once all Hugging Face service settings are parser-based and usesParserForServiceSettings can be enabled on
            // HuggingFaceService, which also creates chat completion models. The object parser reads the map through an XContent view
            // without consuming its entries, so the parsed fields must be removed explicitly to satisfy the caller's check that no
            // unknown settings remain in the map.
            map.remove(ServiceFields.MODEL_ID);
            map.remove(ServiceFields.URL);
            map.remove(RateLimitSettings.FIELD_NAME);
            return builder.build();
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, ModelConfigurations.SERVICE_SETTINGS);
        }
    }

    @Override
    public HuggingFaceChatCompletionServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            var update = Update.PARSER.apply(xParser, null);
            // TODO: remove once all Hugging Face service settings are parser-based and usesParserForServiceSettings can be enabled on
            // HuggingFaceService, which also creates chat completion models. The object parser reads the map through an XContent view
            // without consuming its entries, so the parsed field must be removed explicitly to satisfy the caller's check that no
            // unknown settings remain in the map.
            serviceSettings.remove(RateLimitSettings.FIELD_NAME);
            return update.mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse Hugging Face chat completion service settings update", e);
        }
    }

    private final String modelId;
    private final URI uri;
    private final RateLimitSettings rateLimitSettings;

    public HuggingFaceChatCompletionServiceSettings(@Nullable String modelId, String url, @Nullable RateLimitSettings rateLimitSettings) {
        this(modelId, createUri(url), rateLimitSettings);
    }

    public HuggingFaceChatCompletionServiceSettings(@Nullable String modelId, URI uri, @Nullable RateLimitSettings rateLimitSettings) {
        this.modelId = modelId;
        this.uri = Objects.requireNonNull(uri);
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    /**
     * Creates a new instance of {@link HuggingFaceChatCompletionServiceSettings} from a stream input.
     * @param in the stream input
     * @throws IOException if an I/O error occurs
     */
    public HuggingFaceChatCompletionServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readOptionalString();
        this.uri = createUri(in.readString());
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public URI uri() {
        return uri;
    }

    @Override
    public String modelId() {
        return modelId;
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
        if (modelId != null) {
            builder.field(ServiceFields.MODEL_ID, modelId);
        }
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
        return ML_INFERENCE_HUGGING_FACE_CHAT_COMPLETION_ADDED;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return version.supports(ML_INFERENCE_HUGGING_FACE_CHAT_COMPLETION_ADDED);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(modelId);
        out.writeString(uri.toString());
        rateLimitSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        HuggingFaceChatCompletionServiceSettings that = (HuggingFaceChatCompletionServiceSettings) object;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(uri, that.uri)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, uri, rateLimitSettings);
    }

    /**
     * Accumulates the parsed fields and assembles a {@link HuggingFaceChatCompletionServiceSettings}, enforcing that the required
     * {@code url} field is present and a valid URI.
     */
    public static class Builder {

        private String modelId;
        private String url;
        private RateLimitSettings rateLimitSettings;

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public void setRateLimitSettings(RateLimitSettings rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }

        public HuggingFaceChatCompletionServiceSettings build() {
            validateStringIsNotNullOrEmpty(url, ServiceFields.URL);
            return new HuggingFaceChatCompletionServiceSettings(modelId, createUri(url, ServiceFields.URL), rateLimitSettings);
        }
    }

    /**
     * Parses an update request, which may only contain the mutable {@code rate_limit} field. Including any immutable field (such as
     * {@code url} or {@code model_id}) causes the strict parser to reject the request.
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

        public HuggingFaceChatCompletionServiceSettings mergeInto(HuggingFaceChatCompletionServiceSettings existing) {
            return new HuggingFaceChatCompletionServiceSettings(
                existing.modelId,
                existing.uri,
                applyUpdate(rateLimitSettings, existing.rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS)
            );
        }
    }

}
