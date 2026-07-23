/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.completion;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.llama.LlamaServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;

/**
 * Represents the settings for a Llama chat completion service. Extends {@link LlamaServiceSettings}, which carries the
 * model ID, URI, and rate limit settings shared across all Llama tasks. Chat completion adds no settings of its own.
 */
public class LlamaChatCompletionServiceSettings extends LlamaServiceSettings {
    public static final String NAME = "llama_completion_service_settings";

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(false);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(true);

    /**
     * Creates an {@link ObjectParser} for the Llama chat completion service settings.
     *
     * @param ignoreUnknownFields whether the parser should tolerate unknown fields. This is {@code false} for request parsing (so that
     *                            unexpected fields are rejected) and {@code true} for persisted configuration (so that fields written by
     *                            other versions are tolerated).
     * @return the parser
     */
    static ObjectParser<Builder, ConfigurationParseContext> createParser(boolean ignoreUnknownFields) {
        ObjectParser<Builder, ConfigurationParseContext> parser = new ObjectParser<>(
            ModelConfigurations.SERVICE_SETTINGS,
            ignoreUnknownFields,
            Builder::new
        );
        LlamaServiceSettings.declareCommonFields(parser);
        return parser;
    }

    /**
     * Creates a new instance of LlamaChatCompletionServiceSettings from a map of settings.
     *
     * @param map the map containing the service settings
     * @param context the context for parsing configuration settings
     * @return a new instance of LlamaChatCompletionServiceSettings
     */
    public static LlamaChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        return LlamaServiceSettings.fromMap(map, context, parser);
    }

    @Override
    public LlamaChatCompletionServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            return Update.PARSER.apply(xParser, null).mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse Llama chat completion service settings update", e);
        }
    }

    /**
     * Constructs a new LlamaChatCompletionServiceSettings from a StreamInput.
     *
     * @param in the StreamInput to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public LlamaChatCompletionServiceSettings(StreamInput in) throws IOException {
        super(in.readString(), createUri(in.readString()), new RateLimitSettings(in));
    }

    /**
     * Constructs a new LlamaChatCompletionServiceSettings with the specified model ID, URI, and rate limit settings.
     *
     * @param modelId the ID of the model
     * @param uri the URI of the service
     * @param rateLimitSettings the rate limit settings for the service
     */
    public LlamaChatCompletionServiceSettings(String modelId, URI uri, @Nullable RateLimitSettings rateLimitSettings) {
        super(modelId, uri, rateLimitSettings);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId());
        out.writeString(uri().toString());
        rateLimitSettings().writeTo(out);
    }

    /**
     * Builds a {@link LlamaChatCompletionServiceSettings} from the common Llama fields, enforcing that the required
     * {@code model_id} and {@code url} fields are present.
     */
    public static class Builder extends LlamaServiceSettings.Builder<LlamaChatCompletionServiceSettings> {

        @Override
        protected LlamaChatCompletionServiceSettings build(String modelId, URI uri, RateLimitSettings rateLimitSettings) {
            return new LlamaChatCompletionServiceSettings(modelId, uri, rateLimitSettings);
        }
    }

    /**
     * Parses an update request, which may only contain the mutable {@code rate_limit} field. Including any immutable field (such as
     * {@code model_id} or {@code url}) causes the strict parser to reject the request.
     */
    private static class Update extends LlamaServiceSettings.CommonUpdate {

        private static final ObjectParser<Update, Void> PARSER = new ObjectParser<>(ModelConfigurations.SERVICE_SETTINGS, Update::new);

        static {
            LlamaServiceSettings.declareCommonUpdatableFields(PARSER);
        }

        public LlamaChatCompletionServiceSettings mergeInto(LlamaChatCompletionServiceSettings existing) {
            return new LlamaChatCompletionServiceSettings(existing.modelId(), existing.uri(), mergedRateLimitSettings(existing));
        }
    }
}
