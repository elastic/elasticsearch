/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xpack.inference.common.parser.Headers;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.openai.OpenAiTaskSettings;

import java.io.IOException;
import java.util.Map;

/**
 * Defines the task settings for the openai embeddings service.
 * <p>
 * User is an optional unique identifier representing the end-user, which can help OpenAI to monitor and detect abuse.
 * <a href="https://platform.openai.com/docs/api-reference/embeddings/create">see the openai docs for more details</a>
 */
public class OpenAiEmbeddingsTaskSettings extends OpenAiTaskSettings {

    public static final String NAME = "openai_embeddings_task_settings";
    private static final String PARSER_NAME = "openai_embeddings_task_settings_parser";

    // Default for testing
    static final TransportVersion INFERENCE_API_OPENAI_EMBEDDINGS_HEADERS = TransportVersion.fromName(
        "inference_api_openai_embeddings_headers"
    );

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(PARSER_NAME, false, Builder::new);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(PARSER_NAME, true, Builder::new);

    public static final OpenAiEmbeddingsTaskSettings EMPTY = new OpenAiEmbeddingsTaskSettings(
        StatefulValue.undefined(),
        Headers.UNDEFINED_INSTANCE
    );

    public static OpenAiEmbeddingsTaskSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        if (map.isEmpty()) {
            return EMPTY;
        }
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        return parseSettingsFromMap(map, context, parser);
    }

    @Override
    public OpenAiEmbeddingsTaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return parseUpdate(newSettings, Update.PARSER).mergeInto(this);
    }

    // Package-private for tests in the same package
    OpenAiEmbeddingsTaskSettings(StatefulValue<String> user, Headers headers) {
        super(user, headers);
    }

    public OpenAiEmbeddingsTaskSettings(StreamInput in) throws IOException {
        super(in, INFERENCE_API_OPENAI_EMBEDDINGS_HEADERS);
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
        writeCommonSettings(out, INFERENCE_API_OPENAI_EMBEDDINGS_HEADERS);
    }

    private static class Builder extends OpenAiTaskSettings.Builder<OpenAiEmbeddingsTaskSettings> {
        @Override
        protected OpenAiEmbeddingsTaskSettings build(StatefulValue<String> user, Headers headers) {
            return new OpenAiEmbeddingsTaskSettings(user, headers);
        }
    }

    /**
     * Parses an update request. The parser is strict, so unknown fields are rejected.
     */
    private static class Update extends OpenAiTaskSettings.CommonUpdate {

        private static final ObjectParser<Update, Void> PARSER = new ObjectParser<>(PARSER_NAME, Update::new);

        static {
            OpenAiTaskSettings.declareCommonUpdatableFields(PARSER);
        }

        OpenAiEmbeddingsTaskSettings mergeInto(OpenAiEmbeddingsTaskSettings existing) {
            return new OpenAiEmbeddingsTaskSettings(mergedUser(existing), mergedHeaders(existing));
        }
    }
}
