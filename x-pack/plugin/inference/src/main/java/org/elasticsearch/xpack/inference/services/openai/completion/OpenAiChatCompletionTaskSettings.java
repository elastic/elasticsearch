/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xpack.inference.common.parser.Headers;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.openai.OpenAiTaskSettings;

import java.io.IOException;
import java.util.Map;

public class OpenAiChatCompletionTaskSettings extends OpenAiTaskSettings {

    public static final String NAME = "openai_completion_task_settings";
    private static final String PARSER_NAME = "openai_completion_task_settings_parser";

    private static final TransportVersion INFERENCE_API_OPENAI_HEADERS = TransportVersion.fromName("inference_api_openai_headers");

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(PARSER_NAME, false, Builder::new);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(PARSER_NAME, true, Builder::new);

    public static final OpenAiChatCompletionTaskSettings EMPTY = new OpenAiChatCompletionTaskSettings(
        StatefulValue.undefined(),
        Headers.UNDEFINED_INSTANCE
    );

    public static OpenAiChatCompletionTaskSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        if (map.isEmpty()) {
            return EMPTY;
        }
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        return parseSettingsFromMap(map, context, parser);
    }

    @Override
    public OpenAiChatCompletionTaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return parseUpdate(newSettings, Update.PARSER).mergeInto(this);
    }

    // Package-private for tests in the same package
    OpenAiChatCompletionTaskSettings(StatefulValue<String> user, Headers headers) {
        super(user, headers);
    }

    public static OpenAiChatCompletionTaskSettings read(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(INFERENCE_API_OPENAI_TASK_SETTINGS_TRI_STATE)) {
            return new OpenAiChatCompletionTaskSettings(StatefulValue.read(in, StreamInput::readString), new Headers(in));
        }

        StatefulValue<String> user;
        var userString = in.readOptionalString();
        if (Strings.isNullOrEmpty(userString) == false) {
            user = StatefulValue.of(userString);
        } else {
            user = StatefulValue.undefined();
        }

        Headers headers;

        if (in.getTransportVersion().supports(INFERENCE_API_OPENAI_HEADERS)) {
            var headersMap = in.readOptionalImmutableMap(StreamInput::readString, StreamInput::readString);
            headers = headersMap == null ? Headers.UNDEFINED_INSTANCE : new Headers(StatefulValue.of(headersMap));
        } else {
            headers = Headers.UNDEFINED_INSTANCE;
        }

        return new OpenAiChatCompletionTaskSettings(user, headers);
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
        if (out.getTransportVersion().supports(INFERENCE_API_OPENAI_TASK_SETTINGS_TRI_STATE)) {
            StatefulValue.write(out, user(), StreamOutput::writeString);
            headers().writeTo(out);
            return;
        }

        out.writeOptionalString(user().orElse(null));
        if (out.getTransportVersion().supports(INFERENCE_API_OPENAI_HEADERS)) {
            out.writeOptionalMap(headers().mapValue().orElse(null), StreamOutput::writeString, StreamOutput::writeString);
        }
    }

    private static class Builder extends OpenAiTaskSettings.Builder<OpenAiChatCompletionTaskSettings> {
        @Override
        protected OpenAiChatCompletionTaskSettings build(StatefulValue<String> user, Headers headers) {
            return new OpenAiChatCompletionTaskSettings(user, headers);
        }
    }

    /**
     * Parses an update request. The parser is strict, so unknown fields are rejected.
     */
    private static class Update extends OpenAiTaskSettings.CommonUpdate {

        private static final ObjectParser<Update, Void> PARSER = createUpdateParser(PARSER_NAME, Update::new);

        OpenAiChatCompletionTaskSettings mergeInto(OpenAiChatCompletionTaskSettings existing) {
            return new OpenAiChatCompletionTaskSettings(mergedUser(existing), mergedHeaders(existing));
        }
    }
}
