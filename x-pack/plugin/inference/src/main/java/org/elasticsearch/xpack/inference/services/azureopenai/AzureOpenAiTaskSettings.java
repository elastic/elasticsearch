/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.inference.common.parser.Headers;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Base class for Azure OpenAI task settings (embeddings and completion). Holds optional user and optional
 * custom HTTP headers via {@link Headers}.
 */
public abstract class AzureOpenAiTaskSettings<T extends AzureOpenAiTaskSettings<T>> implements TaskSettings {

    protected static final TransportVersion INFERENCE_AZURE_OPENAI_TASK_SETTINGS_HEADERS = TransportVersion.fromName(
        "inference_azure_openai_task_settings_headers"
    );

    private static final Settings EMPTY_SETTINGS = new Settings(null, null);

    public record Settings(@Nullable String user, @Nullable Headers headers) {}

    public static Settings createSettings(String user, Headers headers) {
        if (user == null && headers == null) {
            return EMPTY_SETTINGS;
        }
        return new Settings(user, headers);
    }

    private static final ConstructingObjectParser<Settings, Void> STORAGE_PARSER = createParser(true);

    private static final ConstructingObjectParser<Settings, Void> REQUEST_PARSER = createParser(false);

    private static ConstructingObjectParser<Settings, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<Settings, Void> a = new ConstructingObjectParser<>(
            "azure_openai_task_settings_parser",
            ignoreUnknownFields,
            args -> createSettings((String) args[0], Headers.create(args[1]))
        );

        a.declareString(optionalConstructorArg(), new ParseField(AzureOpenAiServiceFields.USER));
        Headers.initParser(a);

        return a;
    }

    private static Settings parseSettingsFromMap(Map<String, Object> map, ConfigurationParseContext configurationParseContext) {
        if (map.isEmpty()) {
            return EMPTY_SETTINGS;
        }


        try {
            try (
                XContentBuilder xContent = XContentBuilder.builder(JsonXContent.jsonXContent).map(map);
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    XContentParserConfiguration.EMPTY,
                    Strings.toString(xContent)
                )
            ) {
                if (configurationParseContext == ConfigurationParseContext.REQUEST) {
                    return REQUEST_PARSER.parse(parser, null);
                } else {
                    return STORAGE_PARSER.parse(parser, null);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private final Settings taskSettings;

    public AzureOpenAiTaskSettings(Map<String, Object> map, ConfigurationParseContext configurationParseContext) {
        this(parseSettingsFromMap(map, configurationParseContext));
    }

    public AzureOpenAiTaskSettings(@Nullable String user, @Nullable Headers headers) {
        this(createSettings(user, headers));
    }

    protected AzureOpenAiTaskSettings(Settings taskSettings) {
        this.taskSettings = Objects.requireNonNull(taskSettings);
    }

    public AzureOpenAiTaskSettings(StreamInput in) throws IOException {
        this(readTaskSettingsFromStream(in));
    }

    private static Settings readTaskSettingsFromStream(StreamInput in) throws IOException {
        String user = in.readOptionalString();
        Headers headers = in.getTransportVersion().supports(INFERENCE_AZURE_OPENAI_TASK_SETTINGS_HEADERS)
            ? in.readOptionalWriteable(Headers::new)
            : null;
        return createSettings(user, headers);
    }

    public String user() {
        return taskSettings.user();
    }

    /**
     * Optional custom HTTP headers to send with the request. May be null or have null/empty map inside.
     */
    public Headers headers() {
        return taskSettings.headers();
    }

    @Override
    public boolean isEmpty() {
        var user = taskSettings.user();
        var headers = taskSettings.headers();
        return (user == null || user.isEmpty()) && (headers == null || headers.isEmpty());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (taskSettings.user() != null) {
            builder.field(AzureOpenAiServiceFields.USER, taskSettings.user());
        }
        if (taskSettings.headers() != null) {
            taskSettings.headers().toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AzureOpenAiTaskSettings<?> that = (AzureOpenAiTaskSettings<?>) o;
        return Objects.equals(taskSettings, that.taskSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskSettings);
    }

    @Override
    public T updatedTaskSettings(Map<String, Object> newSettings) {
        Settings updated = parseSettingsFromMap(new HashMap<>(newSettings), ConfigurationParseContext.REQUEST);
        var userToUse = updated.user() == null ? taskSettings.user() : updated.user();
        var headersToUse = updated.headers() == null ? taskSettings.headers() : updated.headers();
        return create(userToUse, headersToUse);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        assert false : "should never be called when supportsVersion is used";
        return INFERENCE_AZURE_OPENAI_TASK_SETTINGS_HEADERS;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return INFERENCE_AZURE_OPENAI_TASK_SETTINGS_HEADERS.supports(version);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(user());
        if (out.getTransportVersion().supports(INFERENCE_AZURE_OPENAI_TASK_SETTINGS_HEADERS)) {
            out.writeOptionalWriteable(headers());
        }
    }

    protected abstract T create(@Nullable String user, @Nullable Headers headers);
}
