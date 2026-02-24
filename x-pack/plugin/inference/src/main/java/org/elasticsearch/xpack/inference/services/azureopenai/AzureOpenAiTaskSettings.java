/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.inference.InferenceUtils;
import org.elasticsearch.xpack.inference.common.parser.Headers;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Base class for Azure OpenAI task settings (embeddings and completion). Holds optional user and optional
 * custom HTTP headers via {@link Headers}.
 */
public abstract class AzureOpenAiTaskSettings<T extends AzureOpenAiTaskSettings<T>> implements TaskSettings {

    private static final Settings EMPTY_SETTINGS = new Settings(null, null);

    protected static final TransportVersion INFERENCE_AZURE_OPENAI_TASK_SETTINGS_HEADERS = TransportVersion.fromName(
        "inference_azure_openai_task_settings_headers"
    );

    protected record Settings(@Nullable String user, @Nullable Headers headers) {}

    private static final ConstructingObjectParser<Settings, Void> STORAGE_PARSER = createParser(true);
    private static final ConstructingObjectParser<Settings, Void> REQUEST_PARSER = createParser(false);

    private static ConstructingObjectParser<Settings, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<Settings, Void> constructingObjectParser = new ConstructingObjectParser<>(
            "azure_openai_task_settings_parser",
            ignoreUnknownFields,
            args -> createSettings((String) args[0], Headers.create(args[1]))
        );

        constructingObjectParser.declareString(optionalConstructorArg(), new ParseField(AzureOpenAiServiceFields.USER));
        Headers.initParser(constructingObjectParser);

        return constructingObjectParser;
    }

    private static Settings createSettings(@Nullable String user, @Nullable Headers headers) {
        if (user == null && headers == null) {
            return EMPTY_SETTINGS;
        }
        return new Settings(user, headers);
    }

    protected abstract static class Factory<T> {
        private T emptyInstance;

        protected abstract T create(@Nullable String user, @Nullable Headers headers);

        protected abstract T createEmptyInstance();

        public T emptySettings() {
            // Ideally we'd be able to pass the empty instance in via the Factory constructor, but since the empty instance relies on the
            // factory to be created, we have to lazily create it here. The empty instance will call the AzureOpenAiTaskSettings
            // constructor with the factory. If we don't do it this way we end up getting an NPE in the constructor because the factory
            // hasn't finished initialization yet.
            if (emptyInstance == null) {
                emptyInstance = createEmptyInstance();
            }
            return emptyInstance;
        }
    }

    protected static <T extends AzureOpenAiTaskSettings<T>> T parseSettingsFromMap(
        Map<String, Object> map,
        ConfigurationParseContext configurationParseContext,
        Factory<T> factory
    ) {
        if (map.isEmpty()) {
            return factory.emptySettings();
        }

        try {
            try (
                var xContent = XContentBuilder.builder(JsonXContent.jsonXContent).map(map);
                var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, Strings.toString(xContent))
            ) {
                Settings createdSettings;

                if (configurationParseContext == ConfigurationParseContext.REQUEST) {
                    createdSettings = REQUEST_PARSER.parse(parser, null);
                    validateSettings(createdSettings);
                } else {
                    createdSettings = STORAGE_PARSER.parse(parser, null);
                }

                return factory.create(createdSettings.user(), createdSettings.headers());
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse Azure OpenAI task settings", e);
        }
    }

    private static void validateSettings(Settings settings) {
        var validationException = new ValidationException();

        if (settings.user() != null && settings.user().isEmpty()) {
            validationException.addValidationError(
                InferenceUtils.mustBeNonEmptyString(AzureOpenAiServiceFields.USER, ModelConfigurations.TASK_SETTINGS)
            );
            throw validationException;
        }
    }

    private final Settings taskSettings;
    private final Factory<T> factory;

    protected AzureOpenAiTaskSettings(@Nullable String user, @Nullable Headers headers, Factory<T> factory) {
        this(createSettings(user, headers), factory);
    }

    protected AzureOpenAiTaskSettings(Settings taskSettings, Factory<T> factory) {
        this.taskSettings = Objects.requireNonNull(taskSettings);
        this.factory = Objects.requireNonNull(factory);
    }

    protected AzureOpenAiTaskSettings(StreamInput in, Factory<T> factory) throws IOException {
        this(readTaskSettingsFromStream(in), factory);
    }

    private static Settings readTaskSettingsFromStream(StreamInput in) throws IOException {
        var user = in.readOptionalString();
        var headers = in.getTransportVersion().supports(INFERENCE_AZURE_OPENAI_TASK_SETTINGS_HEADERS)
            ? in.readOptionalWriteable(Headers::new)
            : null;
        return createSettings(user, headers);
    }

    public String user() {
        return taskSettings.user();
    }

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
        var updated = parseSettingsFromMap(new HashMap<>(newSettings), ConfigurationParseContext.REQUEST, factory);
        var userToUse = updated.user() == null ? taskSettings.user() : updated.user();
        var headersToUse = updated.headers() == null ? taskSettings.headers() : updated.headers();
        return factory.create(userToUse, headersToUse);
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
}
