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
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.inference.InferenceUtils;
import org.elasticsearch.xpack.inference.common.parser.Headers;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.inference.common.parser.Headers.UNDEFINED_INSTANCE;

/**
 * Base class for Azure OpenAI task settings. Holds optional user and optional
 * custom HTTP headers via {@link Headers}.
 */
public abstract class AzureOpenAiTaskSettings<T extends AzureOpenAiTaskSettings<T>> implements TaskSettings {

    protected static final TransportVersion INFERENCE_AZURE_OPENAI_TASK_SETTINGS_HEADERS = TransportVersion.fromName(
        "inference_azure_openai_task_settings_headers"
    );

    // Default for testing
    protected record CommonSettings(StatefulValue<String> user, Headers headers) {
        public CommonSettings {
            Objects.requireNonNull(user);
            Objects.requireNonNull(headers);
        }

        public boolean isEmpty() {
            // user is empty if it is not present or if it is empty
            // (although the parser should prevent an empty string by throwing a validation exception)
            return user.orElse("").isEmpty() && headers().isEmpty();
        }
    }

    /**
     * Sentinel for parser: when "user" field is present with value null.
     */
    private static final Object USER_PARSER_NULL_SENTINEL = new Object();

    private static final ConstructingObjectParser<CommonSettings, Void> STORAGE_PARSER = createParser(true);
    private static final ConstructingObjectParser<CommonSettings, Void> REQUEST_PARSER = createParser(false);

    private static ConstructingObjectParser<CommonSettings, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<CommonSettings, Void> constructingObjectParser = new ConstructingObjectParser<>(
            "azure_openai_task_settings_parser",
            ignoreUnknownFields,
            args -> createSettings(args[0], args[1])
        );

        constructingObjectParser.declareField(
            optionalConstructorArg(),
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? USER_PARSER_NULL_SENTINEL : p.text(),
            new ParseField(AzureOpenAiServiceFields.USER),
            ObjectParser.ValueType.STRING_OR_NULL
        );

        Headers.initParser(constructingObjectParser);

        return constructingObjectParser;
    }

    private static CommonSettings createSettings(Object userArg, Object headersArg) {
        StatefulValue<String> user;
        if (userArg == null) {
            user = StatefulValue.undefined();
        } else if (userArg == USER_PARSER_NULL_SENTINEL) {
            user = StatefulValue.nullInstance();
        } else {
            user = StatefulValue.of((String) userArg);
        }

        Headers headers = headersArg instanceof Headers
            ? (Headers) headersArg
            : Headers.create(headersArg, ModelConfigurations.TASK_SETTINGS);
        return new CommonSettings(user, headers);
    }

    protected abstract static class Factory<T> {
        private T emptyInstance;

        protected abstract T create(CommonSettings commonSettings);

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
                CommonSettings parsed;

                if (configurationParseContext == ConfigurationParseContext.REQUEST) {
                    parsed = REQUEST_PARSER.parse(parser, null);
                    validateParsedRequest(parsed);
                } else {
                    parsed = STORAGE_PARSER.parse(parser, null);
                }

                return factory.create(parsed);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse Azure OpenAI task settings", e);
        }
    }

    private static void validateParsedRequest(CommonSettings parsed) {
        if (parsed.user().isPresent() && parsed.user().get().isEmpty()) {
            var validationException = new ValidationException();
            validationException.addValidationError(
                InferenceUtils.mustBeNonEmptyString(AzureOpenAiServiceFields.USER, ModelConfigurations.TASK_SETTINGS)
            );
            throw validationException;
        }
    }

    private final CommonSettings taskSettings;
    private final Factory<T> factory;

    protected AzureOpenAiTaskSettings(@Nullable String user, @Nullable Headers headers, Factory<T> factory) {
        this(createSettings(user, headers), factory);
    }

    protected AzureOpenAiTaskSettings(CommonSettings taskSettings, Factory<T> factory) {
        this.taskSettings = Objects.requireNonNull(taskSettings);
        this.factory = Objects.requireNonNull(factory);
    }

    protected AzureOpenAiTaskSettings(StreamInput in, Factory<T> factory) throws IOException {
        this(readTaskSettingsFromStream(in), factory);
    }

    private static CommonSettings readTaskSettingsFromStream(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(INFERENCE_AZURE_OPENAI_TASK_SETTINGS_HEADERS)) {
            var user = StatefulValue.read(in, StreamInput::readString);
            return new CommonSettings(user, new Headers(in));
        } else {
            var user = StatefulValue.<String>undefined();
            var userString = in.readOptionalString();
            if (Strings.isNullOrEmpty(userString) == false) {
                user = StatefulValue.of(userString);
            }

            return new CommonSettings(user, UNDEFINED_INSTANCE);
        }
    }

    public StatefulValue<String> user() {
        return taskSettings.user();
    }

    public Headers headers() {
        return taskSettings.headers();
    }

    @Override
    public boolean isEmpty() {
        return taskSettings.isEmpty();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        var user = taskSettings.user();

        if (user.isPresent() && user.get().isEmpty() == false) {
            builder.field(AzureOpenAiServiceFields.USER, user.get());
        }

        taskSettings.headers().toXContent(builder, params);
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

        var userToUse = taskSettings.user();
        if (updated.user().isPresent()) {
            userToUse = updated.user();
        } else if (updated.user().isNull()) {
            userToUse = StatefulValue.undefined();
        }

        var headersToUse = taskSettings.headers();
        if (updated.headers().isPresent()) {
            headersToUse = updated.headers();
        } else if (updated.headers().isNull()) {
            headersToUse = Headers.UNDEFINED_INSTANCE;
        }

        if (userToUse.isUndefined() && headersToUse.mapValue().isUndefined()) {
            return factory.emptySettings();
        }

        return factory.create(new CommonSettings(userToUse, headersToUse));
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
        if (out.getTransportVersion().supports(INFERENCE_AZURE_OPENAI_TASK_SETTINGS_HEADERS)) {
            StatefulValue.write(out, taskSettings.user(), StreamOutput::writeString);
            taskSettings.headers().writeTo(out);
        } else {
            out.writeOptionalString(user().orElse(null));
        }
    }
}
