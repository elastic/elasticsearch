/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.inference.InferenceUtils;
import org.elasticsearch.xpack.inference.common.parser.Headers;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.common.parser.Headers.UNDEFINED_INSTANCE;

public abstract class OpenAiTaskSettings implements TaskSettings {

    public static final TransportVersion INFERENCE_API_OPENAI_TASK_SETTINGS_TRI_STATE = TransportVersion.fromName(
        "inference_api_openai_task_settings_tri_state"
    );

    public abstract static class Builder<T extends OpenAiTaskSettings> {

        private StatefulValue<String> user = StatefulValue.undefined();
        private Headers headers = UNDEFINED_INSTANCE;

        public void setUser(StatefulValue<String> user) {
            this.user = Objects.requireNonNull(user);
        }

        public void setHeadersArg(Object headersArg) {
            this.headers = Headers.create(headersArg, ModelConfigurations.TASK_SETTINGS);
        }

        protected abstract T build(StatefulValue<String> user, Headers headers);

        public final T build(ConfigurationParseContext context) {
            // Persisted settings are parsed leniently; only user-supplied requests are validated.
            if (context == ConfigurationParseContext.REQUEST) {
                validateUserIsNotEmpty(user);
            }
            return build(user, headers);
        }
    }

    public static <B extends Builder<? extends OpenAiTaskSettings>> ObjectParser<B, ConfigurationParseContext> createParser(
        String parserName,
        boolean ignoreUnknownFields,
        Supplier<B> builderSupplier
    ) {
        ObjectParser<B, ConfigurationParseContext> parser = new ObjectParser<>(parserName, ignoreUnknownFields, builderSupplier);

        StatefulValue.declareNullable(
            parser,
            Builder::setUser,
            XContentParser::text,
            new ParseField(OpenAiServiceFields.USER),
            ObjectParser.ValueType.STRING_OR_NULL
        );

        Headers.declare(parser, Builder::setHeadersArg);

        return parser;
    }

    protected static <T extends OpenAiTaskSettings> T parseSettingsFromMap(
        Map<String, Object> map,
        ConfigurationParseContext context,
        ObjectParser<? extends Builder<T>, ConfigurationParseContext> parser
    ) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            return parser.apply(xParser, context).build(context);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, ModelConfigurations.TASK_SETTINGS);
        }
    }

    /**
     * Creates a strict parser for an update request with the common updatable fields ({@code user} and {@code headers}) already
     * declared. Unknown fields are rejected so callers get an error for a misspelled or unsupported field.
     */
    public static <U extends CommonUpdate> ObjectParser<U, Void> createUpdateParser(String parserName, Supplier<U> updateSupplier) {
        var parser = new ObjectParser<U, Void>(parserName, updateSupplier);

        StatefulValue.declareNullable(
            parser,
            (update, value) -> update.user = value,
            XContentParser::text,
            new ParseField(OpenAiServiceFields.USER),
            ObjectParser.ValueType.STRING_OR_NULL
        );

        Headers.declare(parser, (update, value) -> update.headers = Headers.create(value, ModelConfigurations.TASK_SETTINGS));

        return parser;
    }

    /**
     * Fields parsed from an update request. Settings are immutable, so each subclass builds the new instance itself from
     * {@link #mergedUser} / {@link #mergedHeaders}.
     */
    public static class CommonUpdate {

        protected StatefulValue<String> user = StatefulValue.undefined();
        protected Headers headers = UNDEFINED_INSTANCE;

        public void validate() {
            validateUserIsNotEmpty(user);
        }

        protected StatefulValue<String> mergedUser(OpenAiTaskSettings existing) {
            return StatefulValue.applyUpdate(user, existing.user);
        }

        protected Headers mergedHeaders(OpenAiTaskSettings existing) {
            return Headers.applyUpdate(headers, existing.headers);
        }
    }

    protected static <U extends CommonUpdate> U parseUpdate(Map<String, Object> map, ObjectParser<U, Void> parser) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            var update = parser.apply(xParser, null);
            update.validate();
            return update;
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}] update", e, ModelConfigurations.TASK_SETTINGS);
        }
    }

    private static void validateUserIsNotEmpty(StatefulValue<String> user) {
        if (user.isPresent() && user.get().isEmpty()) {
            var validationException = new ValidationException();
            validationException.addValidationError(
                InferenceUtils.mustBeNonEmptyString(OpenAiServiceFields.USER, ModelConfigurations.TASK_SETTINGS)
            );
            throw validationException;
        }
    }

    private final StatefulValue<String> user;
    private final Headers headers;

    protected OpenAiTaskSettings(StatefulValue<String> user, Headers headers) {
        this.user = Objects.requireNonNull(user);
        this.headers = Objects.requireNonNull(headers);
    }

    public StatefulValue<String> user() {
        return user;
    }

    public Headers headers() {
        return headers;
    }

    @Override
    public boolean isEmpty() {
        return user.orElse("").isEmpty() && headers.isEmpty();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (user.isPresent() && user.get().isEmpty() == false) {
            builder.field(OpenAiServiceFields.USER, user.get());
        }

        headers.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OpenAiTaskSettings that = (OpenAiTaskSettings) o;
        return Objects.equals(user, that.user) && Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, headers);
    }

    @Override
    public abstract OpenAiTaskSettings updatedTaskSettings(Map<String, Object> newSettings);
}
