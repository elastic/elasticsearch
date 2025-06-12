/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEmptyString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;

public class InputTypeTranslator implements ToXContentFragment, Writeable {
    public static final String INPUT_TYPE_TRANSLATOR = "input_type";
    public static final String TRANSLATION = "translation";
    public static final String DEFAULT = "default";
    public static final InputTypeTranslator EMPTY_TRANSLATOR = new InputTypeTranslator(null, null);

    public static InputTypeTranslator fromMap(Map<String, Object> map, ValidationException validationException, String serviceName) {
        if (map == null || map.isEmpty()) {
            return EMPTY_TRANSLATOR;
        }

        Map<String, Object> inputTypeTranslation = Objects.requireNonNullElse(
            extractOptionalMap(map, INPUT_TYPE_TRANSLATOR, ModelConfigurations.SERVICE_SETTINGS, validationException),
            new HashMap<>(Map.of())
        );

        Map<String, Object> translationMap = extractOptionalMap(
            inputTypeTranslation,
            TRANSLATION,
            INPUT_TYPE_TRANSLATOR,
            validationException
        );

        var validatedTranslation = InputType.validateInputTypeTranslationValues(translationMap, validationException);

        var defaultValue = extractOptionalEmptyString(inputTypeTranslation, DEFAULT, validationException);

        throwIfNotEmptyMap(inputTypeTranslation, INPUT_TYPE_TRANSLATOR, "input_type_translator");

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new InputTypeTranslator(validatedTranslation, defaultValue);
    }

    private final Map<InputType, String> inputTypeTranslation;
    private final String defaultValue;

    public InputTypeTranslator(@Nullable Map<InputType, String> inputTypeTranslation, @Nullable String defaultValue) {
        this.inputTypeTranslation = Objects.requireNonNullElse(inputTypeTranslation, Map.of());
        this.defaultValue = Objects.requireNonNullElse(defaultValue, "");
    }

    public InputTypeTranslator(StreamInput in) throws IOException {
        this.inputTypeTranslation = in.readImmutableMap(keyReader -> keyReader.readEnum(InputType.class), StreamInput::readString);
        this.defaultValue = in.readString();
    }

    public Map<InputType, String> getInputTypeTranslation() {
        return inputTypeTranslation;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(INPUT_TYPE_TRANSLATOR);
        {
            builder.startObject(TRANSLATION);
            for (var entry : inputTypeTranslation.entrySet()) {
                builder.field(entry.getKey().toString(), entry.getValue());
            }
            builder.endObject();
            builder.field(DEFAULT, defaultValue);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(inputTypeTranslation, StreamOutput::writeEnum, StreamOutput::writeString);
        out.writeString(defaultValue);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        InputTypeTranslator that = (InputTypeTranslator) o;
        return Objects.equals(inputTypeTranslation, that.inputTypeTranslation) && Objects.equals(defaultValue, that.defaultValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputTypeTranslation, defaultValue);
    }
}
