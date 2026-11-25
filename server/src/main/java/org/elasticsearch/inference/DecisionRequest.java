/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.inference.InferenceString.TYPE_FIELD;
import static org.elasticsearch.inference.ModelConfigurations.TASK_SETTINGS;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class handles the parsing of inputs used by the {@link TaskType#DECISION} task type. The {@code "input"} field is the content to
 * make decisions about and is specified either as a string or as a single object specifying the {@link DataType}, {@link DataFormat} and
 * the String value of the input. The {@code format} field is optional, and if not specified will use the default {@link DataFormat} for
 * the given {@link DataType}.
 * <br>
 * String input:
 * <pre>
 * "input": "I was charged twice for my order."</pre>
 * Object input:
 * <pre>
 * "input": {"type": "image", "format": "base64", "value": "data:image/png;base64,..."}</pre>
 * The questions to answer are not part of the request itself. They are stored in the task settings of the inference endpoint and can be
 * replaced per request through the {@code task_settings} field (see {@link DecisionQuestions}):
 * <pre>
 * "task_settings": {
 *   "questions": {
 *     "is_urgent": {"type": "predicate", "instructions": "Does this need a response within the hour?"}
 *   }
 * }</pre>
 *
 * @param input        The {@link InferenceString} to make decisions about
 * @param taskSettings The map of task settings specific to this request
 */
public record DecisionRequest(InferenceString input, Map<String, Object> taskSettings) implements Writeable, ToXContentFragment {

    public static final String INPUT_FIELD = "input";

    /**
     * The {@link DataType}s that can be used as decision inputs.
     */
    public static final Set<DataType> SUPPORTED_DECISION_DATA_TYPES = Collections.unmodifiableSet(
        EnumSet.of(DataType.TEXT, DataType.IMAGE)
    );

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<DecisionRequest, Void> PARSER = new ConstructingObjectParser<>(
        DecisionRequest.class.getSimpleName(),
        args -> new DecisionRequest((InferenceString) args[0], (Map<String, Object>) args[1])
    );

    static {
        PARSER.declareField(
            constructorArg(),
            (parser, context) -> parseInput(parser),
            new ParseField(INPUT_FIELD),
            ObjectParser.ValueType.OBJECT_OR_STRING
        );
        PARSER.declareField(
            optionalConstructorArg(),
            (parser, context) -> parser.mapOrdered(),
            new ParseField(TASK_SETTINGS),
            ObjectParser.ValueType.OBJECT
        );
    }

    public static DecisionRequest of(String input) {
        return new DecisionRequest(InferenceString.ofText(input), null);
    }

    /**
     * @throws IllegalArgumentException if the input has a {@link DataType} outside {@link #SUPPORTED_DECISION_DATA_TYPES}
     */
    public DecisionRequest(InferenceString input, @Nullable Map<String, Object> taskSettings) {
        this.input = Objects.requireNonNull(input);
        this.taskSettings = Objects.requireNonNullElseGet(taskSettings, HashMap::new);
        validateDataType(input);
    }

    public DecisionRequest(StreamInput in) throws IOException {
        this(new InferenceString(in), in.readGenericMap());
    }

    private static void validateDataType(InferenceString input) {
        if (SUPPORTED_DECISION_DATA_TYPES.contains(input.dataType()) == false) {
            throw new IllegalArgumentException(
                Strings.format(
                    "Field [%s] contains unsupported [%s] value [%s]. Supported values are %s",
                    INPUT_FIELD,
                    TYPE_FIELD,
                    input.dataType(),
                    SUPPORTED_DECISION_DATA_TYPES
                )
            );
        }
    }

    private static InferenceString parseInput(XContentParser parser) throws IOException {
        var token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            return InferenceString.ofText(parser.text());
        } else if (token == XContentParser.Token.START_OBJECT) {
            return InferenceString.PARSER.parse(parser, null);
        }
        throw new XContentParseException(Strings.format("Unsupported token [%s] for field [%s]", token, INPUT_FIELD));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        input.writeTo(out);
        out.writeGenericMap(taskSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(INPUT_FIELD, input);
        if (taskSettings.isEmpty() == false) {
            builder.field(TASK_SETTINGS, taskSettings);
        }
        return builder;
    }
}
