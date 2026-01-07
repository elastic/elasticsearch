/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceString.DataFormat;
import org.elasticsearch.inference.InferenceString.DataType;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class handles the parsing of inputs used by the {@link TaskType#EMBEDDING} task type. The input for this task is specified using
 * a list of "content" objects, each of which specifies the {@link DataType}, {@link DataFormat} and the String value of the input. The
 * {@code format} field is optional, and if not specified will use the default {@link DataFormat} for the given {@link DataType}:
 * <pre>
 * "input": [
 *   {
 *     "content": {"type": "image", "format": "base64", "value": "image data"},
 *   },
 *   {
 *     "content": [
 *       {"type": "text", "value": "text input"},
 *       {"type": "image", "value": "image data"}
 *     ]
 *   }
 * ]</pre>
 * It is also possible to specify a single content object rather than a
 * list:
 * <pre>
 * "input": {
 *   "content": {"type": "text", "format": "text", "value": "text input"}
 * }</pre>
 * To preserve input compatibility with the existing {@link TaskType#TEXT_EMBEDDING} task, the input can also be specified as a single
 * String or a list of Strings, each of which will be parsed into a content object with {@link DataType} equal to
 * {@link DataType#TEXT} and {@link DataFormat} equal to {@link DataFormat#TEXT}:
 * <pre>
 * "input": "singe text input"</pre>
 * OR
 * <pre>
 * "input": ["first text input", "second text input"]</pre>
 * @param inputs The list of {@link InferenceStringGroup} inputs to generate embeddings for
 * @param inputType The {@link InputType} of the request
 */
public record EmbeddingRequest(List<InferenceStringGroup> inputs, InputType inputType) implements Writeable, ToXContentFragment {

    private static final String INPUT_FIELD = "input";
    private static final String INPUT_TYPE_FIELD = "input_type";

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<EmbeddingRequest, Void> PARSER = new ConstructingObjectParser<>(
        EmbeddingRequest.class.getSimpleName(),
        args -> new EmbeddingRequest((List<InferenceStringGroup>) args[0], (InputType) args[1])
    );

    static {
        PARSER.declareField(
            constructorArg(),
            (parser, context) -> parseInput(parser),
            new ParseField(INPUT_FIELD),
            ObjectParser.ValueType.OBJECT_ARRAY_OR_STRING
        );
        PARSER.declareField(
            optionalConstructorArg(),
            (parser, context) -> InputType.fromString(parser.text()),
            new ParseField(INPUT_TYPE_FIELD),
            ObjectParser.ValueType.STRING
        );
    }

    public static EmbeddingRequest of(List<InferenceStringGroup> contents) {
        return new EmbeddingRequest(contents, null);
    }

    public EmbeddingRequest(List<InferenceStringGroup> inputs, @Nullable InputType inputType) {
        this.inputs = inputs;
        this.inputType = Objects.requireNonNullElse(inputType, InputType.UNSPECIFIED);
    }

    public EmbeddingRequest(StreamInput in) throws IOException {
        this(in.readCollectionAsImmutableList(InferenceStringGroup::new), in.readEnum(InputType.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(inputs);
        out.writeEnum(inputType);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(INPUT_FIELD, inputs);
        builder.field(INPUT_TYPE_FIELD, inputType);
        return builder;
    }

    private static List<InferenceStringGroup> parseInput(XContentParser parser) throws IOException {
        var token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_STRING || token == XContentParser.Token.START_OBJECT) {
            // Single input of String or content object
            return singletonList(InferenceStringGroup.parse(parser));
        } else if (token == XContentParser.Token.START_ARRAY) {
            // Array of String or content objects
            return XContentParserUtils.parseList(parser, InferenceStringGroup::parse);
        }

        throw new XContentParseException("Unsupported token [" + token + "]");
    }
}
