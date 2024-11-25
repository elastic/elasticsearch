/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public record UnifiedCompletionRequest(
    List<Message> messages,
    @Nullable String model,
    @Nullable Long maxCompletionTokens,
    @Nullable Integer n,
    @Nullable Stop stop,
    @Nullable Boolean stream,
    @Nullable Float temperature,
    @Nullable ToolChoice toolChoice,
    @Nullable List<Tool> tool,
    @Nullable Float topP,
    @Nullable String user
) implements Writeable {

    public sealed interface Content extends NamedWriteable permits ContentObjects, ContentString {}

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<UnifiedCompletionRequest, Void> PARSER = new ConstructingObjectParser<>(
        InferenceAction.NAME,
        args -> new UnifiedCompletionRequest(
            (List<Message>) args[0],
            (String) args[1],
            (Long) args[2],
            (Integer) args[3],
            (Stop) args[4],
            (Boolean) args[5],
            (Float) args[6],
            (ToolChoice) args[7],
            (List<Tool>) args[8],
            (Float) args[9],
            (String) args[10]
        )
    );

    static {
        PARSER.declareObjectArray(constructorArg(), Message.PARSER::apply, new ParseField("messages"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("model"));
        PARSER.declareLong(optionalConstructorArg(), new ParseField("max_completion_tokens"));
        PARSER.declareInt(optionalConstructorArg(), new ParseField("n"));
        PARSER.declareField(optionalConstructorArg(), (p, c) -> parseStop(p), new ParseField("stop"), ObjectParser.ValueType.VALUE_ARRAY);
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField("stream"));
        PARSER.declareFloat(optionalConstructorArg(), new ParseField("temperature"));
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> parseToolChoice(p),
            new ParseField("tool_choice"),
            ObjectParser.ValueType.OBJECT_OR_STRING
        );
        PARSER.declareObjectArray(optionalConstructorArg(), Tool.PARSER::apply, new ParseField("tools"));
        PARSER.declareFloat(optionalConstructorArg(), new ParseField("top_p"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("user"));
    }

    public UnifiedCompletionRequest(StreamInput in) throws IOException {
        this(
            in.readCollectionAsImmutableList(Message::new),
            in.readOptionalString(),
            in.readOptionalVLong(),
            in.readOptionalVInt(),
            in.readOptionalNamedWriteable(Stop.class),
            in.readOptionalBoolean(),
            in.readOptionalFloat(),
            in.readOptionalNamedWriteable(ToolChoice.class),
            in.readCollectionAsImmutableList(Tool::new),
            in.readOptionalFloat(),
            in.readOptionalString()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(messages);
        out.writeOptionalString(model);
        out.writeOptionalVLong(maxCompletionTokens);
        out.writeOptionalVInt(n);
        out.writeOptionalNamedWriteable(stop);
        out.writeOptionalBoolean(stream);
        out.writeOptionalFloat(temperature);
        out.writeOptionalNamedWriteable(toolChoice);
        out.writeOptionalCollection(tool);
        out.writeOptionalFloat(topP);
        out.writeOptionalString(user);
    }

    public record Message(Content content, String role, @Nullable String name, @Nullable String toolCallId, List<ToolCall> toolCalls)
        implements
            Writeable {

        @SuppressWarnings("unchecked")
        static final ConstructingObjectParser<Message, Void> PARSER = new ConstructingObjectParser<>(
            Message.class.getSimpleName(),
            args -> new Message((Content) args[0], (String) args[1], (String) args[2], (String) args[3], (List<ToolCall>) args[4])
        );

        static {
            PARSER.declareField(constructorArg(), (p, c) -> parseContent(p), new ParseField("content"), ObjectParser.ValueType.VALUE_ARRAY);
            PARSER.declareString(constructorArg(), new ParseField("role"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("name"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("tool_call_id"));
            PARSER.declareObjectArray(optionalConstructorArg(), ToolCall.PARSER::apply, new ParseField("tool_calls"));
        }

        private static Content parseContent(XContentParser parser) throws IOException {
            var token = parser.currentToken();
            if (token == XContentParser.Token.START_ARRAY) {
                var parsedContentObjects = XContentParserUtils.parseList(parser, (p) -> ContentObject.PARSER.apply(p, null));
                return new ContentObjects(parsedContentObjects);
            } else if (token == XContentParser.Token.VALUE_STRING) {
                return ContentString.of(parser);
            }

            throw new XContentParseException("Unsupported token [" + token + "]");
        }

        public Message(StreamInput in) throws IOException {
            this(
                in.readNamedWriteable(Content.class),
                in.readString(),
                in.readOptionalString(),
                in.readOptionalString(),
                in.readCollectionAsImmutableList(ToolCall::new)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeNamedWriteable(content);
            out.writeString(role);
            out.writeOptionalString(name);
            out.writeOptionalString(toolCallId);
            out.writeCollection(toolCalls);
        }
    }

    public record ContentObjects(List<ContentObject> contentObjects) implements Content, Writeable {

        public static final String NAME = "content_objects";

        public ContentObjects(StreamInput in) throws IOException {
            this(in.readCollectionAsImmutableList(ContentObject::new));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(contentObjects);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }

    public record ContentObject(String text, String type) implements Writeable {
        static final ConstructingObjectParser<ContentObject, Void> PARSER = new ConstructingObjectParser<>(
            ContentObject.class.getSimpleName(),
            args -> new ContentObject((String) args[0], (String) args[1])
        );

        static {
            PARSER.declareString(constructorArg(), new ParseField("text"));
            PARSER.declareString(constructorArg(), new ParseField("type"));
        }

        public ContentObject(StreamInput in) throws IOException {
            this(in.readString(), in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(text);
            out.writeString(type);
        }

    }

    public record ContentString(String content) implements Content, NamedWriteable {
        public static final String NAME = "content_string";

        public static ContentString of(XContentParser parser) throws IOException {
            var content = parser.text();
            return new ContentString(content);
        }

        public ContentString(StreamInput in) throws IOException {
            this(in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(content);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.value(content);
        }
    }

    public record ToolCall(String id, FunctionField function, String type) implements Writeable {

        static final ConstructingObjectParser<ToolCall, Void> PARSER = new ConstructingObjectParser<>(
            ToolCall.class.getSimpleName(),
            args -> new ToolCall((String) args[0], (FunctionField) args[1], (String) args[2])
        );

        static {
            PARSER.declareString(constructorArg(), new ParseField("id"));
            PARSER.declareObject(constructorArg(), FunctionField.PARSER::apply, new ParseField("function"));
            PARSER.declareString(constructorArg(), new ParseField("type"));
        }

        public ToolCall(StreamInput in) throws IOException {
            this(in.readString(), new FunctionField(in), in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            function.writeTo(out);
            out.writeString(type);
        }

        public record FunctionField(String arguments, String name) implements Writeable {
            static final ConstructingObjectParser<FunctionField, Void> PARSER = new ConstructingObjectParser<>(
                "tool_call_function_field",
                args -> new FunctionField((String) args[0], (String) args[1])
            );

            static {
                PARSER.declareString(constructorArg(), new ParseField("arguments"));
                PARSER.declareString(constructorArg(), new ParseField("name"));
            }

            public FunctionField(StreamInput in) throws IOException {
                this(in.readString(), in.readString());
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(arguments);
                out.writeString(name);
            }
        }
    }

    private static Stop parseStop(XContentParser parser) throws IOException {
        var token = parser.currentToken();
        if (token == XContentParser.Token.START_ARRAY) {
            var parsedStopValues = XContentParserUtils.parseList(parser, XContentParser::text);
            return new StopValues(parsedStopValues);
        } else if (token == XContentParser.Token.VALUE_STRING) {
            return StopString.of(parser);
        }

        throw new XContentParseException("Unsupported token [" + token + "]");
    }

    public sealed interface Stop extends NamedWriteable permits StopString, StopValues {}

    public record StopString(String value) implements Stop, NamedWriteable {
        public static final String NAME = "stop_string";

        public static StopString of(XContentParser parser) throws IOException {
            var content = parser.text();
            return new StopString(content);
        }

        public StopString(StreamInput in) throws IOException {
            this(in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(value);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }

    public record StopValues(List<String> values) implements Stop, NamedWriteable {
        public static final String NAME = "stop_values";

        public StopValues(StreamInput in) throws IOException {
            this(in.readStringCollectionAsImmutableList());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(values);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }

    private static ToolChoice parseToolChoice(XContentParser parser) throws IOException {
        var token = parser.currentToken();
        if (token == XContentParser.Token.START_OBJECT) {
            return ToolChoiceObject.PARSER.apply(parser, null);
        } else if (token == XContentParser.Token.VALUE_STRING) {
            return ToolChoiceString.of(parser);
        }

        throw new XContentParseException("Unsupported token [" + token + "]");
    }

    public sealed interface ToolChoice extends NamedWriteable permits ToolChoiceObject, ToolChoiceString {}

    public record ToolChoiceObject(String type, FunctionField function) implements ToolChoice, NamedWriteable {

        public static final String NAME = "tool_choice_object";

        static final ConstructingObjectParser<ToolChoiceObject, Void> PARSER = new ConstructingObjectParser<>(
            ToolChoiceObject.class.getSimpleName(),
            args -> new ToolChoiceObject((String) args[0], (FunctionField) args[1])
        );

        static {
            PARSER.declareString(constructorArg(), new ParseField("type"));
            PARSER.declareObject(constructorArg(), FunctionField.PARSER::apply, new ParseField("function"));
        }

        public ToolChoiceObject(StreamInput in) throws IOException {
            this(in.readString(), new FunctionField(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(type);
            function.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        public record FunctionField(String name) implements Writeable {
            static final ConstructingObjectParser<FunctionField, Void> PARSER = new ConstructingObjectParser<>(
                "tool_choice_function_field",
                args -> new FunctionField((String) args[0])
            );

            static {
                PARSER.declareString(constructorArg(), new ParseField("name"));
            }

            public FunctionField(StreamInput in) throws IOException {
                this(in.readString());
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(name);
            }
        }
    }

    public record ToolChoiceString(String value) implements ToolChoice, NamedWriteable {
        public static final String NAME = "tool_choice_string";

        public static ToolChoiceString of(XContentParser parser) throws IOException {
            var content = parser.text();
            return new ToolChoiceString(content);
        }

        public ToolChoiceString(StreamInput in) throws IOException {
            this(in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(value);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }

    public record Tool(String type, FunctionField function) implements Writeable {

        static final ConstructingObjectParser<Tool, Void> PARSER = new ConstructingObjectParser<>(
            Tool.class.getSimpleName(),
            args -> new Tool((String) args[0], (FunctionField) args[1])
        );

        static {
            PARSER.declareString(constructorArg(), new ParseField("type"));
            PARSER.declareObject(constructorArg(), FunctionField.PARSER::apply, new ParseField("function"));
        }

        public Tool(StreamInput in) throws IOException {
            this(in.readString(), new FunctionField(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(type);
            function.writeTo(out);
        }

        public record FunctionField(
            @Nullable String description,
            String name,
            @Nullable Map<String, Object> parameters, // TODO can we parse this as a string?
            @Nullable Boolean strict
        ) implements Writeable {

            @SuppressWarnings("unchecked")
            static final ConstructingObjectParser<FunctionField, Void> PARSER = new ConstructingObjectParser<>(
                "tool_function_field",
                args -> new FunctionField((String) args[0], (String) args[1], (Map<String, Object>) args[2], (Boolean) args[3])
            );

            static {
                PARSER.declareString(optionalConstructorArg(), new ParseField("description"));
                PARSER.declareString(constructorArg(), new ParseField("name"));
                PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.mapOrdered(), new ParseField("parameters"));
                PARSER.declareBoolean(optionalConstructorArg(), new ParseField("strict"));
            }

            public FunctionField(StreamInput in) throws IOException {
                this(in.readOptionalString(), in.readString(), in.readGenericMap(), in.readOptionalBoolean());
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeOptionalString(description);
                out.writeString(name);
                out.writeGenericMap(parameters);
                out.writeOptionalBoolean(strict);
            }
        }
    }
}
