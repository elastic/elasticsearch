/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
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
    @Nullable List<String> stop,
    @Nullable Float temperature,
    @Nullable ToolChoice toolChoice,
    @Nullable List<Tool> tools,
    @Nullable Float topP
) implements Writeable {

    public sealed interface Content extends NamedWriteable permits ContentObjects, ContentString {}

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<UnifiedCompletionRequest, Void> PARSER = new ConstructingObjectParser<>(
        UnifiedCompletionRequest.class.getSimpleName(),
        args -> new UnifiedCompletionRequest(
            (List<Message>) args[0],
            (String) args[1],
            (Long) args[2],
            (List<String>) args[3],
            (Float) args[4],
            (ToolChoice) args[5],
            (List<Tool>) args[6],
            (Float) args[7]
        )
    );

    static {
        PARSER.declareObjectArray(constructorArg(), Message.PARSER::apply, new ParseField("messages"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("model"));
        PARSER.declareLong(optionalConstructorArg(), new ParseField("max_completion_tokens"));
        PARSER.declareStringArray(optionalConstructorArg(), new ParseField("stop"));
        PARSER.declareFloat(optionalConstructorArg(), new ParseField("temperature"));
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> parseToolChoice(p),
            new ParseField("tool_choice"),
            ObjectParser.ValueType.OBJECT_OR_STRING
        );
        PARSER.declareObjectArray(optionalConstructorArg(), Tool.PARSER::apply, new ParseField("tools"));
        PARSER.declareFloat(optionalConstructorArg(), new ParseField("top_p"));
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(Content.class, ContentObjects.NAME, ContentObjects::new),
            new NamedWriteableRegistry.Entry(Content.class, ContentString.NAME, ContentString::new),
            new NamedWriteableRegistry.Entry(ToolChoice.class, ToolChoiceObject.NAME, ToolChoiceObject::new),
            new NamedWriteableRegistry.Entry(ToolChoice.class, ToolChoiceString.NAME, ToolChoiceString::new)
        );
    }

    public static UnifiedCompletionRequest of(List<Message> messages) {
        return new UnifiedCompletionRequest(messages, null, null, null, null, null, null, null);
    }

    public UnifiedCompletionRequest(StreamInput in) throws IOException {
        this(
            in.readCollectionAsImmutableList(Message::new),
            in.readOptionalString(),
            in.readOptionalVLong(),
            in.readOptionalStringCollectionAsList(),
            in.readOptionalFloat(),
            in.readOptionalNamedWriteable(ToolChoice.class),
            in.readOptionalCollectionAsList(Tool::new),
            in.readOptionalFloat()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(messages);
        out.writeOptionalString(model);
        out.writeOptionalVLong(maxCompletionTokens);
        out.writeOptionalStringCollection(stop);
        out.writeOptionalFloat(temperature);
        out.writeOptionalNamedWriteable(toolChoice);
        out.writeOptionalCollection(tools);
        out.writeOptionalFloat(topP);
    }

    public record Message(Content content, String role, @Nullable String toolCallId, @Nullable List<ToolCall> toolCalls)
        implements
            Writeable {

        @SuppressWarnings("unchecked")
        static final ConstructingObjectParser<Message, Void> PARSER = new ConstructingObjectParser<>(
            Message.class.getSimpleName(),
            args -> new Message((Content) args[0], (String) args[1], (String) args[2], (List<ToolCall>) args[3])
        );

        static {
            PARSER.declareField(
                optionalConstructorArg(),
                (p, c) -> parseContent(p),
                new ParseField("content"),
                ObjectParser.ValueType.VALUE_ARRAY
            );
            PARSER.declareString(constructorArg(), new ParseField("role"));
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

            throw new XContentParseException("Expected an array start token or a value string token but found token [" + token + "]");
        }

        public Message(StreamInput in) throws IOException {
            this(
                in.readOptionalNamedWriteable(Content.class),
                in.readString(),
                in.readOptionalString(),
                in.readOptionalCollectionAsList(ToolCall::new)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalNamedWriteable(content);
            out.writeString(role);
            out.writeOptionalString(toolCallId);
            out.writeOptionalCollection(toolCalls);
        }
    }

    public record ContentObjects(List<ContentObject> contentObjects) implements Content, NamedWriteable {

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

        public String toString() {
            return text + ":" + type;
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

        public String toString() {
            return content;
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
            @Nullable Map<String, Object> parameters,
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
