/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

public class UnifiedCompletionRequest {

    static final ConstructingObjectParser<Thing, Void> PARSER = new ConstructingObjectParser<>(
        InferenceAction.NAME,
        args -> new Thing(args[0])
    );

    static {

    }

    public static class Thing {
        private final Object obj;

        Thing(Object obj) {
            this.obj = obj;
        }
    }

    // TODO convert these to static classes instead of record to make transport changes easier in the future
    public record Message(Content content, String role, @Nullable String name, @Nullable String toolCallId, List<ToolCall> toolCalls) {

        @SuppressWarnings("unchecked")
        static final ConstructingObjectParser<Message, Void> PARSER = new ConstructingObjectParser<>(
            Message.class.getSimpleName(),
            args -> new Message((Content) args[0], (String) args[1], (String) args[2], (String) args[3], (List<ToolCall>) args[4])
        );
        static {
            PARSER.declareField(
                ConstructingObjectParser.constructorArg(),
                (p, c) -> parseContent(p),
                new ParseField("content"),
                ObjectParser.ValueType.VALUE
            );
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("role"));
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("name"));
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("tool_call_id"));
            PARSER.declareObjectArray(
                ConstructingObjectParser.optionalConstructorArg(),
                ToolCall.PARSER::apply,
                new ParseField("tool_calls")
            );
        }

        private static Content parseContent(XContentParser parser) throws IOException {
            var token = parser.currentToken();
            if (token == XContentParser.Token.START_OBJECT) {
                return ContentObject.PARSER.parse(parser, null);
            } else if (token == XContentParser.Token.VALUE_STRING) {
                return ContentString.of(parser);
            }

            throw new XContentParseException("Unsupported token [" + token + "]");
        }
    }

    sealed interface Content permits ContentObject, ContentString {}

    public record ContentObject(String text, String type) implements Content {
        static final ConstructingObjectParser<ContentObject, Void> PARSER = new ConstructingObjectParser<>(
            ContentObject.class.getSimpleName(),
            args -> new ContentObject((String) args[0], (String) args[1])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("text"));
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("type"));
        }
    }

    public record ContentString(String content) implements Content {
        public static ContentString of(XContentParser parser) throws IOException {
            var content = parser.text();
            return new ContentString(content);
        }
    }

    public record ToolCall(String id, FunctionField function, String type) {

        static final ConstructingObjectParser<ToolCall, Void> PARSER = new ConstructingObjectParser<>(
            ToolCall.class.getSimpleName(),
            args -> new ToolCall((String) args[0], (FunctionField) args[1], (String) args[2])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("id"));
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), FunctionField.PARSER::apply, new ParseField("function"));
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("type"));
        }

        public record FunctionField(String arguments, String name) {
            static final ConstructingObjectParser<FunctionField, Void> PARSER = new ConstructingObjectParser<>(
                FunctionField.class.getSimpleName(),
                args -> new FunctionField((String) args[0], (String) args[1])
            );

            static {
                PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("arguments"));
                PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("name"));
            }
        }
    }

    public static class Builder {
        private Builder() {}

        public Builder setRole(String role) {
            return this;
        }

    }

    private static void moveToFirstToken(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
    }

}
