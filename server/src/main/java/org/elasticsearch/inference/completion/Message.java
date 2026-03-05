/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.completion;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.ROLE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.TOOL_CALLS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.TOOL_CALL_ID_FIELD;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public record Message(Content content, String role, @Nullable String toolCallId, @Nullable List<ToolCall> toolCalls)
    implements
        Writeable,
        ToXContentObject {

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<Message, Void> PARSER = new ConstructingObjectParser<>(
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
            var parsedContentObjects = XContentParserUtils.parseList(parser, (p) -> ContentObject.fromMap(p.map()));
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (content != null) {
            content.toXContent(builder, params);
        }
        builder.field(ROLE_FIELD, role);
        if (toolCallId != null) {
            builder.field(TOOL_CALL_ID_FIELD, toolCallId);
        }
        if (toolCalls != null) {
            builder.field(TOOL_CALLS_FIELD, toolCalls);
        }

        return builder.endObject();
    }
}
