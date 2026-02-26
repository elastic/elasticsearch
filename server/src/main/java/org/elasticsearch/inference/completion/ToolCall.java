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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.ARGUMENTS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.FUNCTION_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.ID_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.NAME_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.TYPE_FIELD;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public record ToolCall(String id, FunctionField function, String type) implements Writeable, ToXContentObject {

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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID_FIELD, id);
        builder.field(FUNCTION_FIELD, function);
        builder.field(TYPE_FIELD, type);
        return builder.endObject();
    }

    public record FunctionField(String arguments, String name) implements Writeable, ToXContentObject {
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

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(ARGUMENTS_FIELD, arguments);
            builder.field(NAME_FIELD, name);
            return builder.endObject();
        }
    }
}
