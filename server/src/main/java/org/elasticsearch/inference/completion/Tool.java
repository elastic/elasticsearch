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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.DESCRIPTION_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.FUNCTION_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.NAME_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.PARAMETERS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.STRICT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.TYPE_FIELD;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public record Tool(String type, FunctionField function) implements Writeable, ToXContentObject {

    public static final ConstructingObjectParser<Tool, Void> PARSER = new ConstructingObjectParser<>(
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(TYPE_FIELD, type);
        builder.field(FUNCTION_FIELD, function);

        return builder.endObject();
    }

    public record FunctionField(
        @Nullable String description,
        String name,
        @Nullable Map<String, Object> parameters,
        @Nullable Boolean strict
    ) implements Writeable, ToXContentObject {

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

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(DESCRIPTION_FIELD, description);
            builder.field(NAME_FIELD, name);
            builder.field(PARAMETERS_FIELD, parameters);
            if (strict != null) {
                builder.field(STRICT_FIELD, strict);
            }
            return builder.endObject();
        }
    }
}
