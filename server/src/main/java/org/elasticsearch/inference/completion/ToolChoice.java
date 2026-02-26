/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.completion;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.FUNCTION_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.NAME_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.TOOL_CHOICE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.TYPE_FIELD;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public sealed interface ToolChoice extends NamedWriteable, ToXContent permits ToolChoice.ToolChoiceObject, ToolChoice.ToolChoiceString {

    record ToolChoiceObject(String type, ToolChoiceObject.FunctionField function) implements ToolChoice, NamedWriteable {

        public static final String NAME = "tool_choice_object";

        public static final ConstructingObjectParser<ToolChoiceObject, Void> PARSER = new ConstructingObjectParser<>(
            ToolChoiceObject.class.getSimpleName(),
            args -> new ToolChoiceObject((String) args[0], (ToolChoiceObject.FunctionField) args[1])
        );

        static {
            PARSER.declareString(constructorArg(), new ParseField("type"));
            PARSER.declareObject(constructorArg(), ToolChoiceObject.FunctionField.PARSER::apply, new ParseField("function"));
        }

        public ToolChoiceObject(StreamInput in) throws IOException {
            this(in.readString(), new ToolChoiceObject.FunctionField(in));
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

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(TOOL_CHOICE_FIELD);
            builder.field(TYPE_FIELD, type);
            builder.field(FUNCTION_FIELD, function);
            return builder.endObject();
        }

        public record FunctionField(String name) implements Writeable, ToXContentObject {
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

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.startObject().field(NAME_FIELD, name).endObject();
            }
        }
    }

    record ToolChoiceString(String value) implements ToolChoice, NamedWriteable {
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

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(TOOL_CHOICE_FIELD, value);
        }
    }
}
