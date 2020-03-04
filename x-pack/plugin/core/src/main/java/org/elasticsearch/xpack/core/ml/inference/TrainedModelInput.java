/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


public class TrainedModelInput implements ToXContentObject, Writeable {

    public static final String NAME = "trained_model_config_input";
    public static final ParseField FIELD_NAMES = new ParseField("field_names");

    public static final ConstructingObjectParser<TrainedModelInput, Void> LENIENT_PARSER = createParser(true);
    public static final ConstructingObjectParser<TrainedModelInput, Void> STRICT_PARSER = createParser(false);

    public static TrainedModelInput fromFieldsAndDefinition(List<String> fieldNames, TrainedModelDefinition definition) {
        List<InputObject> inputObjects = new ArrayList<>(fieldNames.size());
        fieldNames.forEach(fieldName -> {
            ModelFieldType fieldType = definition.inputType(fieldName);
            if (fieldType != null) {
                inputObjects.add(new InputObject(fieldName, fieldType.toString()));
            }
        });
        return new TrainedModelInput(inputObjects);
    }

    private final List<TrainedModelInput.InputObject> fields;
    public TrainedModelInput(List<InputObject> fieldNames) {
        this.fields = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(fieldNames, FIELD_NAMES));
    }

    public TrainedModelInput(StreamInput in) throws IOException {
        this.fields = Collections.unmodifiableList(in.readList(InputObject::new));
    }

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<TrainedModelInput, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<TrainedModelInput, Void> parser = new ConstructingObjectParser<>(NAME,
            ignoreUnknownFields,
            a -> new TrainedModelInput((List<InputObject>) a[0]));
        parser.declareFieldArray(ConstructingObjectParser.constructorArg(),
            (p, c) -> {
                if (p.currentToken().isValue()) {
                    return new InputObject(p.text());
                } else {
                    return InputObject.fromXContent(p, ignoreUnknownFields);
                }
            },
            FIELD_NAMES,
            ObjectParser.ValueType.OBJECT_ARRAY_OR_STRING);
        return parser;
    }

    public static TrainedModelInput fromXContent(XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.parse(parser, null) : STRICT_PARSER.parse(parser, null);
    }

    public List<InputObject> getFieldNames() {
        return fields;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(fields);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_NAMES.getPreferredName(), fields);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrainedModelInput that = (TrainedModelInput) o;
        return Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }

    public static class InputObject implements ToXContentObject, Writeable {

        public static final ParseField FIELD_NAME = new ParseField("field_name");
        public static final ParseField FIELD_TYPE = new ParseField("field_type");

        public static final ConstructingObjectParser<InputObject, Void> LENIENT_PARSER = createParser(true);
        public static final ConstructingObjectParser<InputObject, Void> STRICT_PARSER = createParser(false);

        private static ConstructingObjectParser<InputObject, Void> createParser(boolean ignoreUnknownFields) {
            ConstructingObjectParser<InputObject, Void> parser = new ConstructingObjectParser<>("trained_model_config_input_object",
                ignoreUnknownFields,
                a -> new InputObject((String) a[0], (String) a[1]));
            parser.declareString(ConstructingObjectParser.constructorArg(), FIELD_NAME);
            parser.declareString(ConstructingObjectParser.optionalConstructorArg(), FIELD_TYPE);
            return parser;
        }

        public static InputObject fromXContent(XContentParser parser, boolean lenient) throws IOException {
            return lenient ? LENIENT_PARSER.parse(parser, null) : STRICT_PARSER.parse(parser, null);
        }

        private final String fieldName;
        private final String fieldType;

        public InputObject(String fieldName) {
            this(fieldName, null);
        }

        public InputObject(String fieldName, String fieldType) {
            this.fieldName = ExceptionsHelper.requireNonNull(fieldName, FIELD_NAME);
            this.fieldType = fieldType;
        }

        public InputObject(StreamInput in) throws IOException {
            this.fieldName = in.readString();
            if (in.getVersion().onOrAfter(Version.V_7_7_0)) {
                this.fieldType = in.readOptionalString();
            } else {
                this.fieldType = null;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, fieldType);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TrainedModelInput.InputObject that = (TrainedModelInput.InputObject) o;
            return Objects.equals(fieldName, that.fieldName)
                && Objects.equals(fieldType, that.fieldType);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(fieldName);
            if (out.getVersion().onOrAfter(Version.V_7_7_0)) {
                out.writeOptionalString(fieldType);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(FIELD_NAME.getPreferredName(), fieldName);
            if (fieldType != null) {
                builder.field(FIELD_TYPE.getPreferredName(), fieldType);
            }
            builder.endObject();
            return builder;
        }

        public String getFieldName() {
            return fieldName;
        }

        public String getFieldType() {
            return fieldType;
        }

        public ModelFieldType getModelFieldType() {
            return fieldType == null ? null : ModelFieldType.fromString(fieldName);
        }
    }
}
