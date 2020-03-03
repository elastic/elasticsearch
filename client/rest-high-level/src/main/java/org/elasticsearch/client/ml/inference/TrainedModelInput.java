/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.inference;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TrainedModelInput implements ToXContentObject {

    public static final String NAME = "trained_model_config_input";
    public static final ParseField FIELD_NAMES = new ParseField("field_names");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<TrainedModelInput, Void> PARSER = new ConstructingObjectParser<>(NAME,
        true,
        a -> new TrainedModelInput((List<InputObject>) a[0]));

    static {
        PARSER.declareFieldArray(ConstructingObjectParser.constructorArg(),
            (p, c) -> {
                if (p.currentToken().isValue()) {
                    return new InputObject(p.text());
                } else {
                    return InputObject.fromXContent(p);
                }
            },
            FIELD_NAMES,
            ObjectParser.ValueType.OBJECT_ARRAY_OR_STRING);
    }

    private final List<InputObject> fieldNames;

    public TrainedModelInput(String... fieldNames) {
        this(Arrays.stream(fieldNames).map(InputObject::new).collect(Collectors.toList()));
    }

    TrainedModelInput(List<InputObject> objects) {
        this.fieldNames = objects;
    }

    public static TrainedModelInput fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * @return The list of field names and their types. Stored in {@link InputObject}.
     */
    public List<InputObject> getFieldNames() {
        return fieldNames;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (fieldNames != null) {
            builder.field(FIELD_NAMES.getPreferredName(), fieldNames);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrainedModelInput that = (TrainedModelInput) o;
        return Objects.equals(fieldNames, that.fieldNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldNames);
    }

    public static class InputObject implements ToXContentObject {

        public static final ParseField FIELD_NAME = new ParseField("field_name");
        public static final ParseField FIELD_TYPE = new ParseField("field_type");

        public static final ConstructingObjectParser<InputObject, Void> PARSER = new ConstructingObjectParser<>(
            "trained_model_config_input_object",
            true,
            a -> new InputObject((String) a[0], (String) a[1]));

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD_NAME);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), FIELD_TYPE);
        }

        public static InputObject fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        private final String fieldName;
        private final String fieldType;

        public InputObject(String fieldName) {
            this(fieldName, null);
        }

        public InputObject(String fieldName, String fieldType) {
            this.fieldName = fieldName;
            this.fieldType = fieldType;
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
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(FIELD_NAME.getPreferredName(), fieldName);
            if (fieldType != null) {
                builder.field(FIELD_TYPE.getPreferredName(), fieldType);
            }
            builder.endObject();
            return builder;
        }

        /**
         * @return The field name
         */
        public String getFieldName() {
            return fieldName;
        }

        /**
         * @return The field type. Could be one of [scalar, vector, text, categorical].
         */
        public String getFieldType() {
            return fieldType;
        }
    }
}
