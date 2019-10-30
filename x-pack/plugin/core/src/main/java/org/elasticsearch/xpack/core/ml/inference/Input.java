/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


public class Input implements ToXContentObject, Writeable {

    public static final String NAME = "trained_model_config_input";
    public static final ParseField FIELD_NAMES = new ParseField("field_names");

    public static final ConstructingObjectParser<Input, Void> LENIENT_PARSER = createParser(true);
    public static final ConstructingObjectParser<Input, Void> STRICT_PARSER = createParser(false);
    private final List<String> fieldNames;

    public Input(List<String> fieldNames) {
        this.fieldNames = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(fieldNames, FIELD_NAMES));
    }

    public Input(StreamInput in) throws IOException {
        this.fieldNames = Collections.unmodifiableList(in.readStringList());
    }

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<Input, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<Input, Void> parser = new ConstructingObjectParser<>(NAME,
            ignoreUnknownFields,
            a -> new Input((List<String>) a[0]));
        parser.declareStringArray(ConstructingObjectParser.constructorArg(), FIELD_NAMES);
        return parser;
    }

    public static Input fromXContent(XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.parse(parser, null) : STRICT_PARSER.parse(parser, null);
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(fieldNames);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_NAMES.getPreferredName(), fieldNames);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Input that = (Input) o;
        return Objects.equals(fieldNames, that.fieldNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldNames);
    }

}
