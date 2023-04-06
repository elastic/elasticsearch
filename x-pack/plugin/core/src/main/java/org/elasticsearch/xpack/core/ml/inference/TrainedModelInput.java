/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TrainedModelInput implements ToXContentObject, Writeable {

    public static final String NAME = "trained_model_config_input";
    public static final ParseField FIELD_NAMES = new ParseField("field_names");

    public static final ConstructingObjectParser<TrainedModelInput, Void> LENIENT_PARSER = createParser(true);
    public static final ConstructingObjectParser<TrainedModelInput, Void> STRICT_PARSER = createParser(false);
    private final List<String> fieldNames;

    public TrainedModelInput(List<String> fieldNames) {
        this.fieldNames = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(fieldNames, FIELD_NAMES));
    }

    public TrainedModelInput(StreamInput in) throws IOException {
        this.fieldNames = in.readImmutableList(StreamInput::readString);
    }

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<TrainedModelInput, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<TrainedModelInput, Void> parser = new ConstructingObjectParser<>(
            NAME,
            ignoreUnknownFields,
            a -> new TrainedModelInput((List<String>) a[0])
        );
        parser.declareStringArray(ConstructingObjectParser.constructorArg(), FIELD_NAMES);
        return parser;
    }

    public static TrainedModelInput fromXContent(XContentParser parser, boolean lenient) throws IOException {
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
        TrainedModelInput that = (TrainedModelInput) o;
        return Objects.equals(fieldNames, that.fieldNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldNames);
    }

}
