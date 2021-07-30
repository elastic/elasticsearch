/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class TrainedModelInput implements ToXContentObject {

    public static final String NAME = "trained_model_config_input";
    public static final ParseField FIELD_NAMES = new ParseField("field_names");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<TrainedModelInput, Void> PARSER = new ConstructingObjectParser<>(NAME,
        true,
        a -> new TrainedModelInput((List<String>) a[0]));

    static {
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), FIELD_NAMES);
    }

    private final List<String> fieldNames;

    public TrainedModelInput(List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    public TrainedModelInput(String... fieldNames) {
        this(Arrays.asList(fieldNames));
    }

    public static TrainedModelInput fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public List<String> getFieldNames() {
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

}
