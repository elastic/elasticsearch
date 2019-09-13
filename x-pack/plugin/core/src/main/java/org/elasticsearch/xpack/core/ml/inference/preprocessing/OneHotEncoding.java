/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * PreProcessor for one hot encoding a set of categorical values for a given field.
 */
public class OneHotEncoding implements PreProcessor {

    public static final ParseField NAME = new ParseField("one_hot_encoding");
    public static final ParseField FIELD = new ParseField("field");
    public static final ParseField VALUE_MAP = new ParseField("value_map");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<OneHotEncoding, Void> PARSER = new ConstructingObjectParser<>(
        NAME.getPreferredName(),
        a -> new OneHotEncoding((String)a[0], (Map<String, String>)a[1]));
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.mapStrings(), VALUE_MAP);
    }

    public static OneHotEncoding fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String field;
    private final Map<String, String> valueMap;

    public OneHotEncoding(String field, Map<String, String> valueMap) {
        this.field = ExceptionsHelper.requireNonNull(field, FIELD);
        this.valueMap = Collections.unmodifiableMap(ExceptionsHelper.requireNonNull(valueMap, VALUE_MAP));
    }

    public OneHotEncoding(StreamInput in) throws IOException {
        this.field = in.readString();
        this.valueMap = Collections.unmodifiableMap(in.readMap(StreamInput::readString, StreamInput::readString));
    }

    /**
     * @return Field name on which to one hot encode
     */
    public String getField() {
        return field;
    }

    /**
     * @return Map of Value: ColumnName for the one hot encoding
     */
    public Map<String, String> getValueMap() {
        return valueMap;
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public Map<String, Object> process(Map<String, Object> fields) {
        String value = (String)fields.get(field);
        if (value == null) {
            return fields;
        }
        valueMap.forEach((val, col) -> {
            int encoding = value.equals(val) ? 1 : 0;
            fields.put(col, encoding);
        });
        return fields;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeMap(valueMap, StreamOutput::writeString, StreamOutput::writeString);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD.getPreferredName(), field);
        builder.field(VALUE_MAP.getPreferredName(), valueMap);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OneHotEncoding that = (OneHotEncoding) o;
        return Objects.equals(field, that.field)
            && Objects.equals(valueMap, that.valueMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, valueMap);
    }

}
