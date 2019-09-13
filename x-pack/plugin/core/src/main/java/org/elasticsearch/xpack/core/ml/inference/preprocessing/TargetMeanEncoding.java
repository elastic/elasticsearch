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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


/**
 * PreProcessor for target mean encoding a set of categorical values for a given field.
 */
public class TargetMeanEncoding implements PreProcessor {

    public static final ParseField NAME = new ParseField("target_mean_encoding");
    public static final ParseField FIELD = new ParseField("field");
    public static final ParseField TARGET_MEANS = new ParseField("target_means");
    public static final ParseField DEFAULT_VALUE = new ParseField("default_value");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<TargetMeanEncoding, Void> PARSER = new ConstructingObjectParser<>(
        NAME.getPreferredName(),
        a -> new TargetMeanEncoding((String)a[0], (Map<String, Double>)a[1], (Double)a[2]));
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(),
            (p, c) -> p.map(HashMap::new, XContentParser::doubleValue),
            TARGET_MEANS);
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), DEFAULT_VALUE);
    }

    public static TargetMeanEncoding fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String field;
    private final Map<String, Double> meanMap;
    private final double defaultValue;

    public TargetMeanEncoding(String field, Map<String, Double> meanMap, Double defaultValue) {
        this.field = ExceptionsHelper.requireNonNull(field, FIELD);
        this.meanMap = Collections.unmodifiableMap(ExceptionsHelper.requireNonNull(meanMap, TARGET_MEANS));
        this.defaultValue = ExceptionsHelper.requireNonNull(defaultValue, DEFAULT_VALUE);
    }

    public TargetMeanEncoding(StreamInput in) throws IOException {
        this.field = in.readString();
        this.meanMap = Collections.unmodifiableMap(in.readMap(StreamInput::readString, StreamInput::readDouble));
        this.defaultValue = in.readDouble();
    }

    /**
     * @return Field name on which to one hot encode
     */
    public String getField() {
        return field;
    }

    /**
     * @return Map of Value: targetMean for the target mean encoding
     */
    public Map<String, Double> getMeanMap() {
        return meanMap;
    }

    /**
     * @return The default value to set when a previously unobserved value is seen
     */
    public Double getDefaultValue() {
        return defaultValue;
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
        fields.put(field, meanMap.getOrDefault(value, defaultValue));
        return fields;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeMap(meanMap, StreamOutput::writeString, StreamOutput::writeDouble);
        out.writeDouble(defaultValue);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD.getPreferredName(), field);
        builder.field(TARGET_MEANS.getPreferredName(), meanMap);
        builder.field(DEFAULT_VALUE.getPreferredName(), defaultValue);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TargetMeanEncoding that = (TargetMeanEncoding) o;
        return Objects.equals(field, that.field)
            && Objects.equals(meanMap, that.meanMap)
            && Objects.equals(defaultValue, that.defaultValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, meanMap, defaultValue);
    }

}
