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
 * PreProcessor for frequency encoding a set of categorical values for a given field.
 */
public class FrequencyEncoding implements PreProcessor {

    public static final ParseField NAME = new ParseField("frequency_encoding");
    public static final ParseField FIELD = new ParseField("field");
    public static final ParseField FEATURE_NAME = new ParseField("feature_name");
    public static final ParseField FREQUENCY_MAP = new ParseField("frequency_map");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<FrequencyEncoding, Void> PARSER = new ConstructingObjectParser<>(
        NAME.getPreferredName(),
        a -> new FrequencyEncoding((String)a[0], (String)a[1], (Map<String, Double>)a[2]));
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FEATURE_NAME);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(),
            (p, c) -> p.map(HashMap::new, XContentParser::doubleValue),
            FREQUENCY_MAP);
    }

    public static FrequencyEncoding fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String field;
    private final String featureName;
    private final Map<String, Double> frequencyMap;

    public FrequencyEncoding(String field, String featureName, Map<String, Double> frequencyMap) {
        this.field = ExceptionsHelper.requireNonNull(field, FIELD);
        this.featureName = ExceptionsHelper.requireNonNull(featureName, FEATURE_NAME);
        this.frequencyMap = Collections.unmodifiableMap(ExceptionsHelper.requireNonNull(frequencyMap, FREQUENCY_MAP));
    }

    public FrequencyEncoding(StreamInput in) throws IOException {
        this.field = in.readString();
        this.featureName = in.readString();
        this.frequencyMap = Collections.unmodifiableMap(in.readMap(StreamInput::readString, StreamInput::readDouble));
    }

    /**
     * @return Field name on which to one hot encode
     */
    public String getField() {
        return field;
    }

    /**
     * @return Map of Value: frequency for the frequency encoding
     */
    public Map<String, Double> getFrequencyMap() {
        return frequencyMap;
    }

    /**
     * @return The encoded feature name
     */
    public String getFeatureName() {
        return featureName;
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
        fields.put(featureName, frequencyMap.getOrDefault(value, 0.0));
        return fields;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeString(featureName);
        out.writeMap(frequencyMap, StreamOutput::writeString, StreamOutput::writeDouble);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD.getPreferredName(), field);
        builder.field(FEATURE_NAME.getPreferredName(), featureName);
        builder.field(FREQUENCY_MAP.getPreferredName(), frequencyMap);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FrequencyEncoding that = (FrequencyEncoding) o;
        return Objects.equals(field, that.field)
            && Objects.equals(featureName, that.featureName)
            && Objects.equals(frequencyMap, that.frequencyMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, featureName, frequencyMap);
    }

}
