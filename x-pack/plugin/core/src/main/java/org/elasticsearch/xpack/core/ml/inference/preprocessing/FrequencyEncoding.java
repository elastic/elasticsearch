/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MapHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


/**
 * PreProcessor for frequency encoding a set of categorical values for a given field.
 */
public class FrequencyEncoding implements LenientlyParsedPreProcessor, StrictlyParsedPreProcessor {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(FrequencyEncoding.class);

    public static final ParseField NAME = new ParseField("frequency_encoding");
    public static final ParseField FIELD = new ParseField("field");
    public static final ParseField FEATURE_NAME = new ParseField("feature_name");
    public static final ParseField FREQUENCY_MAP = new ParseField("frequency_map");

    public static final ConstructingObjectParser<FrequencyEncoding, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<FrequencyEncoding, Void> LENIENT_PARSER = createParser(true);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<FrequencyEncoding, Void> createParser(boolean lenient) {
        ConstructingObjectParser<FrequencyEncoding, Void> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            a -> new FrequencyEncoding((String)a[0], (String)a[1], (Map<String, Double>)a[2]));
        parser.declareString(ConstructingObjectParser.constructorArg(), FIELD);
        parser.declareString(ConstructingObjectParser.constructorArg(), FEATURE_NAME);
        parser.declareObject(ConstructingObjectParser.constructorArg(),
            (p, c) -> p.map(HashMap::new, XContentParser::doubleValue),
            FREQUENCY_MAP);
        return parser;
    }

    public static FrequencyEncoding fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null);
    }

    public static FrequencyEncoding fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null);
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
     * @return Field name on which to frequency encode
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
    public void process(Map<String, Object> fields) {
        Object value = MapHelper.dig(field, fields);
        if (value == null) {
            return;
        }
        fields.put(featureName, frequencyMap.getOrDefault(value.toString(), 0.0));
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

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += RamUsageEstimator.sizeOf(field);
        size += RamUsageEstimator.sizeOf(featureName);
        // defSize:0 indicates that there is not a defined size. Finding the shallowSize of Double gives the best estimate
        size += RamUsageEstimator.sizeOfMap(frequencyMap, 0);
        return size;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
