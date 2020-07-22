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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * PreProcessor for target mean encoding a set of categorical values for a given field.
 */
public class TargetMeanEncoding implements LenientlyParsedPreProcessor, StrictlyParsedPreProcessor {

    public static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TargetMeanEncoding.class);
    public static final ParseField NAME = new ParseField("target_mean_encoding");
    public static final ParseField FIELD = new ParseField("field");
    public static final ParseField FEATURE_NAME = new ParseField("feature_name");
    public static final ParseField TARGET_MAP = new ParseField("target_map");
    public static final ParseField DEFAULT_VALUE = new ParseField("default_value");
    public static final ParseField CUSTOM = new ParseField("custom");

    public static final ConstructingObjectParser<TargetMeanEncoding, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<TargetMeanEncoding, Void> LENIENT_PARSER = createParser(true);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<TargetMeanEncoding, Void> createParser(boolean lenient) {
        ConstructingObjectParser<TargetMeanEncoding, Void> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            a -> new TargetMeanEncoding((String)a[0], (String)a[1], (Map<String, Double>)a[2], (Double)a[3], (Boolean)a[4]));
        parser.declareString(ConstructingObjectParser.constructorArg(), FIELD);
        parser.declareString(ConstructingObjectParser.constructorArg(), FEATURE_NAME);
        parser.declareObject(ConstructingObjectParser.constructorArg(),
            (p, c) -> p.map(HashMap::new, XContentParser::doubleValue),
            TARGET_MAP);
        parser.declareDouble(ConstructingObjectParser.constructorArg(), DEFAULT_VALUE);
        parser.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), CUSTOM);
        return parser;
    }

    public static TargetMeanEncoding fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null);
    }

    public static TargetMeanEncoding fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null);
    }

    private final String field;
    private final String featureName;
    private final Map<String, Double> meanMap;
    private final double defaultValue;
    private final boolean custom;

    public TargetMeanEncoding(String field, String featureName, Map<String, Double> meanMap, Double defaultValue, Boolean custom) {
        this.field = ExceptionsHelper.requireNonNull(field, FIELD);
        this.featureName = ExceptionsHelper.requireNonNull(featureName, FEATURE_NAME);
        this.meanMap = Collections.unmodifiableMap(ExceptionsHelper.requireNonNull(meanMap, TARGET_MAP));
        this.defaultValue = ExceptionsHelper.requireNonNull(defaultValue, DEFAULT_VALUE);
        this.custom = custom == null ? false : custom;
    }

    public TargetMeanEncoding(StreamInput in) throws IOException {
        this.field = in.readString();
        this.featureName = in.readString();
        this.meanMap = Collections.unmodifiableMap(in.readMap(StreamInput::readString, StreamInput::readDouble));
        this.defaultValue = in.readDouble();
        this.custom = in.readBoolean();
    }

    /**
     * @return Field name on which to target mean encode
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

    /**
     * @return The feature name for the encoded value
     */
    public String getFeatureName() {
        return featureName;
    }

    @Override
    public Map<String, String> reverseLookup() {
        return Collections.singletonMap(featureName, field);
    }

    @Override
    public boolean isCustom() {
        return custom;
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public List<String> inputFields() {
        return Collections.singletonList(field);
    }

    @Override
    public List<String> outputFields() {
        return Collections.singletonList(featureName);
    }

    @Override
    public void process(Map<String, Object> fields) {
        Object value = fields.get(field);
        if (value == null) {
            return;
        }
        fields.put(featureName, meanMap.getOrDefault(value.toString(), defaultValue));
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeString(featureName);
        out.writeMap(meanMap, StreamOutput::writeString, StreamOutput::writeDouble);
        out.writeDouble(defaultValue);
        out.writeBoolean(custom);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD.getPreferredName(), field);
        builder.field(FEATURE_NAME.getPreferredName(), featureName);
        builder.field(TARGET_MAP.getPreferredName(), meanMap);
        builder.field(DEFAULT_VALUE.getPreferredName(), defaultValue);
        builder.field(CUSTOM.getPreferredName(), custom);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TargetMeanEncoding that = (TargetMeanEncoding) o;
        return Objects.equals(field, that.field)
            && Objects.equals(featureName, that.featureName)
            && Objects.equals(meanMap, that.meanMap)
            && Objects.equals(defaultValue, that.defaultValue)
            && Objects.equals(custom, that.custom);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, featureName, meanMap, defaultValue, custom);
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += RamUsageEstimator.sizeOf(field);
        size += RamUsageEstimator.sizeOf(featureName);
        // defSize:0 indicates that there is not a defined size. Finding the shallowSize of Double gives the best estimate
        size += RamUsageEstimator.sizeOfMap(meanMap, 0);
        return size;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
