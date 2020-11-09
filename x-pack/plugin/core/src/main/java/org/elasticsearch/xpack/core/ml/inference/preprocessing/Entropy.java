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
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.lucene.util.RamUsageEstimator.sizeOf;

/**
 * PreProcessor for n-gram encoding a string
 */
public class Entropy implements LenientlyParsedPreProcessor, StrictlyParsedPreProcessor {

    // Maximum optimization array size
    // Maybe could be dynamically determined based on underlying architecture
    private static final int OPTIMIZATION_SIZE = 127;

    public static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Entropy.class);
    public static final ParseField NAME = new ParseField("entropy_encoding");
    public static final ParseField FIELD = new ParseField("field");
    public static final ParseField FEATURE_NAME = new ParseField("feature_name");
    public static final ParseField CUSTOM = new ParseField("custom");

    private static final ConstructingObjectParser<Entropy, PreProcessorParseContext> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<Entropy, PreProcessorParseContext> LENIENT_PARSER = createParser(true);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<Entropy, PreProcessorParseContext> createParser(boolean lenient) {
        ConstructingObjectParser<Entropy, PreProcessorParseContext> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            (a, c) -> new Entropy((String)a[0],
                (String)a[1],
                a[2] == null ? c.isCustomByDefault() : (Boolean)a[2]));
        parser.declareString(ConstructingObjectParser.constructorArg(), FIELD);
        parser.declareString(ConstructingObjectParser.constructorArg(), FEATURE_NAME);
        parser.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), CUSTOM);
        return parser;
    }

    public static Entropy fromXContentStrict(XContentParser parser, PreProcessorParseContext context) {
        return STRICT_PARSER.apply(parser, context == null ?  PreProcessorParseContext.DEFAULT : context);
    }

    public static Entropy fromXContentLenient(XContentParser parser, PreProcessorParseContext context) {
        return LENIENT_PARSER.apply(parser, context == null ?  PreProcessorParseContext.DEFAULT : context);
    }

    private final String field;
    private final String featureName;
    private final boolean custom;

    public Entropy(String field, String featureName, boolean custom) {
        this.field = ExceptionsHelper.requireNonNull(field, FIELD);
        this.featureName = ExceptionsHelper.requireNonNull(featureName, FEATURE_NAME);
        this.custom = custom;
    }

    public Entropy(StreamInput in) throws IOException {
        this.field = in.readString();
        this.featureName = in.readString();
        this.custom = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeString(featureName);
        out.writeBoolean(custom);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
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
        // TODO should we support an array input for entropy of entries?
        // TODO should entropy also optionally include length as a feature?
        final String stringValue = value.toString();
        double entropy = entropyCalc(stringValue);
        fields.put(featureName, entropy);
    }

    double entropyCalc(String stringValue) {
        final double len = stringValue.length();
        // This provides a nice optimization for throughput.
        // The entire basic latin code block fits is within 0x0000..0x007F.
        // So, this provides a good optimization for those types of characters
        // We still have a sparse hashmap for the rest.
        int[] vals = new int[OPTIMIZATION_SIZE];
        Map<Integer, Integer> biggerCounts = new HashMap<>();
        for (PrimitiveIterator.OfInt it = stringValue.codePoints().iterator(); it.hasNext(); ) {
            int cp = it.next();
            if (cp < OPTIMIZATION_SIZE) {
                vals[cp]++;
            } else {
                biggerCounts.compute(cp, (_k, v) -> v == null ? 1 : v + 1);
            }
        }

        double entropy = 0.0;
        for (int count : vals) {
            if (count > 0) {
                double probability = count / len;
                entropy -= probability * (Math.log(probability) / Math.log(2));
            }
        }
        for (Integer count : biggerCounts.values()) {
            double probability = count / len;
            entropy -= probability * (Math.log(probability) / Math.log(2));
        }
        return entropy;
    }

    @Override
    public Map<String, String> reverseLookup() {
        return outputFields().stream().collect(Collectors.toMap(Function.identity(), ignored -> field));
    }

    @Override
    public String getOutputFieldType(String outputField) {
        return NumberFieldMapper.NumberType.DOUBLE.typeName();
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += sizeOf(field);
        size += sizeOf(featureName);
        return size;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD.getPreferredName(), field);
        builder.field(FEATURE_NAME.getPreferredName(), featureName);
        builder.field(CUSTOM.getPreferredName(), custom);
        builder.endObject();
        return builder;
    }

    public String getField() {
        return field;
    }

    public String getFeatureName() {
        return featureName;
    }

    @Override
    public boolean isCustom() {
        return custom;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Entropy entropy = (Entropy) o;
        return custom == entropy.custom &&
            Objects.equals(field, entropy.field) &&
            Objects.equals(featureName, entropy.featureName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, featureName, custom);
    }

}
