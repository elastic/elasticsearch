/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.modelsize;

import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.FrequencyEncoding;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.lucene.util.RamUsageEstimator.alignObjectSize;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.ml.inference.modelsize.SizeEstimatorHelper.sizeOfHashMap;
import static org.elasticsearch.xpack.ml.inference.modelsize.SizeEstimatorHelper.sizeOfString;

public class FrequencyEncodingSize implements PreprocessorSize {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<FrequencyEncodingSize, Void> PARSER = new ConstructingObjectParser<>(
        "frequency_encoding_size",
        false,
        a -> new FrequencyEncodingSize((Integer)a[0], (Integer)a[1], (List<Integer>)a[2])
    );
    static {
        PARSER.declareInt(constructorArg(), FIELD_LENGTH);
        PARSER.declareInt(constructorArg(), FEATURE_NAME_LENGTH);
        PARSER.declareIntArray(constructorArg(), FIELD_VALUE_LENGTHS);
    }

    public static FrequencyEncodingSize fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final int fieldLength;
    private final int featureNameLength;
    private final int[] fieldValueLengths;

    FrequencyEncodingSize(int fieldLength, int featureNameLength, List<Integer> fieldValueLengths) {
        this.fieldLength = fieldLength;
        this.featureNameLength = featureNameLength;
        this.fieldValueLengths = fieldValueLengths.stream().mapToInt(Integer::intValue).toArray();
    }

    @Override
    public long ramBytesUsed() {
        final long sizeOfDoubleObject = shallowSizeOfInstance(Double.class);
        long size = FrequencyEncoding.SHALLOW_SIZE;
        size += sizeOfString(fieldLength);
        size += sizeOfString(featureNameLength);
        size += sizeOfHashMap(
            Arrays.stream(fieldValueLengths).mapToLong(SizeEstimatorHelper::sizeOfString).boxed().collect(Collectors.toList()),
            Stream.generate(() -> sizeOfDoubleObject).limit(fieldValueLengths.length).collect(Collectors.toList()));
        return alignObjectSize(size);
    }

    @Override
    public String getName() {
        return FrequencyEncoding.NAME.getPreferredName();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_LENGTH.getPreferredName(), fieldLength);
        builder.field(FEATURE_NAME_LENGTH.getPreferredName(), featureNameLength);
        builder.field(FIELD_VALUE_LENGTHS.getPreferredName(), fieldValueLengths);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FrequencyEncodingSize that = (FrequencyEncodingSize) o;
        return fieldLength == that.fieldLength &&
            featureNameLength == that.featureNameLength &&
            Arrays.equals(fieldValueLengths, that.fieldValueLengths);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(fieldLength, featureNameLength);
        result = 31 * result + Arrays.hashCode(fieldValueLengths);
        return result;
    }
}
