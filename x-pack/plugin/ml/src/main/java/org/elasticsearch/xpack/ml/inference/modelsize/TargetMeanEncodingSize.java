/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.modelsize;

import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.TargetMeanEncoding;

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

public class TargetMeanEncodingSize implements PreprocessorSize {
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<TargetMeanEncodingSize, Void> PARSER = new ConstructingObjectParser<>(
        "target_mean_encoding_size",
        false,
        a -> new TargetMeanEncodingSize((Integer)a[0], (Integer)a[1], (List<Integer>)a[2])
    );
    static {
        PARSER.declareInt(constructorArg(), FIELD_LENGTH);
        PARSER.declareInt(constructorArg(), FEATURE_NAME_LENGTH);
        PARSER.declareIntArray(constructorArg(), FIELD_VALUE_LENGTHS);
    }

    public static TargetMeanEncodingSize fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final int fieldLength;
    private final int featureNameLength;
    private final int[] fieldValueLengths;

    TargetMeanEncodingSize(int fieldLength, int featureNameLength, List<Integer> fieldValueLengths) {
        this.fieldLength = fieldLength;
        this.featureNameLength = featureNameLength;
        this.fieldValueLengths = fieldValueLengths.stream().mapToInt(Integer::intValue).toArray();
    }

    @Override
    public long ramBytesUsed() {
        final long sizeOfDoubleObject = shallowSizeOfInstance(Double.class);
        long size = TargetMeanEncoding.SHALLOW_SIZE;
        size += sizeOfString(fieldLength);
        size += sizeOfString(featureNameLength);
        size += sizeOfHashMap(
            Arrays.stream(fieldValueLengths).mapToLong(SizeEstimatorHelper::sizeOfString).boxed().collect(Collectors.toList()),
            Stream.generate(() -> sizeOfDoubleObject).limit(fieldValueLengths.length).collect(Collectors.toList())
        );
        return alignObjectSize(size);
    }

    @Override
    public String getName() {
        return TargetMeanEncoding.NAME.getPreferredName();
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
        TargetMeanEncodingSize that = (TargetMeanEncodingSize) o;
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
