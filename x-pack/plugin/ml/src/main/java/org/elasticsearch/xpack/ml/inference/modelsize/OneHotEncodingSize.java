/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.modelsize;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncoding;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.lucene.util.RamUsageEstimator.alignObjectSize;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.ml.inference.modelsize.SizeEstimatorHelper.sizeOfHashMap;
import static org.elasticsearch.xpack.ml.inference.modelsize.SizeEstimatorHelper.sizeOfString;

public class OneHotEncodingSize implements PreprocessorSize {

    private static final ParseField FEATURE_NAME_LENGTHS = new ParseField("feature_name_lengths");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<OneHotEncodingSize, Void> PARSER = new ConstructingObjectParser<>(
        "one_hot_encoding_size",
        false,
        a -> new OneHotEncodingSize((Integer)a[0], (List<Integer>)a[1], (List<Integer>)a[2])
    );
    static {
        PARSER.declareInt(constructorArg(), FIELD_LENGTH);
        PARSER.declareIntArray(constructorArg(), FEATURE_NAME_LENGTHS);
        PARSER.declareIntArray(constructorArg(), FIELD_VALUE_LENGTHS);
    }

    public static OneHotEncodingSize fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final int fieldLength;
    private final int[] featureNameLengths;
    private final int[] fieldValueLengths;

    OneHotEncodingSize(int fieldLength, List<Integer> featureNameLengths, List<Integer> fieldValueLengths) {
        assert featureNameLengths.size() == fieldValueLengths.size();
        this.fieldLength = fieldLength;
        this.featureNameLengths = featureNameLengths.stream().mapToInt(Integer::intValue).toArray();
        this.fieldValueLengths = fieldValueLengths.stream().mapToInt(Integer::intValue).toArray();
    }

    @Override
    public long ramBytesUsed() {
        long size = OneHotEncoding.SHALLOW_SIZE;
        size += sizeOfString(fieldLength);
        size += sizeOfHashMap(
            Arrays.stream(fieldValueLengths).mapToLong(SizeEstimatorHelper::sizeOfString).boxed().collect(Collectors.toList()),
            Arrays.stream(featureNameLengths).mapToLong(SizeEstimatorHelper::sizeOfString).boxed().collect(Collectors.toList())
        );
        return alignObjectSize(size);
    }

    @Override
    public String getName() {
        return OneHotEncoding.NAME.getPreferredName();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_LENGTH.getPreferredName(), fieldLength);
        builder.field(FEATURE_NAME_LENGTHS.getPreferredName(), featureNameLengths);
        builder.field(FIELD_VALUE_LENGTHS.getPreferredName(), fieldValueLengths);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OneHotEncodingSize that = (OneHotEncodingSize) o;
        return fieldLength == that.fieldLength &&
            Arrays.equals(featureNameLengths, that.featureNameLengths) &&
            Arrays.equals(fieldValueLengths, that.fieldValueLengths);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(fieldLength);
        result = 31 * result + Arrays.hashCode(featureNameLengths);
        result = 31 * result + Arrays.hashCode(fieldValueLengths);
        return result;
    }
}
