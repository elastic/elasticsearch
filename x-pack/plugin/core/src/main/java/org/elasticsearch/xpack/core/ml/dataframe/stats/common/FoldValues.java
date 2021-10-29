/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.common;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class FoldValues implements Writeable, ToXContentObject {

    public static final ParseField FOLD = new ParseField("fold");
    public static final ParseField VALUES = new ParseField("values");

    public static FoldValues fromXContent(XContentParser parser, boolean ignoreUnknownFields) {
        return createParser(ignoreUnknownFields).apply(parser, null);
    }

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<FoldValues, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<FoldValues, Void> parser = new ConstructingObjectParser<>(
            "fold_values",
            ignoreUnknownFields,
            a -> new FoldValues((int) a[0], (List<Double>) a[1])
        );
        parser.declareInt(ConstructingObjectParser.constructorArg(), FOLD);
        parser.declareDoubleArray(ConstructingObjectParser.constructorArg(), VALUES);
        return parser;
    }

    private final int fold;
    private final double[] values;

    private FoldValues(int fold, List<Double> values) {
        this(fold, values.stream().mapToDouble(Double::doubleValue).toArray());
    }

    public FoldValues(int fold, double[] values) {
        this.fold = fold;
        this.values = values;
    }

    public FoldValues(StreamInput in) throws IOException {
        fold = in.readVInt();
        values = in.readDoubleArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(fold);
        out.writeDoubleArray(values);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FOLD.getPreferredName(), fold);
        builder.array(VALUES.getPreferredName(), values);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FoldValues other = (FoldValues) o;
        return fold == other.fold && Arrays.equals(values, other.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fold, Arrays.hashCode(values));
    }
}
