/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.stats.common;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class FoldValues implements ToXContentObject {

    public static final ParseField FOLD = new ParseField("fold");
    public static final ParseField VALUES = new ParseField("values");

    @SuppressWarnings("unchecked")
    public static ConstructingObjectParser<FoldValues, Void> PARSER = new ConstructingObjectParser<>("fold_values", true,
        a -> new FoldValues((int) a[0], (List<Double>) a[1]));

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), FOLD);
        PARSER.declareDoubleArray(ConstructingObjectParser.constructorArg(), VALUES);
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

    public int getFold() {
        return fold;
    }

    public double[] getValues() {
        return values;
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
