/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference.trainedmodel.langident;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Represents a single layer in the compressed Lang Net
 */
public class LangNetLayer implements ToXContentObject {

    public static final ParseField NAME = new ParseField("lang_net_layer");

    private static final ParseField NUM_ROWS = new ParseField("num_rows");
    private static final ParseField NUM_COLS = new ParseField("num_cols");
    private static final ParseField WEIGHTS = new ParseField("weights");
    private static final ParseField BIAS = new ParseField("bias");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<LangNetLayer, Void> PARSER = new ConstructingObjectParser<>(
        NAME.getPreferredName(),
        true,
        a -> new LangNetLayer(
            (List<Double>) a[0],
            (int) a[1],
            (int) a[2],
            (List<Double>) a[3]));

    static {
        PARSER.declareDoubleArray(constructorArg(), WEIGHTS);
        PARSER.declareInt(constructorArg(), NUM_COLS);
        PARSER.declareInt(constructorArg(), NUM_ROWS);
        PARSER.declareDoubleArray(constructorArg(), BIAS);
    }

    private final double[] weights;
    private final int weightRows;
    private final int weightCols;
    private final double[] bias;

    private LangNetLayer(List<Double> weights, int numCols, int numRows, List<Double> bias) {
        this(weights.stream().mapToDouble(Double::doubleValue).toArray(),
            numCols,
            numRows,
            bias.stream().mapToDouble(Double::doubleValue).toArray());
    }

    LangNetLayer(double[] weights, int numCols, int numRows, double[] bias) {
        this.weights = weights;
        this.weightCols = numCols;
        this.weightRows = numRows;
        this.bias = bias;
    }

    double[] getWeights() {
        return weights;
    }

    int getWeightRows() {
        return weightRows;
    }

    int getWeightCols() {
        return weightCols;
    }

    double[] getBias() {
        return bias;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NUM_COLS.getPreferredName(), weightCols);
        builder.field(NUM_ROWS.getPreferredName(), weightRows);
        builder.field(WEIGHTS.getPreferredName(), weights);
        builder.field(BIAS.getPreferredName(), bias);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LangNetLayer that = (LangNetLayer) o;
        return Arrays.equals(weights, that.weights)
            && Arrays.equals(bias, that.bias)
            && Objects.equals(weightCols, that.weightCols)
            && Objects.equals(weightRows, that.weightRows);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(weights), Arrays.hashCode(bias), weightCols, weightRows);
    }
}
