/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 * This Java port of CLD3 was derived from Google's CLD3 project at https://github.com/google/cld3
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Represents a single layer in the compressed Lang Net
 */
public class LangNetLayer implements ToXContentObject, Writeable, Accountable {

    public static final ParseField NAME = new ParseField("lang_net_layer");
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(LangNetLayer.class);

    public static final ParseField NUM_ROWS = new ParseField("num_rows");
    public static final ParseField NUM_COLS = new ParseField("num_cols");
    public static final ParseField WEIGHTS = new ParseField("weights");
    public static final ParseField BIAS = new ParseField("bias");

    public static final ConstructingObjectParser<LangNetLayer, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<LangNetLayer, Void> LENIENT_PARSER = createParser(true);


    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<LangNetLayer, Void> createParser(boolean lenient) {
        ConstructingObjectParser<LangNetLayer, Void> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            a -> new LangNetLayer(
                (List<Double>) a[0],
                (int) a[1],
                (int) a[2],
                (List<Double>) a[3]));
        parser.declareDoubleArray(constructorArg(), WEIGHTS);
        parser.declareInt(constructorArg(), NUM_COLS);
        parser.declareInt(constructorArg(), NUM_ROWS);
        parser.declareDoubleArray(constructorArg(), BIAS);
        return parser;
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
        if (weights.length != numCols * numRows) {
            throw ExceptionsHelper.badRequestException("malformed network layer. Total vector size [{}] does not equal [{}] x [{}].",
                weights.length,
                numCols,
                numRows);
        }
    }

    LangNetLayer(StreamInput in) throws IOException {
        this.weights = in.readDoubleArray();
        this.bias = in.readDoubleArray();
        this.weightRows = in.readInt();
        this.weightCols = in.readInt();
    }

    double[] productPlusBias(boolean applyRelu, double[] x) {
        double[] y = Arrays.copyOf(bias, bias.length);

        for (int i = 0; i < x.length; ++i) {
            double scale = x[i];
            if (applyRelu) {
                if (scale > 0) {
                    for (int j = 0; j < y.length; ++j) {
                        y[j] += weights[rowMajorIndex(i, weightCols, j)] * scale;
                    }
                }
            } else {
                for (int j = 0; j < y.length; ++j) {
                    y[j] += weights[rowMajorIndex(i, weightCols, j)] * scale;
                }
            }
        }
        return y;
    }

    private static int rowMajorIndex(int row, int colDim, int col) {
        return row * colDim + col;
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
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += RamUsageEstimator.sizeOf(weights);
        size += RamUsageEstimator.sizeOf(bias);
        return size;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDoubleArray(weights);
        out.writeDoubleArray(bias);
        out.writeInt(weightRows);
        out.writeInt(weightCols);
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
