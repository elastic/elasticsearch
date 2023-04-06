/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.results;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.MlParserUtils;

import java.io.IOException;
import java.util.Arrays;

/**
 * All results must have a request_id.
 * If the error field is not null the instance is an error result
 * so the inference and time_ms fields will be null.
 */
public class PyTorchInferenceResult implements ToXContentObject {

    private static final ParseField INFERENCE = new ParseField("inference");

    public static final ConstructingObjectParser<PyTorchInferenceResult, Void> PARSER = new ConstructingObjectParser<>(
        "pytorch_inference_result",
        a -> new PyTorchInferenceResult((double[][][]) a[0])
    );

    static {
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> MlParserUtils.parse3DArrayOfDoubles(INFERENCE.getPreferredName(), p),
            INFERENCE,
            ObjectParser.ValueType.VALUE_ARRAY
        );
    }

    public static PyTorchInferenceResult fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final double[][][] inference;

    public PyTorchInferenceResult(@Nullable double[][][] inference) {
        this.inference = inference;
    }

    public double[][][] getInferenceResult() {
        return inference;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (inference != null) {
            builder.startArray(INFERENCE.getPreferredName());
            for (double[][] doubles : inference) {
                builder.startArray();
                for (int j = 0; j < inference[0].length; j++) {
                    builder.value(doubles[j]);
                }
                builder.endArray();
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Arrays.deepHashCode(inference);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;

        PyTorchInferenceResult that = (PyTorchInferenceResult) other;
        return Arrays.deepEquals(inference, that.inference);
    }
}
