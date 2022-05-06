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
import java.util.Objects;

/**
 * All results must have a request_id.
 * If the error field is not null the instance is an error result
 * so the inference and time_ms fields will be null.
 */
public class PyTorchInferenceResult implements ToXContentObject {

    private static final ParseField INFERENCE = new ParseField("inference");
    private static final ParseField TIME_MS = new ParseField("time_ms");

    public static final ConstructingObjectParser<PyTorchInferenceResult, Void> PARSER = new ConstructingObjectParser<>(
        "pytorch_inference_result",
        a -> new PyTorchInferenceResult((String) a[0], (double[][][]) a[1], (Long) a[2])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), PyTorchResult.REQUEST_ID);
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> MlParserUtils.parse3DArrayOfDoubles(INFERENCE.getPreferredName(), p),
            INFERENCE,
            ObjectParser.ValueType.VALUE_ARRAY
        );
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), TIME_MS);
    }

    public static PyTorchInferenceResult fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final String requestId;
    private final double[][][] inference;
    private final Long timeMs;

    public PyTorchInferenceResult(String requestId, @Nullable double[][][] inference, @Nullable Long timeMs) {
        this.requestId = Objects.requireNonNull(requestId);
        this.inference = inference;
        this.timeMs = timeMs;
    }

    public String getRequestId() {
        return requestId;
    }

    public double[][][] getInferenceResult() {
        return inference;
    }

    public Long getTimeMs() {
        return timeMs;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(PyTorchResult.REQUEST_ID.getPreferredName(), requestId);
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
        if (timeMs != null) {
            builder.field(TIME_MS.getPreferredName(), timeMs);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, timeMs, Arrays.deepHashCode(inference));
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;

        PyTorchInferenceResult that = (PyTorchInferenceResult) other;
        return Objects.equals(requestId, that.requestId)
            && Arrays.deepEquals(inference, that.inference)
            && Objects.equals(timeMs, that.timeMs);
    }
}
