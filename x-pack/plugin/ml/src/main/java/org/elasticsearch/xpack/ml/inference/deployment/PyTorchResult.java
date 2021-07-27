/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ml.utils.MlParserUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * All results must have a request_id.
 * If the error field is not null the instance is an error result
 * so the inference and time_ms fields will be null.
 */
public class PyTorchResult implements ToXContentObject, Writeable {

    private static final ParseField REQUEST_ID = new ParseField("request_id");
    private static final ParseField INFERENCE = new ParseField("inference");
    private static final ParseField ERROR = new ParseField("error");
    private static final ParseField TIME_MS = new ParseField("time_ms");

    public static final ConstructingObjectParser<PyTorchResult, Void> PARSER = new ConstructingObjectParser<>("pytorch_result",
        a -> new PyTorchResult((String) a[0], (double[][][]) a[1], (Long) a[2], (String) a[3]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), REQUEST_ID);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            (p, c) ->
                MlParserUtils.parseArrayOfArraysOfArrays(INFERENCE.getPreferredName(), p),
            INFERENCE,
            ObjectParser.ValueType.VALUE_ARRAY
        );
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), TIME_MS);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ERROR);
    }

    public static PyTorchResult fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final String requestId;
    private final double[][][] inference;
    private final Long timeMs;
    private final String error;

    public PyTorchResult(String requestId,
                         @Nullable double[][][] inference,
                         @Nullable Long timeMs,
                         @Nullable String error) {
        this.requestId = Objects.requireNonNull(requestId);
        this.inference = inference;
        this.timeMs = timeMs;
        this.error = error;
    }

    public PyTorchResult(StreamInput in) throws IOException {
        requestId = in.readString();
        boolean hasInference = in.readBoolean();
        if (hasInference) {
            inference = in.readArray(in2 -> in2.readArray(StreamInput::readDoubleArray, double[][]::new), double[][][]::new);
        } else {
            inference = null;
        }
        timeMs = in.readOptionalLong();
        error = in.readOptionalString();
    }

    public String getRequestId() {
        return requestId;
    }

    public boolean isError() {
        return error != null;
    }

    public String getError() {
        return error;
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
        builder.field(REQUEST_ID.getPreferredName(), requestId);
        if (inference != null) {
            builder.startArray(INFERENCE.getPreferredName());
            for (int i=0; i<inference.length; i++) {
                builder.startArray();
                for (int j=0; j<inference[0].length; j++)
                {
                    builder.startArray();
                    for (int k=0; k< inference[0][0].length; k++) {
                        builder.value(inference[i][j][k]);
                    }
                    builder.endArray();
                }
                builder.endArray();
            }
            builder.endArray();
        }
        if (timeMs != null) {
            builder.field(TIME_MS.getPreferredName(), timeMs);
        }
        if (error != null) {
            builder.field(ERROR.getPreferredName(), error);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

        out.writeString(requestId);
        if (inference == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeArray(
                (out2, arr) -> out2.writeArray(StreamOutput::writeDoubleArray, arr),
                inference);
        }
        out.writeOptionalLong(timeMs);
        out.writeOptionalString(error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, Arrays.deepHashCode(inference), error);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;

        PyTorchResult that = (PyTorchResult) other;
        return Objects.equals(requestId, that.requestId)
            && Arrays.deepEquals(inference, that.inference)
            && Objects.equals(error, that.error);
    }
}
