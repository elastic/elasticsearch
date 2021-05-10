/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.deployment;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.MlParserUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/*
 * TODO This does not necessarily belong in core. Will have to reconsider
 * once we figure the format we store inference results in client calls.
*/
public class PyTorchResult implements ToXContentObject, Writeable {

    private static final ParseField REQUEST_ID = new ParseField("request_id");
    private static final ParseField INFERENCE = new ParseField("inference");
    private static final ParseField ERROR = new ParseField("error");

    public static final ConstructingObjectParser<PyTorchResult, Void> PARSER = new ConstructingObjectParser<>("pytorch_result",
        a -> new PyTorchResult((String) a[0], (double[][]) a[1], (String) a[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), REQUEST_ID);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> {
                List<List<Double>> listOfListOfDoubles = MlParserUtils.parseArrayOfArrays(
                    INFERENCE.getPreferredName(), XContentParser::doubleValue, p);
                double[][] primitiveDoubles = new double[listOfListOfDoubles.size()][];
                for (int i = 0; i < listOfListOfDoubles.size(); i++) {
                    List<Double> row = listOfListOfDoubles.get(i);
                    primitiveDoubles[i] = row.stream().mapToDouble(d -> d).toArray();
                }
                return primitiveDoubles;
            },
            INFERENCE,
            ObjectParser.ValueType.VALUE_ARRAY
        );
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ERROR);
    }

    private final String requestId;
    private final double[][] inference;
    private final String error;

    public PyTorchResult(String requestId, @Nullable double[][] inference, @Nullable String error) {
        this.requestId = Objects.requireNonNull(requestId);
        this.inference = inference;
        this.error = error;
    }

    public PyTorchResult(StreamInput in) throws IOException {
        requestId = in.readString();
        boolean hasInference = in.readBoolean();
        if (hasInference) {
            inference = in.readArray(StreamInput::readDoubleArray, length -> new double[length][]);
        } else {
            inference = null;
        }
        error = in.readOptionalString();
    }

    public String getRequestId() {
        return requestId;
    }

    public double[][] getInferenceResult() {
        return inference;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(REQUEST_ID.getPreferredName(), requestId);
        if (inference != null) {
            builder.field(INFERENCE.getPreferredName(), inference);
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
            out.writeArray(StreamOutput::writeDoubleArray, inference);
        }
        out.writeOptionalString(error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, Arrays.hashCode(inference), error);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;

        PyTorchResult that = (PyTorchResult) other;
        return Objects.equals(requestId, that.requestId)
            && Objects.equals(inference, that.inference)
            && Objects.equals(error, that.error);
    }
}
