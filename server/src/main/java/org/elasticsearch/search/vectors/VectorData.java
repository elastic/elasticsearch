/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.Strings.format;

public record VectorData(float[] floatVector, byte[] byteVector) implements Writeable, ToXContentFragment {

    private VectorData(float[] floatVector) {
        this(floatVector, null);
    }

    private VectorData(byte[] byteVector) {
        this(null, byteVector);
    }

    public VectorData(StreamInput in) throws IOException {
        this(in.readOptionalFloatArray(), in.readOptionalByteArray());
    }

    public VectorData {
        if (false == (floatVector == null ^ byteVector == null)) {
            throw new IllegalArgumentException("please supply exactly either a float or a byte vector");
        }
    }

    public byte[] asByteVector() {
        if (byteVector != null) {
            return byteVector;
        }
        DenseVectorFieldMapper.ElementType.BYTE.checkVectorBounds(floatVector);
        byte[] vec = new byte[floatVector.length];
        for (int i = 0; i < floatVector.length; i++) {
            vec[i] = (byte) floatVector[i];
        }
        return vec;
    }

    // Explicit byte parsing is only supported when operation on hex-encoded byte vectors
    // which are defined only for ElementType.BYTE, so we should throw if we request to convert it to float
    public float[] asFloatVector() {
        return asFloatVector(true);
    }

    public float[] asFloatVector(boolean failIfByte) {
        if (byteVector != null) {
            if (failIfByte) {
                throw new UnsupportedOperationException("cannot convert to float, as we're explicitly using a byte vector");
            } else {
                float[] vec = new float[byteVector.length];
                for (int i = 0; i < byteVector.length; i++) {
                    vec[i] = byteVector[i];
                }
                return vec;
            }
        }
        return floatVector;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalFloatArray(floatVector);
        out.writeOptionalByteArray(byteVector);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray();
        if (floatVector != null) {
            for (float v : floatVector) {
                builder.value(v);
            }
        } else {
            for (byte b : byteVector) {
                builder.value(b);
            }
        }
        builder.endArray();
        return builder;
    }

    @Override
    public String toString() {
        return floatVector != null ? Arrays.toString(floatVector) : Arrays.toString(byteVector);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        VectorData other = (VectorData) obj;
        return Arrays.equals(floatVector, other.floatVector) && Arrays.equals(byteVector, other.byteVector);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(floatVector), Arrays.hashCode(byteVector));
    }

    public static VectorData parseXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        return switch (token) {
            case START_ARRAY -> parseQueryVectorArray(parser);
            case VALUE_STRING -> parseHexEncodedVector(parser);
            case VALUE_NUMBER -> parseNumberVector(parser);
            default -> throw new ParsingException(parser.getTokenLocation(), format("Unknown type [%s] for parsing vector", token));
        };
    }

    private static VectorData parseQueryVectorArray(XContentParser parser) throws IOException {
        XContentParser.Token token;
        List<Float> vectorArr = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.VALUE_NUMBER || token == XContentParser.Token.VALUE_STRING) {
                vectorArr.add(parser.floatValue());
            } else {
                throw new ParsingException(parser.getTokenLocation(), format("Type [%s] not supported for query vector", token));
            }
        }
        float[] floatVector = new float[vectorArr.size()];
        for (int i = 0; i < vectorArr.size(); i++) {
            floatVector[i] = vectorArr.get(i);
        }
        return VectorData.fromFloats(floatVector);
    }

    private static VectorData parseHexEncodedVector(XContentParser parser) throws IOException {
        return VectorData.fromBytes(HexFormat.of().parseHex(parser.text()));
    }

    private static VectorData parseNumberVector(XContentParser parser) throws IOException {
        float val = parser.floatValue();
        return VectorData.fromFloats(new float[] { val });
    }

    public static VectorData fromFloats(float[] vec) {
        return vec == null ? null : new VectorData(vec);
    }

    public static VectorData fromBytes(byte[] vec) {
        return vec == null ? null : new VectorData(vec);
    }
}
