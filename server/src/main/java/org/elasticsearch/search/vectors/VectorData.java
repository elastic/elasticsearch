/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.Strings.format;

public record VectorData(float[] floatVector, byte[] byteVector, String base64Vector) implements Writeable, ToXContentFragment {

    private static final TransportVersion QUERY_VECTOR_BASE64 = TransportVersion.fromName("knn_query_vector_base64");

    public VectorData(float[] floatVector) {
        this(floatVector, null, null);
    }

    public VectorData(byte[] byteVector) {
        this(null, byteVector, null);
    }

    public VectorData(String base64Vector) {
        this(null, null, base64Vector);
    }

    public VectorData(StreamInput in) throws IOException {
        this(in.readOptionalFloatArray(), in.readOptionalByteArray(), readOptionalBase64Vector(in));
    }

    public VectorData {
        int count = (floatVector != null ? 1 : 0) + (byteVector != null ? 1 : 0) + (base64Vector != null ? 1 : 0);
        if (count != 1) {
            throw new IllegalArgumentException("please supply exactly one of a float vector, byte vector, or base64 vector");
        }
    }

    public boolean isFloat() {
        return floatVector != null;
    }

    public boolean isBase64() {
        return base64Vector != null;
    }

    public String base64Vector() {
        return base64Vector;
    }

    public byte[] asByteVector() {
        if (base64Vector != null) {
            throw new IllegalStateException("base64 query vector must be resolved against the field type before use");
        }
        if (byteVector != null) {
            return byteVector;
        }
        DenseVectorFieldMapper.BYTE_ELEMENT.checkVectorBounds(floatVector);
        byte[] vec = new byte[floatVector.length];
        for (int i = 0; i < floatVector.length; i++) {
            vec[i] = (byte) floatVector[i];
        }
        return vec;
    }

    public float[] asFloatVector() {
        if (base64Vector != null) {
            throw new IllegalStateException("base64 query vector must be resolved against the field type before use");
        }
        if (floatVector != null) {
            return floatVector;
        }
        float[] vec = new float[byteVector.length];
        for (int i = 0; i < byteVector.length; i++) {
            vec[i] = byteVector[i];
        }
        return vec;
    }

    public void addToBuffer(DenseVectorFieldMapper.Element element, ByteBuffer byteBuffer) {
        if (base64Vector != null) {
            throw new IllegalStateException("base64 query vector must be resolved against the field type before use");
        }
        if (floatVector != null) {
            element.writeValues(byteBuffer, floatVector);
        } else {
            byteBuffer.put(byteVector);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (base64Vector != null && out.getTransportVersion().supports(QUERY_VECTOR_BASE64) == false) {
            throw new IllegalArgumentException(
                "query_vector contains base64 but transport version ["
                    + out.getTransportVersion()
                    + "] does not support base64 query vectors"
            );
        }
        out.writeOptionalFloatArray(floatVector);
        out.writeOptionalByteArray(byteVector);
        if (out.getTransportVersion().supports(QUERY_VECTOR_BASE64)) {
            out.writeOptionalString(base64Vector);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (floatVector != null) {
            builder.startArray();
            for (float v : floatVector) {
                builder.value(v);
            }
            builder.endArray();
        } else if (base64Vector != null) {
            builder.value(base64Vector);
        } else {
            builder.value(HexFormat.of().formatHex(byteVector));
        }
        return builder;
    }

    @Override
    public String toString() {
        if (floatVector != null) {
            return Arrays.toString(floatVector);
        }
        if (byteVector != null) {
            return Arrays.toString(byteVector);
        }
        return base64Vector;
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
        return Arrays.equals(floatVector, other.floatVector)
            && Arrays.equals(byteVector, other.byteVector)
            && Objects.equals(base64Vector, other.base64Vector);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(floatVector), Arrays.hashCode(byteVector), base64Vector);
    }

    public static VectorData parseXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        return switch (token) {
            case START_ARRAY -> parseQueryVectorArray(parser);
            case VALUE_STRING -> parseStringVector(parser);
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

    private static VectorData parseStringVector(XContentParser parser) throws IOException {
        return VectorData.fromBase64(parser.text());
    }

    private static VectorData parseNumberVector(XContentParser parser) throws IOException {
        return VectorData.fromFloats(new float[] { parser.floatValue() });
    }

    public static VectorData fromFloats(float[] vec) {
        return vec == null ? null : new VectorData(vec);
    }

    public static VectorData fromBytes(byte[] vec) {
        return vec == null ? null : new VectorData(vec);
    }

    public static VectorData fromBase64(String base64) {
        return base64 == null ? null : new VectorData(null, null, base64);
    }

    private static String readOptionalBase64Vector(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(QUERY_VECTOR_BASE64)) {
            return in.readOptionalString();
        }
        return null;
    }

}
