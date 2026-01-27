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

public record VectorData(float[] floatVector, byte[] byteVector, String stringVector) implements Writeable, ToXContentFragment {

    private static final TransportVersion QUERY_VECTOR_BASE64 = TransportVersion.fromName("knn_query_vector_base64");

    public VectorData(float[] floatVector) {
        this(floatVector, null, null);
    }

    public VectorData(byte[] byteVector) {
        this(null, byteVector, null);
    }

    public VectorData(String stringVector) {
        this(null, null, stringVector);
    }

    public VectorData(StreamInput in) throws IOException {
        this(in.readOptionalFloatArray(), in.readOptionalByteArray(), readOptionalStringVector(in));
    }

    public VectorData {
        int count = (floatVector != null ? 1 : 0) + (byteVector != null ? 1 : 0) + (stringVector != null ? 1 : 0);
        if (count != 1) {
            throw new IllegalArgumentException("please supply exactly one of a float vector, byte vector, or base64 vector");
        }
    }

    public boolean isFloat() {
        return floatVector != null;
    }

    public boolean isStringVector() {
        return stringVector != null;
    }

    public String stringVector() {
        return stringVector;
    }

    public byte[] asByteVector() {
        if (stringVector != null) {
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
        if (stringVector != null) {
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
        if (stringVector != null) {
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
        if (out.getTransportVersion().supports(QUERY_VECTOR_BASE64)) {
            out.writeOptionalFloatArray(floatVector);
            out.writeOptionalByteArray(byteVector);
            out.writeOptionalString(stringVector);
            return;
        }
        if (stringVector != null) {
            try {
                out.writeOptionalFloatArray(floatVector);
                out.writeOptionalByteArray(HexFormat.of().parseHex(stringVector));
                return;
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "query_vector contains base64 but transport version ["
                        + out.getTransportVersion()
                        + "] does not support base64 query vectors",
                    e
                );
            }
        }
        out.writeOptionalFloatArray(floatVector);
        out.writeOptionalByteArray(byteVector);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (floatVector != null) {
            builder.startArray();
            for (float v : floatVector) {
                builder.value(v);
            }
            builder.endArray();
        } else if (stringVector != null) {
            builder.value(stringVector);
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
        return stringVector;
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
        if (Arrays.equals(floatVector, other.floatVector) == false) {
            return false;
        }
        byte[] thisBytes = bytesOrNull();
        byte[] otherBytes = other.bytesOrNull();
        if (thisBytes != null || otherBytes != null) {
            return Arrays.equals(thisBytes, otherBytes);
        }
        return Objects.equals(stringVector, other.stringVector);
    }

    @Override
    public int hashCode() {
        byte[] bytes = bytesOrNull();
        if (bytes != null) {
            return Objects.hash(Arrays.hashCode(floatVector), Arrays.hashCode(bytes));
        }
        return Objects.hash(Arrays.hashCode(floatVector), stringVector);
    }

    private static byte[] tryParseHex(String encoded) {
        try {
            return HexFormat.of().parseHex(encoded);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private byte[] bytesOrNull() {
        if (byteVector != null) {
            return byteVector;
        }
        if (stringVector != null) {
            return tryParseHex(stringVector);
        }
        return null;
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
        return VectorData.fromStringVector(parser.text());
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

    public static VectorData fromStringVector(String encoded) {
        return encoded == null ? null : new VectorData(null, null, encoded);
    }

    private static String readOptionalStringVector(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(QUERY_VECTOR_BASE64)) {
            return in.readOptionalString();
        }
        return null;
    }

}
