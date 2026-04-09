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
import org.elasticsearch.index.codec.vectors.BFloat16;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
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
            throw new IllegalArgumentException("please supply exactly one of a float vector, byte vector, or encoded (hex/base64) vector");
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

    public int size() {
        if (floatVector != null) {
            return floatVector.length;
        }
        if (byteVector != null) {
            return byteVector.length;
        }
        return 0;
    }

    public byte[] asByteVector() {
        if (stringVector != null) {
            throw new IllegalStateException("encoded query vector must be resolved against the field type before use");
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
            throw new IllegalStateException("encoded query vector must be resolved against the field type before use");
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
            throw new IllegalStateException("encoded query vector must be resolved against the field type before use");
        }
        if (floatVector != null) {
            element.writeValues(byteBuffer, floatVector);
        } else {
            byteBuffer.put(byteVector);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalFloatArray(floatVector);
        if (out.getTransportVersion().supports(QUERY_VECTOR_BASE64)) {
            out.writeOptionalByteArray(byteVector);
            out.writeOptionalString(stringVector);
            return;
        }
        if (stringVector != null) {
            try {
                out.writeOptionalByteArray(HexFormat.of().parseHex(stringVector));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "failed to parse field [query_vector]: query_vector is not a valid hex string and transport version ["
                        + out.getTransportVersion()
                        + "] only supports hex-encoded vectors",
                    e
                );
            }
            return;
        }
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
        if (obj instanceof VectorData other) {
            return Arrays.equals(floatVector, other.floatVector)
                && Arrays.equals(byteVector, other.byteVector)
                && Objects.equals(stringVector, other.stringVector);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(floatVector), Arrays.hashCode(byteVector), stringVector);
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

    /**
     * Decodes an encoded string (hex or base64) to VectorData based on the element type and dimensions.
     * Base64 encoding is supported for all element types with proper byte interpretation.
     * Hex encoding produces byte vectors and is supported for BYTE and BIT element types, and for FLOAT/BFLOAT16
     * when the decoded byte length matches the expected dimensions.
     *
     * @param encoded the encoded vector string
     * @param elementType the element type (BYTE, FLOAT, BFLOAT16, BIT)
     * @param dims the expected dimensions
     * @return the decoded VectorData
     * @throws IllegalArgumentException if the string cannot be decoded or doesn't match expected dimensions
     */
    public static VectorData decodeQueryVector(String encoded, ElementType elementType, int dims) {
        byte[] hexBytes = tryParseHex(encoded);

        // Prefer hex if it matches expected dimensions (hex always produces byte[])
        if (hexBytes != null && hexBytes.length == dims) {
            return VectorData.fromBytes(hexBytes);
        }

        // For BIT element type, check hex with bit dimensions
        if (elementType == ElementType.BIT && hexBytes != null && hexBytes.length == dims / Byte.SIZE) {
            return VectorData.fromBytes(hexBytes);
        }

        byte[] base64Bytes = tryParseBase64(encoded);

        if (hexBytes == null && base64Bytes == null) {
            throw new IllegalArgumentException("failed to parse field [query_vector]: [query_vector] must be a valid base64 or hex string");
        }

        // Try base64 if it matches expected dimensions for the element type
        if (base64Bytes != null && matchesExpectedBase64Length(base64Bytes.length, elementType, dims)) {
            return decodeBase64Vector(base64Bytes, elementType, dims);
        }

        // Fall back to hex if available (downstream will handle dimension mismatch)
        if (hexBytes != null) {
            throw new IllegalArgumentException(
                "The query vector has a different number of dimensions [" + hexBytes.length + "] than the document vectors [" + dims + "]."
            );
        }

        // base64 was parsed but doesn't match dimensions
        throw invalidBase64Length(base64Bytes.length, elementType);
    }

    private static byte[] tryParseHex(String encoded) {
        try {
            return HexFormat.of().parseHex(encoded);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static byte[] tryParseBase64(String encoded) {
        try {
            return Base64.getDecoder().decode(encoded);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static boolean matchesExpectedBase64Length(int length, ElementType elementType, int dims) {
        return switch (elementType) {
            case BYTE -> length == dims;
            case BIT -> length == dims / Byte.SIZE;
            case FLOAT -> length == dims * Float.BYTES;
            case BFLOAT16 -> length == dims * Float.BYTES || length == dims * BFloat16.BYTES;
        };
    }

    private static VectorData decodeBase64Vector(byte[] base64Bytes, ElementType elementType, int dims) {
        return switch (elementType) {
            case BYTE, BIT -> VectorData.fromBytes(base64Bytes);
            case FLOAT -> decodeFloatVector(base64Bytes);
            case BFLOAT16 -> decodeBase64BFloat16Vector(base64Bytes, dims);
        };
    }

    private static VectorData decodeBase64BFloat16Vector(byte[] base64Bytes, int dims) {
        // Prefer bfloat16 if it matches exactly, otherwise float
        return base64Bytes.length == dims * BFloat16.BYTES ? decodeBFloat16Vector(base64Bytes) : decodeFloatVector(base64Bytes);
    }

    private static IllegalArgumentException invalidBase64Length(int length, ElementType elementType) {
        String expectedType = switch (elementType) {
            case BYTE, BIT -> "byte";
            case FLOAT -> "float";
            case BFLOAT16 -> "float or bfloat16";
        };
        return new IllegalArgumentException(
            "failed to parse field [query_vector]: "
                + "[query_vector] must contain a valid Base64-encoded "
                + expectedType
                + " vector, but the decoded bytes length ["
                + length
                + "] is not compatible with the expected vector length"
        );
    }

    private static VectorData decodeFloatVector(byte[] bytes) {
        int numFloats = bytes.length / Float.BYTES;
        float[] floats = new float[numFloats];
        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).asFloatBuffer().get(floats);
        return VectorData.fromFloats(floats);
    }

    private static VectorData decodeBFloat16Vector(byte[] bytes) {
        int numFloats = bytes.length / BFloat16.BYTES;
        float[] floats = new float[numFloats];
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        BFloat16.bFloat16ToFloat(buffer.asShortBuffer(), floats);
        return VectorData.fromFloats(floats);
    }

}
