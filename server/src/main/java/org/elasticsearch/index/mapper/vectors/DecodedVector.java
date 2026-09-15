/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.elasticsearch.index.codec.vectors.BFloat16;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;

/**
 * A dense vector decoded from a hex or base64 string.
 */
public abstract sealed class DecodedVector permits DecodedVector.ByteVector, DecodedVector.EncodedFloatVector, DecodedVector.FloatVector {

    /**
     * Decodes a dense vector supplied as a hex or base64 string, resolving which encoding was used and how
     * the resulting bytes should be read.
     *
     * @param encoded     hex or base64 string
     * @param elementType element type of the field
     * @param dims        expected number of dimensions
     * @return the decoded vector
     * @throws IllegalArgumentException if the string cannot be decoded or doesn't match the expected dimensions
     */
    public static DecodedVector decode(String encoded, ElementType elementType, int dims) {
        return decode(encoded, elementType, dims, true);
    }

    /**
     * Decodes a dense vector supplied as a hex or base64 string, resolving which encoding was used and how
     * the resulting bytes should be read.
     *
     * @param encoded     hex or base64 string
     * @param elementType element type of the field
     * @param dims        expected number of dimensions
     * @param parseHex    flag controlling if hex parsing is attempted
     * @return the decoded vector
     * @throws IllegalArgumentException if the string cannot be decoded or doesn't match the expected dimensions
     */
    public static DecodedVector decode(String encoded, ElementType elementType, int dims, boolean parseHex) {
        boolean isHex = parseHex && isHexString(encoded);
        int hexVectorLength = encoded.length() / 2;
        if (isHex && hexVectorLength == elementType.vectorLength(dims)) {
            return new ByteVector(HexFormat.of().parseHex(encoded));
        }

        // Try base64 if it matches expected dimensions for the element type
        byte[] base64Bytes = tryParseBase64(encoded);
        if (base64Bytes != null && matchesExpectedBase64Length(base64Bytes.length, elementType, dims)) {
            if (elementType == ElementType.BFLOAT16 && base64Bytes.length == dims * BFloat16.BYTES) {
                float[] widened = new float[dims];
                BFloat16.bFloat16ToFloat(base64Bytes, 0, widened, 0, dims, ByteOrder.BIG_ENDIAN);
                return new FloatVector(widened);
            }
            return byteBackedVector(base64Bytes, elementType);
        }

        // The value is hex but doesn't match the expected dimensions
        if (isHex) {
            throw new IllegalArgumentException(
                "failed to decode vector: hex-decoded vector has a different number of dimensions ["
                    + elementType.dims(hexVectorLength)
                    + "] than the expected ["
                    + dims
                    + "]"
            );
        }

        // base64 was parsed but doesn't match dimensions
        if (base64Bytes != null) {
            throw invalidBase64Length(base64Bytes.length, elementType);
        }

        throw new IllegalArgumentException(
            "failed to decode vector: value must be a valid base64" + (parseHex ? " or hex" : "") + " string"
        );
    }

    /**
     * Returns {@code true} if this vector's components are single bytes.
     *
     * @return {@code true} for byte vectors, {@code false} otherwise
     */
    public boolean isByteVector() {
        return false;
    }

    /**
     * Returns the backing byte array for byte vectors.
     *
     * @return the byte array
     * @throws IllegalStateException if this is not a byte vector
     */
    public byte[] bytes() {
        throw new IllegalStateException("not a byte vector");
    }

    /**
     * Returns the vector components as {@code float[]}.
     *
     * @return float array of vector components
     */
    public abstract float[] toFloatArray();

    /**
     * Returns the vector components as a list of floats.
     *
     * @return list of boxed float components
     */
    public abstract List<Object> toFloatList();

    /**
     * Returns the base64 encoding of this vector, one byte per component for byte vectors and four big-endian
     * bytes per component otherwise.
     *
     * @return base64-encoded string representation
     */
    public abstract String toBase64();

    /** A vector whose components are stored as one raw byte each. */
    static final class ByteVector extends DecodedVector {
        private final byte[] bytes;

        ByteVector(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public boolean isByteVector() {
            return true;
        }

        @Override
        public byte[] bytes() {
            return bytes;
        }

        @Override
        public float[] toFloatArray() {
            float[] values = new float[bytes.length];
            for (int i = 0; i < values.length; i++) {
                values[i] = bytes[i];
            }
            return values;
        }

        @Override
        public List<Object> toFloatList() {
            List<Object> values = new ArrayList<>(bytes.length);
            for (byte b : bytes) {
                values.add((float) b);
            }
            return values;
        }

        @Override
        public String toBase64() {
            return Base64.getEncoder().encodeToString(bytes);
        }
    }

    /** A vector whose components are stored as four big-endian bytes each (IEEE 754 float). */
    static final class EncodedFloatVector extends DecodedVector {
        private final byte[] bytes;

        EncodedFloatVector(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public float[] toFloatArray() {
            float[] values = new float[bytes.length / Float.BYTES];
            ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).asFloatBuffer().get(values);
            return values;
        }

        @Override
        public List<Object> toFloatList() {
            ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
            int count = bytes.length / Float.BYTES;
            List<Object> values = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                values.add(buffer.getFloat());
            }
            return values;
        }

        @Override
        public String toBase64() {
            return Base64.getEncoder().encodeToString(bytes);
        }
    }

    /** A vector whose components are already decoded as {@code float} values. */
    static final class FloatVector extends DecodedVector {
        private final float[] floats;

        FloatVector(float[] floats) {
            this.floats = floats;
        }

        @Override
        public float[] toFloatArray() {
            return floats;
        }

        @Override
        public List<Object> toFloatList() {
            List<Object> values = new ArrayList<>(floats.length);
            for (float f : floats) {
                values.add(f);
            }
            return values;
        }

        @Override
        public String toBase64() {
            ByteBuffer buffer = ByteBuffer.allocate(floats.length * Float.BYTES).order(ByteOrder.BIG_ENDIAN);
            buffer.asFloatBuffer().put(floats);
            return Base64.getEncoder().encodeToString(buffer.array());
        }
    }

    private static DecodedVector byteBackedVector(byte[] bytes, ElementType elementType) {
        return switch (elementType) {
            case BYTE, BIT -> new ByteVector(bytes);
            case FLOAT, BFLOAT16 -> new EncodedFloatVector(bytes);
        };
    }

    private static boolean isHexString(String s) {
        int len = s.length();
        if (len % 2 != 0) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            if (HexFormat.isHexDigit(s.charAt(i)) == false) {
                return false;
            }
        }
        return true;
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
            case BYTE, BIT -> length == elementType.vectorLength(dims);
            case FLOAT -> length == dims * Float.BYTES;
            case BFLOAT16 -> length == dims * Float.BYTES || length == dims * BFloat16.BYTES;
        };
    }

    private static IllegalArgumentException invalidBase64Length(int length, ElementType elementType) {
        String expectedType = switch (elementType) {
            case BYTE, BIT -> "byte";
            case FLOAT -> "float";
            case BFLOAT16 -> "float or bfloat16";
        };
        return new IllegalArgumentException(
            "failed to decode vector: value must contain a valid Base64-encoded "
                + expectedType
                + " vector, but the decoded bytes length ["
                + length
                + "] is not compatible with the expected vector length"
        );
    }
}
