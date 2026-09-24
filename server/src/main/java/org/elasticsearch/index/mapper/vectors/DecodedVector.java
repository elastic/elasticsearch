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
import org.elasticsearch.xcontent.XContentString;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;

/**
 * A dense vector decoded from a hex or base64 string, or UTF-8 byte slice.
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
        boolean isHex = parseHex && isHex(encoded);
        int hexVectorLength = encoded.length() / 2;
        if (isHex && hexVectorLength == elementType.vectorLength(dims)) {
            return new ByteVector(HexFormat.of().parseHex(encoded));
        }
        return decodeBase64OrFail(tryParseBase64(encoded), isHex, hexVectorLength, elementType, dims, parseHex);
    }

    /**
     * Decodes a dense vector supplied as a UTF-8 byte slice of a hex or base64 string, resolving which encoding
     * was used and how the resulting bytes should be read.
     *
     * @param utf8        UTF-8 bytes of the hex or base64 string
     * @param elementType element type of the field
     * @param dims        expected number of dimensions
     * @return the decoded vector
     * @throws IllegalArgumentException if the bytes are not valid hex or base64, or don't match the expected dimensions
     */
    public static DecodedVector decode(XContentString.UTF8Bytes utf8, ElementType elementType, int dims) {
        return decode(utf8, elementType, dims, true);
    }

    /**
     * Decodes a dense vector supplied as a UTF-8 byte slice of a hex or base64 string, resolving which encoding
     * was used and how the resulting bytes should be read.
     *
     * @param utf8        UTF-8 bytes of the hex or base64 string
     * @param elementType element type of the field
     * @param dims        expected number of dimensions
     * @param parseHex    flag controlling if hex parsing is attempted
     * @return the decoded vector
     * @throws IllegalArgumentException if the bytes are not valid hex or base64, or don't match the expected dimensions
     */
    public static DecodedVector decode(XContentString.UTF8Bytes utf8, ElementType elementType, int dims, boolean parseHex) {
        boolean isHex = parseHex && isHex(utf8.bytes(), utf8.offset(), utf8.length());
        int hexVectorLength = utf8.length() / 2;
        if (isHex && hexVectorLength == elementType.vectorLength(dims)) {
            return new ByteVector(parseHexDigits(utf8.bytes(), utf8.offset(), utf8.length()));
        }
        return decodeBase64OrFail(tryParseBase64(utf8), isHex, hexVectorLength, elementType, dims, parseHex);
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
     * Returns the vector components as a list of numbers: {@code Integer} for byte-backed vectors,
     * {@code Float} otherwise.
     *
     * @return list of numeric components
     */
    public abstract List<Object> toValueList();

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
        public List<Object> toValueList() {
            List<Object> values = new ArrayList<>(bytes.length);
            for (byte b : bytes) {
                values.add((int) b);
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
        private final ByteBuffer buffer;

        EncodedFloatVector(ByteBuffer buffer) {
            this.buffer = buffer.slice().order(ByteOrder.BIG_ENDIAN);
        }

        @Override
        public float[] toFloatArray() {
            float[] values = new float[buffer.remaining() / Float.BYTES];
            buffer.asFloatBuffer().get(values);
            return values;
        }

        @Override
        public List<Object> toValueList() {
            int count = buffer.remaining() / Float.BYTES;
            List<Object> values = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                values.add(buffer.getFloat(i * Float.BYTES));
            }
            return values;
        }

        @Override
        public String toBase64() {
            return Base64.getEncoder().encodeToString(toByteArray(buffer));
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
        public List<Object> toValueList() {
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

    /**
     * Completes decoding once the hex fast path has been ruled out: attempts to interpret {@code base64Bytes}
     * (null when the input was not valid base64) according to the element type, or reports why the input
     * could not be decoded.
     *
     * @param base64Bytes     the pre-attempted base64 decoded bytes, or {@code null} if decoding failed
     * @param isHex           whether the input looked like a hex string (wrong length for hex path)
     * @param hexVectorLength vector length the input would have had as hex, used only in the error message
     */
    private static DecodedVector decodeBase64OrFail(
        ByteBuffer base64Bytes,
        boolean isHex,
        int hexVectorLength,
        ElementType elementType,
        int dims,
        boolean parseHex
    ) {
        if (base64Bytes == null) {
            // Not valid base64, so a hex-looking value can only have had the wrong length
            if (isHex) {
                throw invalidHexDimensions(elementType, hexVectorLength, dims);
            }
            throw new IllegalArgumentException(
                "failed to decode vector: value must be a valid base64" + (parseHex ? " or hex" : "") + " string"
            );
        }

        int length = base64Bytes.remaining();
        if (matchesExpectedBase64Length(length, elementType, dims) == false) {
            // The value is also valid hex, so the hex dimension mismatch is the more useful message
            if (isHex) {
                throw invalidHexDimensions(elementType, hexVectorLength, dims);
            }
            throw invalidBase64Length(length, elementType, dims);
        }

        if ((elementType == ElementType.FLOAT || elementType == ElementType.BFLOAT16) && length == dims * BFloat16.BYTES) {
            float[] widened = new float[dims];
            BFloat16.bFloat16ToFloat(base64Bytes.order(ByteOrder.BIG_ENDIAN), widened);
            return new FloatVector(widened);
        }
        return byteBackedVector(base64Bytes, elementType);
    }

    private static DecodedVector byteBackedVector(ByteBuffer buffer, ElementType elementType) {
        return switch (elementType) {
            case BYTE, BIT -> new ByteVector(toByteArray(buffer));
            case FLOAT, BFLOAT16 -> new EncodedFloatVector(buffer);
        };
    }

    private static boolean isHex(String s) {
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

    private static boolean isHex(byte[] bytes, int offset, int length) {
        if (length % 2 != 0) {
            return false;
        }
        for (int i = offset, end = offset + length; i < end; i++) {
            if (HexFormat.isHexDigit(bytes[i] & 0xFF) == false) {
                return false;
            }
        }
        return true;
    }

    /** Parses hex digit pairs from an ASCII byte slice. Callers must validate with {@link #isHex} first. */
    private static byte[] parseHexDigits(byte[] bytes, int offset, int length) {
        byte[] result = new byte[length / 2];
        for (int i = 0; i < result.length; i++) {
            int high = HexFormat.fromHexDigit(bytes[offset + i * 2] & 0xFF);
            int low = HexFormat.fromHexDigit(bytes[offset + i * 2 + 1] & 0xFF);
            result[i] = (byte) ((high << 4) | low);
        }
        return result;
    }

    private static ByteBuffer tryParseBase64(String encoded) {
        try {
            return ByteBuffer.wrap(Base64.getDecoder().decode(encoded));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static ByteBuffer tryParseBase64(XContentString.UTF8Bytes utf8) {
        ByteBuffer srcBuffer = ByteBuffer.wrap(utf8.bytes(), utf8.offset(), utf8.length());
        try {
            return Base64.getDecoder().decode(srcBuffer);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Converts a {@link ByteBuffer} to a byte array, avoiding an array copy when the buffer's backing array
     * exactly covers the readable region. Does not modify the buffer's position.
     */
    private static byte[] toByteArray(ByteBuffer buffer) {
        if (buffer.hasArray() && buffer.arrayOffset() == 0 && buffer.position() == 0 && buffer.remaining() == buffer.array().length) {
            return buffer.array();
        }
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(buffer.position(), bytes);
        return bytes;
    }

    private static boolean matchesExpectedBase64Length(int length, ElementType elementType, int dims) {
        return switch (elementType) {
            case BYTE, BIT -> length == elementType.vectorLength(dims);
            case FLOAT, BFLOAT16 -> length == dims * Float.BYTES || length == dims * BFloat16.BYTES;
        };
    }

    private static IllegalArgumentException invalidBase64Length(int length, ElementType elementType, int dims) {
        String expected = switch (elementType) {
            case BYTE, BIT -> "[" + elementType.vectorLength(dims) + ']';
            case FLOAT, BFLOAT16 -> "[" + dims * Float.BYTES + "] or [" + dims * BFloat16.BYTES + "]";
        };
        return new IllegalArgumentException(
            "failed to decode vector: Base64 decoded vector byte length ["
                + length
                + "] does not match the expected length of "
                + expected
                + " for dimension count ["
                + dims
                + "]"
        );
    }

    private static IllegalArgumentException invalidHexDimensions(ElementType elementType, int hexVectorLength, int dims) {
        return new IllegalArgumentException(
            "failed to decode vector: hex-decoded vector has a different number of dimensions ["
                + elementType.dims(hexVectorLength)
                + "] than the expected ["
                + dims
                + "]"
        );
    }
}
