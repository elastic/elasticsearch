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
public final class DecodedVector {

    /**
     * Which array backs this vector and how its contents are read as components.
     */
    private enum Layout {
        BYTES,
        ENCODED_FLOATS,
        DECODED_FLOATS
    }

    private final byte[] bytes;
    private final float[] floats;
    private final Layout layout;

    private DecodedVector(byte[] bytes, Layout layout) {
        this.bytes = bytes;
        this.floats = null;
        this.layout = layout;
    }

    private DecodedVector(float[] floats) {
        this.bytes = null;
        this.floats = floats;
        this.layout = Layout.DECODED_FLOATS;
    }

    /**
     * Decodes a dense vector supplied as a hex or base64 string, resolving which encoding was used and how
     * the resulting bytes should be read.
     *
     * @param encoded     hex or base64 string
     * @param elementType element type of the field
     * @param dims        expected number of dimensions
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
     * @throws IllegalArgumentException if the string cannot be decoded or doesn't match the expected dimensions
     */
    public static DecodedVector decode(String encoded, ElementType elementType, int dims, boolean parseHex) {
        boolean isHex = parseHex && isHexString(encoded);
        int hexVectorLength = encoded.length() / 2;
        if (isHex && hexVectorLength == elementType.vectorLength(dims)) {
            return new DecodedVector(HexFormat.of().parseHex(encoded), Layout.BYTES);
        }

        // Try base64 if it matches expected dimensions for the element type
        byte[] base64Bytes = tryParseBase64(encoded);
        if (base64Bytes != null && matchesExpectedBase64Length(base64Bytes.length, elementType, dims)) {
            if (elementType == ElementType.BFLOAT16 && base64Bytes.length == dims * BFloat16.BYTES) {
                float[] widened = new float[dims];
                BFloat16.bFloat16ToFloat(base64Bytes, 0, widened, 0, dims, ByteOrder.BIG_ENDIAN);
                return new DecodedVector(widened);
            }
            return new DecodedVector(base64Bytes, layoutFor(elementType));
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

        if (base64Bytes == null) {
            StringBuilder sb = new StringBuilder("failed to decode vector: value must be a valid base64");
            if (parseHex) {
                sb.append(" or hex");
            }
            sb.append(" string");
            throw new IllegalArgumentException(sb.toString());
        }

        // base64 was parsed but doesn't match dimensions
        throw invalidBase64Length(base64Bytes.length, elementType);
    }

    public boolean isByteVector() {
        return layout == Layout.BYTES;
    }

    public byte[] bytes() {
        if (isByteVector() == false) {
            throw new IllegalStateException("vector components are not bytes, layout is [" + layout + "]");
        }
        return bytes;
    }

    public float[] toFloatArray() {
        return switch (layout) {
            case BYTES -> {
                float[] values = new float[componentCount()];
                for (int i = 0; i < values.length; i++) {
                    values[i] = bytes[i];
                }
                yield values;
            }
            case ENCODED_FLOATS -> {
                float[] values = new float[componentCount()];
                ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).asFloatBuffer().get(values);
                yield values;
            }
            case DECODED_FLOATS -> floats;
        };
    }

    public List<Object> toFloatList() {
        List<Object> values = new ArrayList<>(componentCount());
        switch (layout) {
            case BYTES -> {
                for (byte b : bytes) {
                    values.add((float) b);
                }
            }
            case ENCODED_FLOATS -> {
                ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
                int count = bytes.length / Float.BYTES;
                for (int i = 0; i < count; i++) {
                    values.add(buffer.getFloat());
                }
            }
            case DECODED_FLOATS -> {
                for (float f : floats) {
                    values.add(f);
                }
            }
        }
        return values;
    }

    /**
     * Returns the base64 encoding of this vector, one byte per component for byte vectors and four big-endian
     * bytes per component otherwise.
     */
    public String toBase64() {
        return switch (layout) {
            case BYTES, ENCODED_FLOATS -> Base64.getEncoder().encodeToString(bytes);
            case DECODED_FLOATS -> {
                ByteBuffer buffer = ByteBuffer.allocate(floats.length * Float.BYTES).order(ByteOrder.BIG_ENDIAN);
                buffer.asFloatBuffer().put(floats);
                yield Base64.getEncoder().encodeToString(buffer.array());
            }
        };
    }

    private int componentCount() {
        return switch (layout) {
            case BYTES -> bytes.length;
            case ENCODED_FLOATS -> bytes.length / Float.BYTES;
            case DECODED_FLOATS -> floats.length;
        };
    }

    private static Layout layoutFor(ElementType elementType) {
        return switch (elementType) {
            case BYTE, BIT -> Layout.BYTES;
            case FLOAT, BFLOAT16 -> Layout.ENCODED_FLOATS;
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
