/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.index.codec.vectors.BFloat16;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentString;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.index.codec.vectors.VectorTestUtils.randomByteVector;
import static org.elasticsearch.index.codec.vectors.VectorTestUtils.randomFloatVector;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapperTestUtils.randomCompatibleDimensions;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Unit tests for {@link DecodedVector}, covering every element type and every supported encoding
 * format (hex and the base64 variants) as well as error paths for wrong dimensions and invalid input.
 * Each test runs against both the {@code String} and {@code UTF8Bytes} overloads of {@link DecodedVector#decode}.
 */
public class DecodedVectorTests extends ESTestCase {
    /** Max error from a round-trip through bfloat16 (half an ulp) for the [-1, +1) values randomFloatVector produces. */
    private static final float BFLOAT16_DELTA = 0x1p-9f;

    /** Which input form to test. */
    private enum InputKind {
        STRING,
        UTF8_BYTES
    }

    /**
     * The base64 payload layouts that {@link DecodedVector} accepts.
     */
    private enum Base64Form {
        RAW_BYTES,
        FLOAT32,
        BFLOAT16
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<Object[]> params = new ArrayList<>();
        for (ElementType elementType : ElementType.values()) {
            for (InputKind inputKind : InputKind.values()) {
                params.add(new Object[] { elementType, inputKind });
            }
        }
        return params;
    }

    private final ElementType elementType;
    private final InputKind inputKind;

    public DecodedVectorTests(ElementType elementType, InputKind inputKind) {
        this.elementType = elementType;
        this.inputKind = inputKind;
    }

    /**
     * A valid hex string (of the correct length for the element type) should decode to a
     * {@link DecodedVector.ByteVector} regardless of element type. Both lower-case and upper-case
     * hex strings are accepted.
     */
    public void testDecodeHex() {
        int dims = randomDims();
        int vectorLength = elementType.vectorLength(dims);
        byte[] raw = randomByteVector(vectorLength);

        // HexFormat.of() produces lowercase; randomly test upper-case too
        String hex = HexFormat.of().formatHex(raw);
        if (randomBoolean()) {
            hex = hex.toUpperCase(Locale.ROOT);
        }

        DecodedVector decoded = decode(hex, dims);
        assertByteVector(decoded, raw);
        assertEquals(Base64.getEncoder().encodeToString(raw), decoded.toBase64());
    }

    /**
     * When {@code parseHex=false}, a valid hex string of the correct length is not decoded via the
     * hex path, so the call falls through to the base64 path and fails.
     */
    public void testDecodeHexWithParseHexDisabled() {
        // A single-byte vector hex-encodes to 2 characters, which are also valid base64 decoding to exactly 1 byte —
        // i.e. the expected vector length — so the base64 fallback would succeed and no exception would be thrown.
        // Require dims >= 16 to rule out that degenerate case.
        int dims = randomCompatibleDimensions(elementType, 16, 128);
        int vectorLength = elementType.vectorLength(dims);
        byte[] raw = randomByteVector(vectorLength);
        String hex = HexFormat.of().formatHex(raw);

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> decode(hex, dims, false));
        assertThat(ex.getMessage(), containsString("failed to decode vector: Base64 decoded vector byte length ["));
    }

    /**
     * Sweeps every accepted base64 layout for this element type.
     */
    public void testDecodeBase64() {
        int dims = randomDims();
        for (Base64Form form : acceptedBase64Forms()) {
            assertDecodesBase64(form, dims);
        }
    }

    /**
     * A hex string whose byte length does not match {@code elementType.vectorLength(dims)} triggers
     * an error that reports the actual and expected dimension counts.
     */
    public void testDecodeWrongDimensionsHex() {
        int dims = randomDims();
        // A hex string always decodes as base64 too, so pick a byte count whose base64 representation will not match the expected dimension
        // count
        int wrongLength = wrongByteArrayLength(dims, elementType);
        byte[] raw = randomByteArrayOfLength(wrongLength);
        String hex = HexFormat.of().formatHex(raw);

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> decode(hex, dims));
        assertThat(ex.getMessage(), containsString("dimensions [" + elementType.dims(wrongLength) + "]"));
        assertThat(ex.getMessage(), containsString("] than the expected [" + dims + "]"));
    }

    /**
     * A base64 payload whose decoded byte length matches none of the accepted lengths for the
     * element type triggers an error that names the expected component type and the actual byte count.
     */
    public void testDecodeWrongDimensionsBase64() {
        int dims = randomDims();
        int wrongLength = wrongByteArrayLength(dims, elementType);
        byte[] raw = randomByteArrayOfLength(wrongLength);
        // Set the first byte to 0xFF so the first base64 character is '/' (not a hex digit),
        // ensuring the string is not misidentified as a hex string
        raw[0] = (byte) 0xFF;
        String encoded = Base64.getEncoder().encodeToString(raw);

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> decode(encoded, dims));
        assertThat(ex.getMessage(), containsString("Base64 decoded vector byte length [" + wrongLength + "]"));
        assertThat(ex.getMessage(), containsString("for dimension count [" + dims + "]"));
        switch (elementType) {
            case BYTE, BIT -> assertThat(ex.getMessage(), containsString("expected length of [" + elementType.vectorLength(dims) + "]"));
            case FLOAT, BFLOAT16 -> {
                assertThat(
                    ex.getMessage(),
                    containsString("expected length of [" + (dims * Float.BYTES) + "] or [" + (dims * BFloat16.BYTES) + "]")
                );
            }
        }
    }

    /**
     * A string that is neither a valid hex string nor a valid base64 string is rejected with an
     * error whose text includes "or hex" when hex parsing is enabled, and omits it when disabled.
     */
    public void testDecodeInvalidString() {
        int dims = randomDims();
        String invalid = "not-valid-base64!!!";

        IllegalArgumentException ex3 = expectThrows(IllegalArgumentException.class, () -> decode(invalid, dims));
        assertThat(ex3.getMessage(), containsString("value must be a valid base64 or hex string"));

        IllegalArgumentException ex4 = expectThrows(IllegalArgumentException.class, () -> decode(invalid, dims, false));
        assertThat(ex4.getMessage(), containsString("value must be a valid base64 string"));
    }

    private List<Base64Form> acceptedBase64Forms() {
        return switch (elementType) {
            case BYTE, BIT -> List.of(Base64Form.RAW_BYTES);
            case FLOAT, BFLOAT16 -> List.of(Base64Form.FLOAT32, Base64Form.BFLOAT16);
        };
    }

    private int randomDims() {
        return randomCompatibleDimensions(elementType, 2, 128);
    }

    private void assertDecodesBase64(Base64Form form, int dims) {
        switch (form) {
            case RAW_BYTES -> {
                byte[] raw = randomByteVector(elementType.vectorLength(dims));
                String encoded = Base64.getEncoder().encodeToString(raw);

                DecodedVector decoded = decode(encoded, dims);

                assertByteVector(decoded, raw);
                assertEquals(form.name(), encoded, decoded.toBase64());
            }
            case FLOAT32 -> {
                float[] floats = randomFloatVector(dims);
                byte[] bytes = floatsToBigEndianBytes(floats);
                String encoded = Base64.getEncoder().encodeToString(bytes);

                DecodedVector decoded = decode(encoded, dims);

                assertThat(decoded, instanceOf(DecodedVector.EncodedFloatVector.class));
                assertFloatVector(decoded, floats, 0f);
                assertEquals(form.name(), encoded, decoded.toBase64());
            }
            case BFLOAT16 -> {
                float[] floats = randomFloatVector(dims);
                byte[] bf16Bytes = new byte[dims * BFloat16.BYTES];
                BFloat16.floatToBFloat16(floats, 0, bf16Bytes, 0, dims, ByteOrder.BIG_ENDIAN);
                String encoded = Base64.getEncoder().encodeToString(bf16Bytes);

                DecodedVector decoded = decode(encoded, dims);

                assertThat(decoded, instanceOf(DecodedVector.FloatVector.class));
                assertFloatVector(decoded, floats, BFLOAT16_DELTA);

                // FloatVector.toBase64() widens to float32 (4 bytes per component) — does not preserve the 2-byte input
                float[] widened = new float[dims];
                BFloat16.bFloat16ToFloat(bf16Bytes, 0, widened, 0, dims, ByteOrder.BIG_ENDIAN);
                assertEquals(form.name(), Base64.getEncoder().encodeToString(floatsToBigEndianBytes(widened)), decoded.toBase64());
            }
        }
    }

    private DecodedVector decode(String encoded, int dims) {
        return decode(encoded, dims, true);
    }

    private DecodedVector decode(String encoded, int dims, boolean parseHex) {
        return switch (inputKind) {
            case STRING -> DecodedVector.decode(encoded, elementType, dims, parseHex);
            case UTF8_BYTES -> {
                byte[] encodedBytes = encoded.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                // Randomly embed the payload inside a larger array to exercise non-zero offset paths,
                // since optimizedText().bytes() normally returns a slice with offset != 0.
                int leading = randomIntBetween(0, 4);
                int trailing = randomIntBetween(0, 4);
                byte[] padded = new byte[leading + encodedBytes.length + trailing];
                System.arraycopy(encodedBytes, 0, padded, leading, encodedBytes.length);
                XContentString.UTF8Bytes utf8 = new XContentString.UTF8Bytes(padded, leading, encodedBytes.length);
                yield DecodedVector.decode(utf8, elementType, dims, parseHex);
            }
        };
    }

    private static void assertByteVector(DecodedVector decoded, byte[] expectedBytes) {
        assertThat(decoded, instanceOf(DecodedVector.ByteVector.class));
        assertTrue(decoded.isByteVector());
        assertArrayEquals(expectedBytes, decoded.bytes());

        float[] expectedFloats = expectedFloatsFromBytes(expectedBytes);
        assertArrayEquals(expectedFloats, decoded.toFloatArray(), 0f);
        assertEquals(toIntObjectList(expectedBytes), decoded.toValueList());
    }

    private static void assertFloatVector(DecodedVector decoded, float[] expectedFloats, float delta) {
        assertFalse(decoded.isByteVector());
        assertThat(expectThrows(IllegalStateException.class, decoded::bytes).getMessage(), containsString("not a byte vector"));
        assertArrayEquals(expectedFloats, decoded.toFloatArray(), delta);

        List<Object> actualList = decoded.toValueList();
        assertEquals(expectedFloats.length, actualList.size());
        for (int i = 0; i < expectedFloats.length; i++) {
            assertThat(actualList.get(i), instanceOf(Float.class));
            assertEquals(expectedFloats[i], (Float) actualList.get(i), delta);
        }
    }

    /**
     * Generate a byte array length guaranteed to not match {@code dims} when the byte array is encoded as hex or base64.
     */
    private static int wrongByteArrayLength(int dims, ElementType elementType) {
        // If the element type is BFLOAT16, use FLOAT instead to ensure that we don't generate a byte array length that is exactly twice
        // the expected size. When using floating-point element types (FLOAT or BFLOAT16), a byte array of this length could be interpreted
        // as 32-bit floats.
        var element = DenseVectorFieldMapper.Element.getElement(elementType == ElementType.BFLOAT16 ? ElementType.FLOAT : elementType);
        return element.getNumBytes(dims) + randomIntBetween(1, 10);
    }

    private static float[] expectedFloatsFromBytes(byte[] bytes) {
        float[] floats = new float[bytes.length];
        for (int i = 0; i < floats.length; i++) {
            floats[i] = bytes[i];
        }
        return floats;
    }

    /** Encodes {@code floats} as big-endian IEEE-754 bytes. */
    private static byte[] floatsToBigEndianBytes(float[] floats) {
        ByteBuffer buffer = ByteBuffer.allocate(floats.length * Float.BYTES).order(ByteOrder.BIG_ENDIAN);
        buffer.asFloatBuffer().put(floats);
        return buffer.array();
    }

    private static List<Object> toIntObjectList(byte[] bytes) {
        List<Object> list = new ArrayList<>(bytes.length);
        for (byte b : bytes) {
            list.add((int) b);
        }
        return list;
    }
}
