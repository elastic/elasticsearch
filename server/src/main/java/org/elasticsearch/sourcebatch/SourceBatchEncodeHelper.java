/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Shared utilities for the inline binary array and key-value formats used by
 * batch encoders.
 */
public final class SourceBatchEncodeHelper {

    private SourceBatchEncodeHelper() {}

    /**
     * The inline array bytes and their {@code FIXED_ARRAY} / {@code UNION_ARRAY} kind, as produced
     * by {@link #packArray} and used when storing array-valued fields in a batch column.
     */
    public record PackedArray(byte arrayType, byte[] packed) {}

    /**
     * Packs the array the parser is positioned at (just after {@code START_ARRAY}) into inline
     * array bytes. Returns a {@link PackedArray} describing the wire type and packed bytes.
     */
    public static PackedArray packArray(XContentParser parser) throws IOException {
        return parseArray(parser, new ArrayScratch(), false);
    }

    /**
     * Like {@link #packArray(XContentParser)}, accumulating the elements in {@code scratch}, which an encoder reuses across arrays
     * so that each array does not grow fresh buffers. With {@code floatElements}, every number element is parsed straight to a
     * {@link SourceValueType#FLOAT}, as the row path parses the elements of a {@code float} vector, instead of being typed by
     * {@link XContentParser#numberType}: this rounds the value once, to the float the row path indexes, and parses it once,
     * where typing it first and reading it as a double would parse it twice. Non-number elements are packed as without it.
     */
    public static PackedArray packArray(XContentParser parser, ArrayScratch scratch, boolean floatElements) throws IOException {
        return parseArray(parser, scratch, floatElements);
    }

    /**
     * Reusable element buffers for {@link #packArray(XContentParser, ArrayScratch, boolean)}. Not thread-safe; nested arrays use
     * their own buffers, so one scratch serves one top-level array at a time.
     */
    public static final class ArrayScratch {
        private static final int INITIAL_CAPACITY = 16;

        private byte[] types = new byte[INITIAL_CAPACITY];
        private long[] numerics = new long[INITIAL_CAPACITY];
        private Object[] vars = new Object[INITIAL_CAPACITY];

        private void grow() {
            int newCap = types.length * 2;
            types = Arrays.copyOf(types, newCap);
            numerics = Arrays.copyOf(numerics, newCap);
            vars = Arrays.copyOf(vars, newCap);
        }

        /** Drops the references to the variable-length elements of the last array, so the scratch does not retain them. */
        private void clear(int count) {
            Arrays.fill(vars, 0, count, null);
        }
    }

    /**
     * The client's spelling of a floating-point token, {@code text}, when it is not the canonical
     * {@code Double.toString} rendering of {@code value}, which readers fall back to; {@code null} when it is, the
     * common case, so that nothing is stored for it.
     */
    @Nullable
    public static XContentString.UTF8Bytes lexicalForm(String text, double value) {
        if (Double.toString(value).equals(text)) {
            return null;
        }
        return new XContentString.UTF8Bytes(text.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Packs accumulated inline-array elements, choosing {@link SourceValueType#FIXED_ARRAY} when all
     * elements share a fixed-width type and {@code forceUnion} is false.
     */
    public static PackedArray packAccumulatedElements(
        byte[] elemTypes,
        long[] elemNumeric,
        Object[] elemVar,
        int count,
        boolean forceUnion
    ) {
        boolean useFixed = false;
        byte sharedType = 0;
        if (forceUnion == false && count > 0) {
            sharedType = elemTypes[0];
            useFixed = true;
            for (int i = 1; i < count; i++) {
                if (elemTypes[i] != sharedType) {
                    useFixed = false;
                    break;
                }
            }
            if (useFixed && SourceValueType.elemDataSize(sharedType) == 0) {
                useFixed = false;
            }
        }

        byte[] packed;
        byte arrayType;
        if (useFixed) {
            packed = packFixedArray(sharedType, elemNumeric, elemVar, count);
            arrayType = SourceValueType.FIXED_ARRAY;
        } else {
            packed = packUnionArray(elemTypes, elemNumeric, elemVar, count);
            arrayType = SourceValueType.UNION_ARRAY;
        }
        return new PackedArray(arrayType, packed);
    }

    /**
     * Packs a union array: per element: type(1) + data. No count byte — byte length terminates.
     */
    public static byte[] packUnionArray(byte[] elemTypes, long[] elemNumeric, Object[] elemVar, int count) {
        int size = 0;
        for (int i = 0; i < count; i++) {
            size += 1; // type byte
            size += elemDataSize(elemTypes[i], elemVar[i]);
        }
        byte[] packed = new byte[size];
        int pos = 0;
        for (int i = 0; i < count; i++) {
            packed[pos++] = elemTypes[i];
            pos = writeElemData(packed, pos, elemTypes[i], elemNumeric[i], elemVar[i]);
        }
        return packed;
    }

    /**
     * Packs a fixed array: element_type(1) + per element: data only. No count byte — byte length
     * terminates.
     */
    public static byte[] packFixedArray(byte sharedType, long[] elemNumeric, Object[] elemVar, int count) {
        int size = 1; // shared type byte
        for (int i = 0; i < count; i++) {
            size += elemDataSize(sharedType, elemVar[i]);
        }
        byte[] packed = new byte[size];
        packed[0] = sharedType;
        int pos = 1;
        for (int i = 0; i < count; i++) {
            pos = writeElemData(packed, pos, sharedType, elemNumeric[i], elemVar[i]);
        }
        return packed;
    }

    /**
     * Serializes an object from the parser into KEY_VALUE binary format.
     * Parser must be positioned after START_OBJECT.
     */
    public static byte[] serializeKeyValue(XContentParser parser) throws IOException {
        KeyValueWriter writer = KeyValueWriter.forObjectPayload();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IllegalStateException("Expected FIELD_NAME but got " + token);
            }
            String fieldName = parser.currentName();
            token = parser.nextToken(); // value token
            writeElementValue(writer, fieldName, parser, token);
        }
        return writer.toBytes();
    }

    private static PackedArray parseArray(XContentParser parser, ArrayScratch scratch, boolean floatElements) throws IOException {
        int count = 0;
        try {
            boolean forceUnion = false;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (count >= scratch.types.length) {
                    scratch.grow();
                }
                parseElement(parser, token, scratch.types, scratch.numerics, scratch.vars, count, floatElements);
                if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                    forceUnion = true;
                }
                count++;
            }
            return packAccumulatedElements(scratch.types, scratch.numerics, scratch.vars, count, forceUnion);
        } finally {
            scratch.clear(count);
        }
    }

    /** Parses the array element at {@code token} into slot {@code i} of the element buffers. */
    private static void parseElement(
        XContentParser parser,
        XContentParser.Token token,
        byte[] elemTypes,
        long[] elemNumeric,
        Object[] elemVar,
        int i,
        boolean floatElements
    ) throws IOException {
        switch (token) {
            case START_OBJECT -> {
                elemTypes[i] = SourceValueType.KEY_VALUE;
                elemVar[i] = serializeKeyValue(parser);
            }
            case START_ARRAY -> {
                PackedArray nested = parseArray(parser, new ArrayScratch(), false);
                elemTypes[i] = nested.arrayType();
                elemVar[i] = nested.packed();
            }
            case VALUE_STRING -> {
                elemTypes[i] = SourceValueType.STRING;
                elemVar[i] = parser.optimizedText().bytes();
            }
            case VALUE_NUMBER -> {
                if (floatElements) {
                    elemTypes[i] = SourceValueType.FLOAT;
                    elemNumeric[i] = Float.floatToRawIntBits(parser.floatValue());
                    return;
                }
                XContentParser.NumberType numType = parser.numberType();
                switch (numType) {
                    case INT -> {
                        elemTypes[i] = SourceValueType.INT;
                        elemNumeric[i] = parser.intValue();
                    }
                    case LONG -> {
                        elemTypes[i] = SourceValueType.LONG;
                        elemNumeric[i] = parser.longValue();
                    }
                    case FLOAT -> {
                        elemTypes[i] = SourceValueType.FLOAT;
                        elemNumeric[i] = Float.floatToRawIntBits(parser.floatValue());
                    }
                    case DOUBLE -> {
                        elemTypes[i] = SourceValueType.DOUBLE;
                        elemNumeric[i] = Double.doubleToRawLongBits(parser.doubleValue());
                    }
                    default -> {
                        elemTypes[i] = SourceValueType.STRING;
                        elemVar[i] = parser.optimizedText().bytes();
                    }
                }
            }
            case VALUE_BOOLEAN -> elemTypes[i] = parser.booleanValue() ? SourceValueType.TRUE : SourceValueType.FALSE;
            case VALUE_NULL -> elemTypes[i] = SourceValueType.NULL;
            default -> throw new IllegalStateException("Unexpected token in array: " + token);
        }
    }

    private static void writeElementValue(KeyValueWriter writer, String fieldName, XContentParser parser, XContentParser.Token token)
        throws IOException {
        switch (token) {
            case VALUE_STRING -> {
                XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                writer.writeStringField(fieldName, str.bytes(), str.offset(), str.length());
            }
            case VALUE_NUMBER -> {
                XContentParser.NumberType numType = parser.numberType();
                switch (numType) {
                    case INT -> writer.writeIntField(fieldName, parser.intValue());
                    case LONG -> writer.writeLongField(fieldName, parser.longValue());
                    case FLOAT -> writer.writeFloatField(fieldName, parser.floatValue());
                    case DOUBLE -> writer.writeDoubleField(fieldName, parser.doubleValue());
                    default -> {
                        XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                        writer.writeStringField(fieldName, str.bytes(), str.offset(), str.length());
                    }
                }
            }
            case VALUE_BOOLEAN -> writer.writeBooleanField(fieldName, parser.booleanValue());
            case VALUE_NULL -> writer.writeNullField(fieldName);
            case START_OBJECT -> {
                writer.beginObjectField(fieldName);
                XContentParser.Token inner;
                while ((inner = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (inner != XContentParser.Token.FIELD_NAME) {
                        throw new IllegalStateException("Expected FIELD_NAME but got " + inner);
                    }
                    String nestedName = parser.currentName();
                    inner = parser.nextToken();
                    writeElementValue(writer, nestedName, parser, inner);
                }
                writer.endObjectField();
            }
            case START_ARRAY -> writer.writeArrayField(fieldName, parseArray(parser, new ArrayScratch(), false));
            default -> throw new IllegalStateException("Unexpected token: " + token);
        }
    }

    private static int elemDataSize(byte type, Object varData) {
        return switch (type) {
            case SourceValueType.INT, SourceValueType.FLOAT -> 4;
            case SourceValueType.LONG, SourceValueType.DOUBLE -> 8;
            case SourceValueType.STRING -> {
                XContentString.UTF8Bytes str = (XContentString.UTF8Bytes) varData;
                yield 4 + (str != null ? str.length() : 0);
            }
            case SourceValueType.KEY_VALUE, SourceValueType.UNION_ARRAY, SourceValueType.FIXED_ARRAY -> {
                byte[] bytes = (byte[]) varData;
                yield 4 + bytes.length;
            }
            default -> 0; // NULL, TRUE, FALSE
        };
    }

    private static int writeElemData(byte[] packed, int pos, byte type, long numeric, Object var) {
        switch (type) {
            case SourceValueType.INT, SourceValueType.FLOAT -> {
                ByteUtils.writeIntLE((int) numeric, packed, pos);
                pos += 4;
            }
            case SourceValueType.LONG, SourceValueType.DOUBLE -> {
                ByteUtils.writeLongLE(numeric, packed, pos);
                pos += 8;
            }
            case SourceValueType.STRING -> {
                XContentString.UTF8Bytes str = (XContentString.UTF8Bytes) var;
                int len = str.length();
                ByteUtils.writeIntLE(len, packed, pos);
                pos += 4;
                System.arraycopy(str.bytes(), str.offset(), packed, pos, len);
                pos += len;
            }
            case SourceValueType.KEY_VALUE, SourceValueType.UNION_ARRAY, SourceValueType.FIXED_ARRAY -> {
                byte[] bytes = (byte[]) var;
                ByteUtils.writeIntLE(bytes.length, packed, pos);
                pos += 4;
                System.arraycopy(bytes, 0, packed, pos, bytes.length);
                pos += bytes.length;
            }
        }
        return pos;
    }
}
