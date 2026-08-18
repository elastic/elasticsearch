/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Shared utilities for the inline binary array and key-value formats used by both
 * EIRF and ESCF batch encoders.
 * TODO: Delete EIRF from Javadoc once EIRF goes away
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
        return parseArray(parser);
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
        // TODO: Eventually expose a recycler here and use a recycling instance
        BytesStreamOutput out = new BytesStreamOutput(64);
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IllegalStateException("Expected FIELD_NAME but got " + token);
            }
            byte[] keyBytes = parser.currentName().getBytes(StandardCharsets.UTF_8);
            token = parser.nextToken(); // value token

            // key_length(i32) + key_bytes
            out.writeIntLE(keyBytes.length);
            out.writeBytes(keyBytes, 0, keyBytes.length);

            // type(1) + value_data
            writeElementValue(out, parser, token);
        }
        return BytesReference.toBytes(out.bytes());
    }

    private static PackedArray parseArray(XContentParser parser) throws IOException {
        byte[] elemTypes = new byte[16];
        long[] elemNumeric = new long[16];
        Object[] elemVar = new Object[16];
        int count = 0;
        boolean forceUnion = false;

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (count >= elemTypes.length) {
                int newCap = elemTypes.length * 2;
                elemTypes = Arrays.copyOf(elemTypes, newCap);
                elemNumeric = Arrays.copyOf(elemNumeric, newCap);
                elemVar = Arrays.copyOf(elemVar, newCap);
            }
            switch (token) {
                case START_OBJECT -> {
                    elemTypes[count] = SourceValueType.KEY_VALUE;
                    elemVar[count] = serializeKeyValue(parser);
                    forceUnion = true;
                }
                case START_ARRAY -> {
                    PackedArray nested = parseArray(parser);
                    elemTypes[count] = nested.arrayType();
                    elemVar[count] = nested.packed();
                    forceUnion = true;
                }
                case VALUE_STRING -> {
                    elemTypes[count] = SourceValueType.STRING;
                    elemVar[count] = parser.optimizedText().bytes();
                }
                case VALUE_NUMBER -> {
                    XContentParser.NumberType numType = parser.numberType();
                    switch (numType) {
                        case INT, LONG -> {
                            long val = parser.longValue();
                            if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                                elemTypes[count] = SourceValueType.INT;
                                elemNumeric[count] = val;
                            } else {
                                elemTypes[count] = SourceValueType.LONG;
                                elemNumeric[count] = val;
                            }
                        }
                        case FLOAT, DOUBLE -> {
                            double val = parser.doubleValue();
                            float fval = (float) val;
                            if ((double) fval == val) {
                                elemTypes[count] = SourceValueType.FLOAT;
                                elemNumeric[count] = Float.floatToRawIntBits(fval);
                            } else {
                                elemTypes[count] = SourceValueType.DOUBLE;
                                elemNumeric[count] = Double.doubleToRawLongBits(val);
                            }
                        }
                        default -> {
                            elemTypes[count] = SourceValueType.STRING;
                            elemVar[count] = parser.optimizedText().bytes();
                        }
                    }
                }
                case VALUE_BOOLEAN -> elemTypes[count] = parser.booleanValue() ? SourceValueType.TRUE : SourceValueType.FALSE;
                case VALUE_NULL -> elemTypes[count] = SourceValueType.NULL;
                default -> throw new IllegalStateException("Unexpected token in array: " + token);
            }
            count++;
        }

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

    private static void writeElementValue(BytesStreamOutput out, XContentParser parser, XContentParser.Token token) throws IOException {
        switch (token) {
            case VALUE_STRING -> {
                XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                out.writeByte(SourceValueType.STRING);
                out.writeIntLE(str.length());
                out.writeBytes(str.bytes(), str.offset(), str.length());
            }
            case VALUE_NUMBER -> {
                XContentParser.NumberType numType = parser.numberType();
                switch (numType) {
                    case INT, LONG -> {
                        long val = parser.longValue();
                        if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                            out.writeByte(SourceValueType.INT);
                            out.writeIntLE((int) val);
                        } else {
                            out.writeByte(SourceValueType.LONG);
                            out.writeLongLE(val);
                        }
                    }
                    case FLOAT, DOUBLE -> {
                        double val = parser.doubleValue();
                        float fval = (float) val;
                        if ((double) fval == val) {
                            out.writeByte(SourceValueType.FLOAT);
                            out.writeIntLE(Float.floatToRawIntBits(fval));
                        } else {
                            out.writeByte(SourceValueType.DOUBLE);
                            out.writeLongLE(Double.doubleToRawLongBits(val));
                        }
                    }
                    default -> {
                        XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                        out.writeByte(SourceValueType.STRING);
                        out.writeIntLE(str.length());
                        out.writeBytes(str.bytes(), str.offset(), str.length());
                    }
                }
            }
            case VALUE_BOOLEAN -> out.writeByte(parser.booleanValue() ? SourceValueType.TRUE : SourceValueType.FALSE);
            case VALUE_NULL -> out.writeByte(SourceValueType.NULL);
            case START_OBJECT -> {
                byte[] nested = serializeKeyValue(parser);
                out.writeByte(SourceValueType.KEY_VALUE);
                out.writeIntLE(nested.length);
                out.writeBytes(nested, 0, nested.length);
            }
            case START_ARRAY -> {
                PackedArray arr = parseArray(parser);
                out.writeByte(arr.arrayType());
                out.writeIntLE(arr.packed().length);
                out.writeBytes(arr.packed(), 0, arr.packed().length);
            }
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
