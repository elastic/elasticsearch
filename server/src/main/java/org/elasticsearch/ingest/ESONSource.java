/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentString;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ESONSource {

    public static class Builder {
        private final Recycler<BytesRef> refRecycler;
        private final List<ESONEntry> keyArray;

        public Builder() {
            this(0);
        }

        public Builder(int expectedSize) {
            this(ESONFlat.getBytesRefRecycler(), expectedSize);
        }

        public Builder(Recycler<BytesRef> refRecycler, int expectedSize) {
            this.refRecycler = refRecycler;
            this.keyArray = new ArrayList<>();
        }

        public ESONIndexed.ESONObject parse(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Expected START_OBJECT but got " + token);
            }

            try (RecyclerBytesStreamOutput bytes = new RecyclerBytesStreamOutput(refRecycler)) {
                parseObject(parser, bytes, keyArray, null);
                ByteArrayOutputStream bao = new ByteArrayOutputStream(bytes.size());
                bytes.bytes().writeTo(bao);
                return new ESONIndexed.ESONObject(0, new ESONFlat(keyArray, new Values(new BytesArray(bao.toByteArray()))));
            }

        }

        private static void parseObject(
            XContentParser parser,
            RecyclerBytesStreamOutput bytes,
            List<ESONEntry> keyArray,
            String objectFieldName
        ) throws IOException {
            ESONEntry.ObjectEntry objEntry = new ESONEntry.ObjectEntry(objectFieldName);
            keyArray.add(objEntry);

            int count = 0;
            String fieldName;
            while ((fieldName = parser.nextFieldName()) != null) {
                parseValue(parser, fieldName, bytes, keyArray);
                count++;
            }

            objEntry.offsetOrCount(count);
        }

        private static void parseArray(
            XContentParser parser,
            RecyclerBytesStreamOutput bytes,
            List<ESONEntry> keyArray,
            String arrayFieldName
        ) throws IOException {
            ESONEntry.ArrayEntry arrEntry = new ESONEntry.ArrayEntry(arrayFieldName);
            keyArray.add(arrEntry);

            int count = 0;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                switch (token) {
                    case START_OBJECT -> parseObject(parser, bytes, keyArray, null);
                    case START_ARRAY -> parseArray(parser, bytes, keyArray, null);
                    default -> {
                        Value type = parseSimpleValue(parser, bytes, token);
                        keyArray.add(new ESONEntry.FieldEntry(null, type));
                    }
                }
                count++;
            }

            arrEntry.offsetOrCount(count);
        }

        private static void parseValue(XContentParser parser, String fieldName, RecyclerBytesStreamOutput bytes, List<ESONEntry> keyArray)
            throws IOException {
            XContentParser.Token token = parser.nextToken();

            switch (token) {
                case START_OBJECT -> parseObject(parser, bytes, keyArray, fieldName);
                case START_ARRAY -> parseArray(parser, bytes, keyArray, fieldName);
                default -> {
                    Value type = parseSimpleValue(parser, bytes, token);
                    keyArray.add(new ESONEntry.FieldEntry(fieldName, type));
                }
            }
        }

        private static Value parseSimpleValue(XContentParser parser, RecyclerBytesStreamOutput bytes, XContentParser.Token token)
            throws IOException {
            long position = bytes.position();

            return switch (token) {
                case VALUE_STRING -> {
                    XContentString.UTF8Bytes stringBytes = parser.optimizedText().bytes();
                    bytes.writeVInt(stringBytes.length());
                    bytes.write(stringBytes.bytes(), stringBytes.offset(), stringBytes.length());
                    yield new VariableValue((int) position, ESONEntry.STRING);
                }
                case VALUE_NUMBER -> {
                    XContentParser.NumberType numberType = parser.numberType();
                    yield switch (numberType) {
                        case INT -> {
                            bytes.writeInt(parser.intValue());
                            yield new FixedValue((int) position, ESONEntry.TYPE_INT);
                        }
                        case LONG -> {
                            bytes.writeLong(parser.longValue());
                            yield new FixedValue((int) position, ESONEntry.TYPE_LONG);
                        }
                        case FLOAT -> {
                            bytes.writeFloat(parser.floatValue());
                            yield new FixedValue((int) position, ESONEntry.TYPE_FLOAT);
                        }
                        case DOUBLE -> {
                            bytes.writeDouble(parser.doubleValue());
                            yield new FixedValue((int) position, ESONEntry.TYPE_DOUBLE);
                        }
                        case BIG_INTEGER, BIG_DECIMAL -> {
                            byte type = numberType == XContentParser.NumberType.BIG_INTEGER ? ESONEntry.BIG_INTEGER : ESONEntry.BIG_DECIMAL;
                            byte[] numberBytes = parser.text().getBytes(StandardCharsets.UTF_8);
                            bytes.writeVInt(numberBytes.length);
                            bytes.write(numberBytes);
                            yield new VariableValue((int) position, type);
                        }
                    };
                }
                case VALUE_BOOLEAN -> parser.booleanValue() ? ConstantValue.TRUE : ConstantValue.FALSE;
                case VALUE_NULL -> ConstantValue.NULL;
                case VALUE_EMBEDDED_OBJECT -> {
                    byte[] binaryValue = parser.binaryValue();
                    bytes.writeVInt(binaryValue.length);
                    bytes.write(binaryValue);
                    yield new VariableValue((int) position, ESONEntry.BINARY);
                }
                default -> throw new IllegalArgumentException("Unexpected token: " + token);
            };
        }
    }

    public interface Value {
        byte type();

        // TODO: Fix
        default int offset() {
            return -1;
        }
    }

    public record Mutation(Object object) implements Value {

        @Override
        public byte type() {
            return ESONEntry.MUTATION;
        }
    }

    public enum ConstantValue implements Value {
        NULL(ESONEntry.TYPE_NULL),
        TRUE(ESONEntry.TYPE_TRUE),
        FALSE(ESONEntry.TYPE_FALSE);

        private final byte type;

        ConstantValue(byte type) {
            this.type = type;
        }

        @Override
        public byte type() {
            return type;
        }

        Object getValue() {
            return switch (this) {
                case NULL -> null;
                case TRUE -> true;
                case FALSE -> false;
            };
        }
    }

    public record FixedValue(int position, byte type) implements Value {
        public Object getValue(Values source) {
            return switch (type) {
                case ESONEntry.TYPE_INT -> source.readInt(position);
                case ESONEntry.TYPE_LONG -> source.readLong(position);
                case ESONEntry.TYPE_FLOAT -> source.readFloat(position);
                case ESONEntry.TYPE_DOUBLE -> source.readDouble(position);
                default -> throw new IllegalArgumentException("Invalid value type: " + type);
            };
        }

        public void writeToXContent(XContentBuilder builder, Values values) throws IOException {
            switch (type) {
                case ESONEntry.TYPE_INT -> builder.value(values.readInt(position));
                case ESONEntry.TYPE_LONG -> builder.value(values.readLong(position));
                case ESONEntry.TYPE_FLOAT -> builder.value(values.readFloat(position));
                case ESONEntry.TYPE_DOUBLE -> builder.value(values.readDouble(position));
                default -> throw new IllegalArgumentException("Invalid value type: " + type);
            }
        }

        @Override
        public int offset() {
            return position;
        }
    }

    public record VariableValue(int position, byte type) implements Value {

        @Override
        public int offset() {
            return position;
        }

        public Object getValue(Values source) {
            return switch (type) {
                case ESONEntry.STRING -> source.readString(position);
                case ESONEntry.BINARY -> source.readByteArray(position);
                case ESONEntry.BIG_INTEGER -> new BigInteger(source.readString(position));
                case ESONEntry.BIG_DECIMAL -> new BigDecimal(source.readString(position));
                default -> throw new IllegalArgumentException("Invalid value type: " + type);
            };
        }

        public void writeToXContent(XContentBuilder builder, Values values) throws IOException {
            BytesRef bytesRef = Values.readByteSlice(values.data, position);
            switch (type) {
                case ESONEntry.STRING -> builder.utf8Value(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                case ESONEntry.BINARY -> builder.value(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                // TODO: Improve?
                case ESONEntry.BIG_INTEGER -> builder.value(
                    new BigInteger(new String(bytesRef.bytes, bytesRef.offset, bytesRef.length, StandardCharsets.UTF_8))
                );
                case ESONEntry.BIG_DECIMAL -> builder.value(
                    new BigDecimal(new String(bytesRef.bytes, bytesRef.offset, bytesRef.length, StandardCharsets.UTF_8))
                );
                default -> throw new IllegalArgumentException("Invalid value type: " + type);
            }
        }
    }

    public record Values(BytesReference data) {

        public int readInt(int position) {
            return data.getInt(position);
        }

        public long readLong(int position) {
            long high = readInt(position) & 0xFFFFFFFFL;
            long low = readInt(position + 4) & 0xFFFFFFFFL;
            return (high << 32) | low;
        }

        public float readFloat(int position) {
            return Float.intBitsToFloat(data.getInt(position));
        }

        public double readDouble(int position) {
            return Double.longBitsToDouble(readLong(position));
        }

        public boolean readBoolean(int position) {
            return data.get(position) != 0;
        }

        private byte[] readByteArray(int position) {
            BytesRef bytesRef = readByteSlice(data, position);
            byte[] bytes = new byte[bytesRef.length];
            System.arraycopy(bytesRef.bytes, bytesRef.offset, bytes, 0, bytesRef.length);
            return bytes;
        }

        public String readString(int position) {
            BytesRef bytesRef = readByteSlice(data, position);
            return new String(bytesRef.bytes, bytesRef.offset, bytesRef.length, java.nio.charset.StandardCharsets.UTF_8);
        }

        public static BytesRef readByteSlice(BytesReference data, int position) {
            byte b = data.get(position);
            if (b >= 0) {
                return data.slice(position + 1, b).toBytesRef();
            }
            int i = b & 0x7F;
            b = data.get(position + 1);
            i |= (b & 0x7F) << 7;
            if (b >= 0) {
                return data.slice(position + 2, i).toBytesRef();
            }
            b = data.get(position + 2);
            i |= (b & 0x7F) << 14;
            if (b >= 0) {
                return data.slice(position + 3, i).toBytesRef();
            }
            b = data.get(position + 3);
            i |= (b & 0x7F) << 21;
            if (b >= 0) {
                return data.slice(position + 4, i).toBytesRef();
            }
            b = data.get(position + 4);
            i |= (b & 0x0F) << 28;
            if ((b & 0xF0) != 0) {
                throw new RuntimeException("Invalid vInt ((" + Integer.toHexString(b) + " & 0x7f) << 28) | " + Integer.toHexString(i));
            }
            return data.slice(position + 5, i).toBytesRef();
        }
    }
}
