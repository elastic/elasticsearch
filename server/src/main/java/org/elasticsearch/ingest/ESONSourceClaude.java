/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ESONSourceClaude {

    // Cache for UTF-8 to UTF-16 string conversion
    private static final Map<String, String> STRING_CACHE = new ConcurrentHashMap<>();

    public static class Builder {

        private final BytesStreamOutput bytes = new BytesStreamOutput();

        public ESONObject parse(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Expected START_OBJECT but got " + token);
            }
            ESONObject result = (ESONObject) parseObject(parser);
            return result;
        }

        private Type parseObject(XContentParser parser) throws IOException {
            Map<String, Type> map = new HashMap<>();
            XContentParser.Token token;
            String currentFieldName = null;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                switch (token) {
                    case FIELD_NAME:
                        currentFieldName = parser.currentName();
                        break;
                    case START_OBJECT:
                        map.put(currentFieldName, parseObject(parser));
                        break;
                    case START_ARRAY:
                        map.put(currentFieldName, parseArray(parser));
                        break;
                    case VALUE_NULL:
                        map.put(currentFieldName, new NullValue());
                        break;
                    default:
                        if (token.isValue()) {
                            map.put(currentFieldName, parseValue(parser, token));
                        }
                }
            }
            return new ESONObject(map);
        }

        private ESONArray parseArray(XContentParser parser) throws IOException {
            List<Type> elements = new ArrayList<>();
            XContentParser.Token token;

            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                switch (token) {
                    case START_OBJECT:
                        elements.add(parseObject(parser));
                        break;
                    case START_ARRAY:
                        elements.add(parseArray(parser));
                        break;
                    case VALUE_NULL:
                        elements.add(new NullValue());
                        break;
                    default:
                        if (token.isValue()) {
                            elements.add(parseValue(parser, token));
                        }
                }
            }
            return new ESONArray(elements);
        }

        private Value parseValue(XContentParser parser, XContentParser.Token token) throws IOException {
            long position = bytes.position();
            ValueType valueType;

            switch (token) {
                case VALUE_NUMBER -> {
                    valueType = handleNumber(parser);
                }
                case VALUE_STRING -> {
                    XContentString xContentString = parser.optimizedText();
                    XContentString.UTF8Bytes utf8Bytes = xContentString.bytes();
                    writeByteArray(utf8Bytes.bytes(), utf8Bytes.offset(), utf8Bytes.length());
                    valueType = ValueType.STRING;
                }
                case VALUE_BOOLEAN -> {
                    boolean boolValue = parser.booleanValue();
                    bytes.writeByte(boolValue ? (byte) 1 : (byte) 0);
                    valueType = ValueType.BOOLEAN;
                }
                case VALUE_EMBEDDED_OBJECT -> {
                    byte[] binaryValue = parser.binaryValue();
                    writeByteArray(binaryValue, 0, binaryValue.length);
                    valueType = ValueType.BINARY;
                }
                default -> throw new IllegalStateException("Unexpected token [" + token + "]");
            }

            return new Value(Math.toIntExact(position), valueType);
        }

        private ValueType handleNumber(XContentParser parser) throws IOException {
            switch (parser.numberType()) {
                case INT -> {
                    int value = parser.intValue();
                    bytes.writeInt(value);
                    return ValueType.INT;
                }
                case LONG -> {
                    long value = parser.longValue();
                    bytes.writeLong(value);
                    return ValueType.LONG;
                }
                case FLOAT -> {
                    float value = parser.floatValue();
                    bytes.writeFloat(value);
                    return ValueType.FLOAT;
                }
                case DOUBLE -> {
                    double value = parser.doubleValue();
                    bytes.writeDouble(value);
                    return ValueType.DOUBLE;
                }
                case BIG_INTEGER -> {
                    // Convert to long or store as string for very large values
                    try {
                        long value = parser.longValue();
                        bytes.writeLong(value);
                        return ValueType.LONG;
                    } catch (NumberFormatException e) {
                        // Store as string if too large for long
                        String stringValue = parser.text();
                        writeString(stringValue);
                        return ValueType.STRING;
                    }
                }
                case BIG_DECIMAL -> {
                    // Convert to double or store as string for high precision
                    try {
                        double value = parser.doubleValue();
                        bytes.writeDouble(value);
                        return ValueType.DOUBLE;
                    } catch (NumberFormatException e) {
                        String stringValue = parser.text();
                        writeString(stringValue);
                        return ValueType.STRING;
                    }
                }
                default -> throw new IllegalStateException("Unexpected number type: " + parser.numberType());
            }
        }

        private void writeByteArray(byte[] value, int offset, int length) throws IOException {
            // Write length first, then bytes
            bytes.writeInt(length);
            bytes.writeBytes(value, offset, length);
        }

        private void writeString(String value) throws IOException {
            byte[] utf8Bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            writeByteArray(utf8Bytes, 0, utf8Bytes.length);
        }

        public ESONSourceClaude build() {
            return new ESONSourceClaude(bytes.bytes());
        }
    }

    private final BytesReference data;

    private ESONSourceClaude(BytesReference data) {
        this.data = data;
    }

    public int readInt(int position) {
        return data.getInt(position);
    }

    public long readLong(int position) {
        // TODO: Not LE
        return data.getLongLE(position);
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

    public byte[] readByteArray(int position) {
        int length = data.getInt(position);
        byte[] result = new byte[length];
        for (int i = 0; i < length; i++) {
            result[i] = data.get(position + 4 + i);
        }
        return result;
    }

    public String readString(int position) {
        byte[] utf8Bytes = readByteArray(position);
        String utf8String = new String(utf8Bytes, java.nio.charset.StandardCharsets.UTF_8);
        // Use cache for frequently accessed strings
        // TODO: No. Not for values.
        return STRING_CACHE.computeIfAbsent(utf8String, k -> k);
    }

    public enum ValueType {
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        BOOLEAN,
        STRING,
        BINARY
    }

    public interface Type {}

    public record ESONObject(Map<String, Type> map) implements Type {

        public Type get(String key) {
                return map.get(key);
            }

            public boolean containsKey(String key) {
                return map.containsKey(key);
            }
        }

    public record ESONArray(List<Type> elements) implements Type {

        public Type get(int index) {
                return elements.get(index);
            }

            public int size() {
                return elements.size();
            }
        }

    public record Value(int position, ValueType valueType) implements Type {

        // Convenience methods to read values
            public Object getValue(ESONSourceClaude source) {
                return switch (valueType) {
                    case INT -> source.readInt(position);
                    case LONG -> source.readLong(position);
                    case FLOAT -> source.readFloat(position);
                    case DOUBLE -> source.readDouble(position);
                    case BOOLEAN -> source.readBoolean(position);
                    case STRING -> source.readString(position);
                    case BINARY -> source.readByteArray(position);
                };
            }
        }

    public static class NullValue implements Type {}
}
