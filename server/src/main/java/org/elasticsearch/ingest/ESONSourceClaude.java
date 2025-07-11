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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class ESONSourceClaude {

    public static final NullValue NULL_VALUE = new NullValue();

    public static class Builder {

        private final BytesStreamOutput bytes = new BytesStreamOutput();
        // TODO: Implement key cache when makes sense
        private final Map<BytesRef, String> keyCache;
        // TODO: Implement ordered
        private final boolean ordered;

        public Builder(boolean ordered) {
            this(new HashMap<>(), ordered);
        }

        public Builder(Map<BytesRef, String> keyCache, boolean ordered) {
            this.keyCache = keyCache;
            this.ordered = ordered;
        }

        public ESONSourceClaude parse(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Expected START_OBJECT but got " + token);
            }

            // Parse into intermediate structure
            ParsedObject rootObject = parseObject(parser);

            // Build final structure with access to bytes
            BytesReference finalBytes = bytes.bytes();
            Values values = new Values(finalBytes);

            ESONObject finalRoot = buildESONObject(rootObject, values);
            return new ESONSourceClaude(finalRoot, finalBytes);
        }

        // Intermediate parsing structures (no access to final bytes yet)
        private ParsedObject parseObject(XContentParser parser) throws IOException {
            Map<String, ParsedType> map = new HashMap<>();
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
                        map.put(currentFieldName, NULL_VALUE);
                        break;
                    default:
                        if (token.isValue()) {
                            map.put(currentFieldName, parseValue(parser, token));
                        }
                }
            }
            return new ParsedObject(map);
        }

        private ParsedArray parseArray(XContentParser parser) throws IOException {
            List<ParsedType> elements = new ArrayList<>();
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
                        elements.add(NULL_VALUE);
                        break;
                    default:
                        if (token.isValue()) {
                            elements.add(parseValue(parser, token));
                        }
                }
            }
            return new ParsedArray(elements);
        }

        private ParsedValue parseValue(XContentParser parser, XContentParser.Token token) throws IOException {
            long position = bytes.position();
            ValueType valueType;

            switch (token) {
                case VALUE_NUMBER -> valueType = handleNumber(parser);
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

            return new ParsedValue(Math.toIntExact(position), valueType);
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

        // Convert intermediate structures to final structures with Values access
        private ESONObject buildESONObject(ParsedObject parsed, Values values) {
            Map<String, Type> finalMap = new HashMap<>();
            for (Map.Entry<String, ParsedType> entry : parsed.map.entrySet()) {
                finalMap.put(entry.getKey(), buildType(entry.getValue(), values));
            }
            return new ESONObject(finalMap, () -> values);
        }

        private ESONArray buildESONArray(ParsedArray parsed, Values values) {
            List<Type> finalElements = new ArrayList<>();
            for (ParsedType element : parsed.elements) {
                finalElements.add(buildType(element, values));
            }
            return new ESONArray(finalElements);
        }

        private Type buildType(ParsedType parsed, Values values) {
            return switch (parsed) {
                case ParsedObject obj -> buildESONObject(obj, values);
                case ParsedArray arr -> buildESONArray(arr, values);
                case ParsedValue val -> new Value(val.position, val.valueType);
                case NullValue nullVal -> nullVal;
                default -> throw new IllegalStateException("Unknown parsed type: " + parsed);
            };
        }
    }

    // Intermediate parsing types (no Values access)
    private interface ParsedType {}

    private record ParsedObject(Map<String, ParsedType> map) implements ParsedType {}

    private record ParsedArray(List<ParsedType> elements) implements ParsedType {}

    private record ParsedValue(int position, ValueType valueType) implements ParsedType {}

    private final ESONObject rootObject;
    private final BytesReference data;

    private ESONSourceClaude(ESONObject rootObject, BytesReference data) {
        this.rootObject = rootObject;
        this.data = data;
    }

    public ESONObject root() {
        return rootObject;
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

    public record ESONObject(Map<String, Type> map, Supplier<Values> valuesSupplier) implements Type, Map<String, Object> {

        // Map interface implementation
        @Override
        public int size() {
            return map.size();
        }

        @Override
        public boolean isEmpty() {
            return map.isEmpty();
        }

        @Override
        public boolean containsKey(Object key) {
            return map.containsKey(key);
        }

        @Override
        public boolean containsValue(Object value) {
            // This would be expensive to implement efficiently
            throw new UnsupportedOperationException("containsValue not supported");
        }

        @Override
        public Object get(Object key) {
            Type type = map.get(key);
            if (type == null) {
                return null;
            }
            return convertTypeToValue(type);
        }

        @Override
        public Object put(String key, Object value) {
            throw new UnsupportedOperationException("ESONObject is read-only");
        }

        @Override
        public Object remove(Object key) {
            throw new UnsupportedOperationException("ESONObject is read-only");
        }

        @Override
        public void putAll(Map<? extends String, ?> m) {
            throw new UnsupportedOperationException("ESONObject is read-only");
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException("ESONObject is read-only");
        }

        @Override
        public Set<String> keySet() {
            return map.keySet();
        }

        @Override
        public Collection<Object> values() {
            return map.values().stream().map(this::convertTypeToValue).toList();
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return map.entrySet()
                .stream()
                .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, entry -> convertTypeToValue(entry.getValue())))
                .entrySet();
        }

        private Object convertTypeToValue(Type type) {
            return switch (type) {
                case ESONObject obj -> obj;
                case ESONArray arr -> arr;
                case NullValue nullVal -> null;
                case Value val -> val.getValue(valuesSupplier.get());
                default -> throw new IllegalStateException("Unknown type: " + type);
            };
        }

        // Direct access methods for better performance
        public Type getType(String key) {
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
        public Object getValue(Values source) {
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

    public static class NullValue implements Type, ParsedType {}

    record Values(BytesReference data) {

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
            return new String(utf8Bytes, java.nio.charset.StandardCharsets.UTF_8);
        }
    }
}
