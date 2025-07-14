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
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;

import java.util.AbstractCollection;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class ESONSource {

    private static final BytesRef BYTES_REF = new BytesRef(new byte[16384]);

    public static class Builder {

        // private final RecyclerBytesStreamOutput bytes = new RecyclerBytesStreamOutput(new Recycler<>() {
        // @Override
        // public V<BytesRef> obtain() {
        // return new V<>() {
        // @Override
        // public BytesRef v() {
        // return BYTES_REF;
        // }
        //
        // @Override
        // public boolean isRecycled() {
        // return true;
        // }
        //
        // @Override
        // public void close() {}
        // };
        // }
        //
        // @Override
        // public int pageSize() {
        // return 16384;
        // }
        // });
        private final RecyclerBytesStreamOutput bytes = new RecyclerBytesStreamOutput(BytesRefRecycler.NON_RECYCLING_INSTANCE);
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

        public ESONSource parse(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Expected START_OBJECT but got " + token);
            }

            // Create a deferred Values supplier that will be populated after parsing
            DeferredValuesSupplier deferredSupplier = new DeferredValuesSupplier();

            ESONObject rootObject = parseObject(parser, bytes, deferredSupplier);

            // Now populate the deferred supplier with the final bytes
            BytesReference finalBytes = bytes.bytes();
            Values values = new Values(finalBytes);
            deferredSupplier.setValues(values);

            return new ESONSource(rootObject);
        }

        // Parse directly into final structures using deferred Values
        private static ESONObject parseObject(XContentParser parser, RecyclerBytesStreamOutput bytes, Supplier<Values> valuesSupplier)
            throws IOException {
            Map<String, Type> map = new HashMap<>();
            String currentFieldName;

            while ((currentFieldName = parser.nextFieldName()) != null) {
                XContentParser.Token token = parser.nextToken();
                map.put(currentFieldName, parseValue(parser, bytes, valuesSupplier, token));
            }
            return new ESONObject(map, valuesSupplier);
        }

        private static ESONArray parseArray(XContentParser parser, RecyclerBytesStreamOutput bytes, Supplier<Values> valuesSupplier)
            throws IOException {
            List<Type> elements = new ArrayList<>();
            XContentParser.Token token;

            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                elements.add(parseValue(parser, bytes, valuesSupplier, token));
            }
            return new ESONArray(elements, valuesSupplier);
        }

        private static Type parseValue(
            XContentParser parser,
            RecyclerBytesStreamOutput bytes,
            Supplier<Values> valuesSupplier,
            XContentParser.Token token
        ) throws IOException {
            long position = bytes.position();
            Type type;

            switch (token) {
                case VALUE_STRING -> {
                    if (parser.optimizedText(bytes) == false) {
                        bytes.seek(position);
                        writeString(bytes, parser.text());
                    }
                    return new VValue(Math.toIntExact(position), Math.toIntExact(bytes.position() - position), ValueType.STRING);
                }
                case VALUE_NUMBER -> {
                    return new Value(Math.toIntExact(position), handleNumber(parser, bytes));
                }
                case VALUE_BOOLEAN -> {
                    bytes.writeBoolean(parser.booleanValue());
                    return new Value(Math.toIntExact(position), ValueType.BOOLEAN);
                }
                case VALUE_NULL -> {
                    return null;
                }
                case START_OBJECT -> {
                    return parseObject(parser, bytes, valuesSupplier);
                }
                case START_ARRAY -> {
                    return parseArray(parser, bytes, valuesSupplier);
                }
                case VALUE_EMBEDDED_OBJECT -> {
                    byte[] binaryValue = parser.binaryValue();
                    bytes.writeBytes(binaryValue, 0, binaryValue.length);
                    return new VValue(Math.toIntExact(position), Math.toIntExact(bytes.position() - position), ValueType.BINARY);
                }
                default -> throw new IllegalStateException("Unexpected token [" + token + "]");
            }
        }

        private static ValueType handleNumber(XContentParser parser, RecyclerBytesStreamOutput bytes) throws IOException {
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
                // TODO: Fix
                case BIG_INTEGER -> {
                    try {
                        long value = parser.longValue();
                        bytes.writeLong(value);
                        return ValueType.LONG;
                    } catch (NumberFormatException e) {
                        String stringValue = parser.text();
                        writeString(bytes, stringValue);
                        return ValueType.STRING;
                    }
                }
                // TODO: Fix
                case BIG_DECIMAL -> {
                    try {
                        double value = parser.doubleValue();
                        bytes.writeDouble(value);
                        return ValueType.DOUBLE;
                    } catch (NumberFormatException e) {
                        String stringValue = parser.text();
                        writeString(bytes, stringValue);
                        return ValueType.STRING;
                    }
                }
                default -> throw new IllegalStateException("Unexpected number type: " + parser.numberType());
            }
        }

        private static void writeByteArray(RecyclerBytesStreamOutput bytes, byte[] value, int offset, int length) throws IOException {
            bytes.writeBytes(value, offset, length);
        }

        private static void writeString(RecyclerBytesStreamOutput bytes, String value) throws IOException {
            byte[] utf8Bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            writeByteArray(bytes, utf8Bytes, 0, utf8Bytes.length);
        }
    }

    // Helper class to defer Values creation until after parsing
    private static class DeferredValuesSupplier implements Supplier<Values> {
        private Values values;

        void setValues(Values values) {
            this.values = values;
        }

        @Override
        public Values get() {
            if (values == null) {
                throw new IllegalStateException("Values not yet available - parsing not complete");
            }
            return values;
        }
    }

    private final ESONObject rootObject;

    private ESONSource(ESONObject rootObject) {
        this.rootObject = rootObject;
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

    public record ESONObject(Map<String, Type> map, Supplier<Values> objectValues) implements Type, Map<String, Object> {

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
            throw new UnsupportedOperationException("containsValue not supported");
        }

        @Override
        public Object get(Object key) {
            Type type = map.get(key);
            if (type == null) {
                return null;
            }
            return convertTypeToValue(type, objectValues);
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
            return new AbstractCollection<>() {
                @Override
                public Iterator<Object> iterator() {
                    return new Iterator<>() {
                        private final Iterator<Type> typeIterator = map.values().iterator();

                        @Override
                        public boolean hasNext() {
                            return typeIterator.hasNext();
                        }

                        @Override
                        public Object next() {
                            return convertTypeToValue(typeIterator.next(), objectValues);
                        }
                    };
                }

                @Override
                public int size() {
                    return map.size();
                }
            };
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return map.entrySet()
                .stream()
                .map(entry -> new LazyEntry(entry.getKey(), entry.getValue()))
                .collect(java.util.stream.Collectors.toSet());
        }

        private class LazyEntry implements Entry<String, Object> {
            private final String key;
            private final Type type;
            private Object cachedValue;
            private boolean valueComputed = false;

            LazyEntry(String key, Type type) {
                this.key = key;
                this.type = type;
            }

            @Override
            public String getKey() {
                return key;
            }

            @Override
            public Object getValue() {
                if (valueComputed == false) {
                    cachedValue = convertTypeToValue(type, objectValues);
                    valueComputed = true;
                }
                return cachedValue;
            }

            @Override
            public Object setValue(Object value) {
                throw new UnsupportedOperationException("ESONObject is read-only");
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) return true;
                if (obj instanceof Entry<?, ?> other) {
                    return java.util.Objects.equals(getKey(), other.getKey()) && java.util.Objects.equals(getValue(), other.getValue());
                }
                return false;
            }

            @Override
            public int hashCode() {
                return java.util.Objects.hash(getKey(), getValue());
            }
        }

        public boolean containsKey(String key) {
            return map.containsKey(key);
        }

    }

    private static Object convertTypeToValue(Type type, Supplier<Values> values) {
        if (type == null) {
            return null;
        } else {
            return switch (type) {
                case ESONObject o -> o;
                case ESONArray a -> a;
                case Value v -> v.getValue(values.get());
                case VValue v -> v.getValue(values.get());
                default -> throw new IllegalArgumentException("Unknown type: " + type);
            };
        }
    }

    public static class ESONArray extends AbstractList<Object> implements Type, List<Object> {

        private final List<Type> elements;
        private final Supplier<Values> arrayValues;

        public ESONArray(List<Type> elements, Supplier<Values> arrayValues) {
            this.elements = elements;
            this.arrayValues = arrayValues;
        }

        @Override
        public Object get(int index) {
            Type type = elements.get(index);
            return convertTypeToValue(type, arrayValues);
        }

        @Override
        public int size() {
            return elements.size();
        }
    }

    public record Value(int position, ValueType valueType) implements Type {

        public Object getValue(Values source) {
            return switch (valueType) {
                case INT -> source.readInt(position);
                case LONG -> source.readLong(position);
                case FLOAT -> source.readFloat(position);
                case DOUBLE -> source.readDouble(position);
                case BOOLEAN -> source.readBoolean(position);
                default -> throw new IllegalArgumentException("Invalid value type: " + valueType);
            };
        }
    }

    public record VValue(int position, int length, ValueType valueType) implements Type {

        public Object getValue(Values source) {
            return switch (valueType) {
                case STRING -> source.readString(position, length);
                case BINARY -> source.readByteArray(position, length);
                default -> throw new IllegalArgumentException("Invalid value type: " + valueType);
            };
        }
    }

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

        private byte[] readByteArray(int position, int length) {
            byte[] result = new byte[length];
            for (int i = 0; i < length; i++) {
                result[i] = data.get(position + i);
            }
            return result;
        }

        public String readString(int position, int length) {
            return new String(readByteArray(position, length), java.nio.charset.StandardCharsets.UTF_8);
        }
    }
}
