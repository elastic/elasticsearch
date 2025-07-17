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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.AbstractCollection;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class ESONSource {

    public static class Builder {

        private final RecyclerBytesStreamOutput bytes;
        private final boolean ordered;

        public Builder(boolean ordered) {
            this(BytesRefRecycler.NON_RECYCLING_INSTANCE, ordered);
        }

        public Builder(Recycler<BytesRef> refRecycler, boolean ordered) {
            this.bytes = new RecyclerBytesStreamOutput(refRecycler);
            this.ordered = ordered;
        }

        public ESONObject parse(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Expected START_OBJECT but got " + token);
            }

            // Create a deferred Values supplier and modifications tracker
            DeferredValuesSupplier deferredSupplier = new DeferredValuesSupplier();

            ESONObject rootObject = parseObject(parser, bytes, deferredSupplier);

            // Now populate the deferred supplier with the final bytes
            BytesReference finalBytes = bytes.bytes();
            Values values = new Values(finalBytes);
            deferredSupplier.setValues(values);

            return rootObject;
        }

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

            switch (token) {
                case VALUE_STRING -> {
                    if (parser.optimizedText(bytes) == false) {
                        bytes.seek(position);
                        writeString(bytes, parser.text());
                    }
                    return new VariableValue(Math.toIntExact(position), Math.toIntExact(bytes.position() - position), ValueType.STRING);
                }
                case VALUE_NUMBER -> {
                    return new FixedValue(Math.toIntExact(position), handleNumber(parser, bytes));
                }
                case VALUE_BOOLEAN -> {
                    bytes.writeBoolean(parser.booleanValue());
                    return new FixedValue(Math.toIntExact(position), ValueType.BOOLEAN);
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
                    return new VariableValue(Math.toIntExact(position), Math.toIntExact(bytes.position() - position), ValueType.BINARY);
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

        private static void writeByteArray(RecyclerBytesStreamOutput bytes, byte[] value, int offset, int length) {
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

    public record Mutation(Object object) implements Type {}

    public record ESONObject(Map<String, Type> map, Supplier<Values> objectValues) implements Type, Map<String, Object>, ToXContent {

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
            } else if (type instanceof Mutation mutation) {
                return mutation.object();
            }
            return convertTypeToValue(type);
        }

        @Override
        public Object put(String key, Object value) {
            Object oldValue = get(key);
            map.put(key, new Mutation(value));
            return oldValue;
        }

        @Override
        public Object remove(Object key) {
            Type type = map.remove(key);
            if (type == null) {
                return null;
            } else if (type instanceof Mutation mutation) {
                return mutation.object();
            }
            return convertTypeToValue(type);
        }

        @Override
        public void putAll(Map<? extends String, ?> m) {
            for (Entry<? extends String, ?> entry : m.entrySet()) {
                put(entry.getKey(), entry.getValue());
            }
        }

        @Override
        public void clear() {
            map.clear();
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
                        private final Iterator<String> keyIterator = map.keySet().iterator();

                        @Override
                        public boolean hasNext() {
                            return keyIterator.hasNext();
                        }

                        @Override
                        public Object next() {
                            return get(keyIterator.next());
                        }
                    };
                }

                @Override
                public int size() {
                    return map.size();
                }
            };
        }

        // TODO: test remove
        @Override
        public Set<Entry<String, Object>> entrySet() {
            return new AbstractSet<>() {
                @Override
                public Iterator<Entry<String, Object>> iterator() {
                    return new Iterator<>() {
                        private final Iterator<Map.Entry<String, Type>> mapIterator = map.entrySet().iterator();

                        @Override
                        public boolean hasNext() {
                            return mapIterator.hasNext();
                        }

                        @Override
                        public Entry<String, Object> next() {
                            Map.Entry<String, Type> mapEntry = mapIterator.next();
                            return new LazyEntry(mapEntry.getKey(), mapEntry.getValue());
                        }

                        @Override
                        public void remove() {
                            mapIterator.remove();
                        }
                    };
                }

                @Override
                public int size() {
                    return map.size();
                }

                @Override
                public boolean contains(Object o) {
                    if ((o instanceof Entry<?, ?>) == false) {
                        return false;
                    }
                    Entry<?, ?> entry = (Entry<?, ?>) o;
                    Object key = entry.getKey();
                    if ((key instanceof String) == false) {
                        return false;
                    }
                    String strKey = (String) key;
                    Object expectedValue = entry.getValue();
                    Object actualValue = ESONObject.this.get(strKey);
                    return java.util.Objects.equals(expectedValue, actualValue);
                }

                @Override
                public boolean remove(Object o) {
                    if ((o instanceof Entry<?, ?>) == false) {
                        return false;
                    }
                    Entry<?, ?> entry = (Entry<?, ?>) o;
                    Object key = entry.getKey();
                    if ((key instanceof String) == false) {
                        return false;
                    }
                    String strKey = (String) key;
                    Object expectedValue = entry.getValue();
                    Object actualValue = ESONObject.this.get(strKey);
                    if (java.util.Objects.equals(expectedValue, actualValue)) {
                        ESONObject.this.remove(strKey);
                        return true;
                    }
                    return false;
                }

            };
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            for (Entry<String, Type> entry : map.entrySet()) {
                builder.field(entry.getKey());
                switch (entry.getValue()) {
                    case ESONObject o -> o.toXContent(builder, params);
                    case ESONArray a -> a.toXContent(builder, params);
                    case FixedValue v -> v.writeToXContent(builder, objectValues.get());
                    case VariableValue v -> v.writeToXContent(builder, objectValues.get());
                    default -> throw new IllegalArgumentException("Unknown type: " + entry.getValue());
                }
            }
            return builder.endObject();
        }

        private class LazyEntry implements Entry<String, Object> {
            private final String key;
            private Object cachedValue;
            private boolean valueComputed = false;

            LazyEntry(String key, Type type) {
                this.key = key;
            }

            @Override
            public String getKey() {
                return key;
            }

            @Override
            public Object getValue() {
                if (valueComputed == false) {
                    cachedValue = ESONObject.this.get(key); // Use the object's get method to handle modifications
                    valueComputed = true;
                }
                return cachedValue;
            }

            @Override
            public Object setValue(Object value) {
                Object oldValue = ESONObject.this.put(key, value);
                cachedValue = value;
                return oldValue;
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
                return new AbstractMap.SimpleEntry<>(getKey(), getValue()).hashCode();
            }
        }

        private Object convertTypeToValue(Type type) {
            if (type == null) {
                return null;
            }
            return switch (type) {
                case ESONObject obj -> obj;
                case ESONArray arr -> arr;
                case FixedValue val -> val.getValue(objectValues.get());
                case VariableValue val -> val.getValue(objectValues.get());
                default -> throw new IllegalStateException("Unknown type: " + type);
            };
        }

        public boolean containsKey(String key) {
            return map.containsKey(key);
        }
    }

    public static class ESONArray extends AbstractList<Object> implements Type, List<Object>, ToXContent {

        private final List<Type> elements;
        private final Supplier<Values> arrayValues;

        public ESONArray(List<Type> elements, Supplier<Values> arrayValues) {
            this.elements = elements;
            this.arrayValues = arrayValues;
        }

        @Override
        public Object get(int index) {
            Type type = elements.get(index);
            if (type == null) {
                return null;
            } else if (type instanceof Mutation mutation) {
                return mutation.object();
            }

            return convertTypeToValue(type);
        }

        @Override
        public void add(int index, Object element) {
            elements.add(index, new Mutation(element));
        }

        @Override
        public Object set(int index, Object element) {
            Object oldValue = get(index);
            elements.set(index, new Mutation(element));
            return oldValue;
        }

        @Override
        public Object remove(int index) {
            Type removedType = elements.remove(index);
            if (removedType == null) {
                return null;
            } else if (removedType instanceof Mutation mutation) {
                return mutation.object();
            }
            return convertTypeToValue(removedType);

        }

        @Override
        public int size() {
            return elements.size();
        }

        private Object convertTypeToValue(Type type) {
            return switch (type) {
                case ESONObject o -> o;
                case ESONArray a -> a;
                case FixedValue v -> v.getValue(arrayValues.get());
                case VariableValue v -> v.getValue(arrayValues.get());
                default -> throw new IllegalArgumentException("Unknown type: " + type);
            };
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray();
            for (Object element : this) {
                if (element instanceof ToXContent toXContent) {
                    toXContent.toXContent(builder, params);
                } else {
                    builder.value(element);
                }
            }
            return builder.endArray();
        }
    }

    public record FixedValue(int position, ValueType valueType) implements Type {

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

        public void writeToXContent(XContentBuilder builder, Values values) throws IOException {
            builder.value(getValue(values));
        }
    }

    public record VariableValue(int position, int length, ValueType valueType) implements Type {

        public Object getValue(Values source) {
            return switch (valueType) {
                case STRING -> source.readString(position, length);
                case BINARY -> source.readByteArray(position, length);
                default -> throw new IllegalArgumentException("Invalid value type: " + valueType);
            };
        }

        public void writeToXContent(XContentBuilder builder, Values values) throws IOException {
            byte[] bytes;
            int offset;
            if (values.data().hasArray()) {
                BytesRef bytesRef = values.data().toBytesRef();
                bytes = bytesRef.bytes;
                offset = bytesRef.offset + position;
            } else {
                bytes = values.readByteArray(position, length);
                offset = 0;
            }
            switch (valueType) {
                case STRING -> builder.utf8Value(bytes, offset, length);
                case BINARY -> builder.value(bytes, offset, length);
                default -> throw new IllegalArgumentException("Invalid value type: " + valueType);
            }
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
