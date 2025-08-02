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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.AbstractCollection;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ESONSource {

    public static class Builder {
        private final BytesStreamOutput bytes;
        private final List<KeyEntry> keyArray;

        public Builder() {
            this(0);
        }

        public Builder(int expectedSize) {
            this(BytesRefRecycler.NON_RECYCLING_INSTANCE, expectedSize);
        }

        public Builder(Recycler<BytesRef> refRecycler, int expectedSize) {
            this.bytes = new BytesStreamOutput(expectedSize);
            this.keyArray = new ArrayList<>();
        }

        public ESONObject parse(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Expected START_OBJECT but got " + token);
            }

            parseObject(parser, bytes, keyArray, null);

            return new ESONObject(0, keyArray, new Values(bytes.bytes()));
        }

        private static void parseObject(XContentParser parser, BytesStreamOutput bytes, List<KeyEntry> keyArray, String objectFieldName)
            throws IOException {
            ObjectEntry objEntry = new ObjectEntry(objectFieldName);
            keyArray.add(objEntry);

            int count = 0;
            String fieldName;
            while ((fieldName = parser.nextFieldName()) != null) {
                parseValue(parser, fieldName, bytes, keyArray);
                count++;
            }

            objEntry.fieldCount = count;
        }

        private static void parseArray(XContentParser parser, BytesStreamOutput bytes, List<KeyEntry> keyArray, String arrayFieldName)
            throws IOException {
            ArrayEntry arrEntry = new ArrayEntry(arrayFieldName);
            keyArray.add(arrEntry);

            int count = 0;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                switch (token) {
                    case START_OBJECT -> parseObject(parser, bytes, keyArray, null);
                    case START_ARRAY -> parseArray(parser, bytes, keyArray, null);
                    default -> {
                        Type type = parseSimpleValue(parser, bytes, token);
                        keyArray.add(new FieldEntry(null, type));
                    }
                }
                count++;
            }

            arrEntry.elementCount = count;
        }

        private static void parseValue(XContentParser parser, String fieldName, BytesStreamOutput bytes, List<KeyEntry> keyArray)
            throws IOException {
            XContentParser.Token token = parser.nextToken();

            switch (token) {
                case START_OBJECT -> parseObject(parser, bytes, keyArray, fieldName);
                case START_ARRAY -> parseArray(parser, bytes, keyArray, fieldName);
                default -> {
                    Type type = parseSimpleValue(parser, bytes, token);
                    keyArray.add(new FieldEntry(fieldName, type));
                }
            }
        }

        private static Type parseSimpleValue(XContentParser parser, BytesStreamOutput bytes, XContentParser.Token token)
            throws IOException {
            long position = bytes.position();

            return switch (token) {
                case VALUE_STRING -> {
                    XContentString.UTF8Bytes stringBytes = parser.optimizedText().bytes();
                    bytes.write(stringBytes.bytes(), stringBytes.offset(), stringBytes.length());
                    yield new VariableValue((int) position, stringBytes.length(), ValueType.STRING);
                }
                case VALUE_NUMBER -> {
                    XContentParser.NumberType numberType = parser.numberType();
                    yield switch (numberType) {
                        case INT -> {
                            bytes.writeInt(parser.intValue());
                            yield new FixedValue((int) position, ValueType.INT);
                        }
                        case LONG -> {
                            bytes.writeLong(parser.longValue());
                            yield new FixedValue((int) position, ValueType.LONG);
                        }
                        case FLOAT -> {
                            bytes.writeFloat(parser.floatValue());
                            yield new FixedValue((int) position, ValueType.FLOAT);
                        }
                        case DOUBLE -> {
                            bytes.writeDouble(parser.doubleValue());
                            yield new FixedValue((int) position, ValueType.DOUBLE);
                        }
                        case BIG_INTEGER, BIG_DECIMAL -> {
                            ValueType valueType = numberType == XContentParser.NumberType.BIG_INTEGER
                                ? ValueType.BIG_INTEGER
                                : ValueType.BIG_DECIMAL;
                            byte[] numberBytes = parser.text().getBytes(StandardCharsets.UTF_8);
                            bytes.write(numberBytes);
                            yield new VariableValue((int) position, numberBytes.length, valueType);
                        }
                    };
                }
                case VALUE_BOOLEAN -> {
                    bytes.writeBoolean(parser.booleanValue());
                    yield new FixedValue((int) position, ValueType.BOOLEAN);
                }
                case VALUE_NULL -> NullValue.INSTANCE;
                case VALUE_EMBEDDED_OBJECT -> {
                    byte[] binaryValue = parser.binaryValue();
                    bytes.write(binaryValue);
                    yield new VariableValue((int) position, binaryValue.length, ValueType.BINARY);
                }
                default -> throw new IllegalArgumentException("Unexpected token: " + token);
            };
        }
    }

    public enum ValueType {
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        BOOLEAN,
        BIG_INTEGER,
        BIG_DECIMAL,
        STRING,
        BINARY
    }

    public interface KeyEntry {

        String key();

        default void writeTo(StreamOutput out) throws IOException {

        }

    }

    public static class ObjectEntry implements KeyEntry {

        private final String key;
        public int fieldCount = 0;
        private Map<String, Type> mutationMap = null;

        public ObjectEntry(String key) {
            this.key = key;
        }

        @Override
        public String key() {
            return key;
        }

        public boolean hasMutations() {
            return mutationMap != null;
        }

        @Override
        public String toString() {
            return "ObjectEntry{" + "key='" + key + '\'' + ", fieldCount=" + fieldCount + ", hasMutations=" + hasMutations() + '}';
        }
    }

    public static class ArrayEntry implements KeyEntry {

        private final String key;
        public int elementCount = 0;
        private List<Type> mutationArray = null;

        public ArrayEntry(String key) {
            this.key = key;
        }

        @Override
        public String key() {
            return key;
        }

        public boolean hasMutations() {
            return mutationArray != null;
        }

        @Override
        public String toString() {
            return "ArrayEntry{" + "key='" + key + '\'' + ", elementCount=" + elementCount + ", hasMutations=" + hasMutations() + '}';
        }
    }

    public static class FieldEntry implements KeyEntry {
        public final String key;
        public final Type type;

        public FieldEntry(String key, Type type) {
            this.key = key;
            this.type = type;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public String toString() {
            return "FieldEntry{" + "key='" + key + '\'' + ", type=" + type + '}';
        }
    }

    public interface Type {}

    public record Mutation(Object object) implements Type {}

    public record ContainerType(int keyArrayIndex) implements Type {}

    public enum NullValue implements Type {
        INSTANCE
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
            switch (valueType) {
                case INT -> builder.value(values.readInt(position));
                case LONG -> builder.value(values.readLong(position));
                case FLOAT -> builder.value(values.readFloat(position));
                case DOUBLE -> builder.value(values.readDouble(position));
                case BOOLEAN -> builder.value(values.readBoolean(position));
                default -> throw new IllegalArgumentException("Invalid value type: " + valueType);
            }
        }
    }

    public record VariableValue(int position, int length, ValueType valueType) implements Type {
        public Object getValue(Values source) {
            return switch (valueType) {
                case STRING -> source.readString(position, length);
                case BINARY -> source.readByteArray(position, length);
                case BIG_INTEGER -> new BigInteger(source.readString(position, length));
                case BIG_DECIMAL -> new BigDecimal(source.readString(position, length));
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
                // TODO: Improve?
                case BIG_INTEGER -> builder.value(new BigInteger(new String(bytes, offset, length, StandardCharsets.UTF_8)));
                case BIG_DECIMAL -> builder.value(new BigDecimal(new String(bytes, offset, length, StandardCharsets.UTF_8)));
                default -> throw new IllegalArgumentException("Invalid value type: " + valueType);
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

    public static class ESONObject implements Type, Map<String, Object>, ToXContent {
        private final int keyArrayIndex;
        private final ObjectEntry objEntry;
        private final List<KeyEntry> keyArray;
        private final Values values;
        private Map<String, Type> materializedMap;

        public ESONObject(int keyArrayIndex, List<KeyEntry> keyArray, Values values) {
            this.keyArrayIndex = keyArrayIndex;
            this.objEntry = (ObjectEntry) keyArray.get(keyArrayIndex);
            this.keyArray = keyArray;
            this.values = values;
        }

        public List<KeyEntry> getKeyArray() {
            return keyArray;
        }

        public Values objectValues() {
            return values;
        }

        private void ensureMaterializedMap() {
            if (materializedMap == null) {
                materializedMap = new HashMap<>(objEntry.fieldCount);

                int currentIndex = keyArrayIndex + 1;
                for (int i = 0; i < objEntry.fieldCount; i++) {
                    KeyEntry entry = keyArray.get(currentIndex);
                    if (entry instanceof FieldEntry fieldEntry) {
                        materializedMap.put(fieldEntry.key, fieldEntry.type);
                        currentIndex++;
                    } else {
                        if (entry instanceof ObjectEntry) {
                            materializedMap.put(entry.key(), new ESONObject(currentIndex, keyArray, values));
                        } else {
                            materializedMap.put(entry.key(), new ESONArray(currentIndex, keyArray, values));
                        }
                        currentIndex = skipContainer(keyArray, entry, currentIndex);
                    }
                }
            }
        }

        @Override
        public int size() {
            if (materializedMap == null) {
                return objEntry.fieldCount;
            } else {
                return materializedMap.size();
            }
        }

        @Override
        public boolean isEmpty() {
            return size() == 0;
        }

        @Override
        public boolean containsKey(Object key) {
            ensureMaterializedMap();
            return materializedMap.containsKey(key);
        }

        @Override
        public boolean containsValue(Object value) {
            throw new UnsupportedOperationException("containsValue not supported");
        }

        @Override
        public Object get(Object key) {
            ensureMaterializedMap();
            Type type = materializedMap.get(key);
            if (type == null) {
                return null;
            } else if (type instanceof Mutation mutation) {
                return mutation.object();
            }
            return convertTypeToValue(type, values);
        }

        @Override
        public Object put(String key, Object value) {
            ensureMaterializedMap();
            Object oldValue = get(key);
            materializedMap.put(key, new Mutation(value));
            objEntry.mutationMap = materializedMap;
            return oldValue;
        }

        @Override
        public Object remove(Object key) {
            ensureMaterializedMap();
            Type type = materializedMap.remove(key);
            objEntry.mutationMap = materializedMap;
            if (type == null) {
                return null;
            } else if (type instanceof Mutation mutation) {
                return mutation.object();
            }
            return convertTypeToValue(type, values);
        }

        @Override
        public void putAll(Map<? extends String, ?> m) {
            for (Entry<? extends String, ?> entry : m.entrySet()) {
                put(entry.getKey(), entry.getValue());
            }
        }

        @Override
        public void clear() {
            // TODO: can probably optimize
            ensureMaterializedMap();
            materializedMap.clear();
            objEntry.mutationMap = materializedMap;
        }

        @Override
        public Set<String> keySet() {
            ensureMaterializedMap();
            return materializedMap.keySet();
        }

        @Override
        public Collection<Object> values() {
            return new AbstractCollection<>() {
                @Override
                public Iterator<Object> iterator() {
                    return new Iterator<>() {
                        private final Iterator<String> keyIterator = keySet().iterator();

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
                    return ESONObject.this.size();
                }
            };
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            ensureMaterializedMap();
            return entrySet(false);
        }

        public Set<Entry<String, Object>> entrySetNullInsteadOfRawValues() {
            if (materializedMap == null) {
                Map<String, Object> emptyMap = Collections.emptyMap();
                return emptyMap.entrySet();
            } else {
                return entrySet(true);
            }
        }

        private Set<Entry<String, Object>> entrySet(boolean nullForRawValues) {
            return new AbstractSet<>() {
                @Override
                public Iterator<Entry<String, Object>> iterator() {
                    return new Iterator<>() {
                        private final Iterator<Map.Entry<String, Type>> mapIterator = materializedMap.entrySet().iterator();

                        @Override
                        public boolean hasNext() {
                            return mapIterator.hasNext();
                        }

                        @Override
                        public Entry<String, Object> next() {
                            Map.Entry<String, Type> mapEntry = mapIterator.next();
                            return new LazyEntry(mapEntry.getKey(), mapEntry.getValue(), nullForRawValues);
                        }

                        @Override
                        public void remove() {
                            objEntry.mutationMap = materializedMap;
                            mapIterator.remove();
                        }
                    };
                }

                @Override
                public int size() {
                    return materializedMap.size();
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

        private class LazyEntry implements Entry<String, Object> {
            private final String key;
            private final Type type;
            private final boolean nullForRawValues;
            private Object cachedValue;
            private boolean valueComputed = false;

            LazyEntry(String key, Type type, boolean nullForRawValues) {
                this.key = key;
                this.type = type;
                this.nullForRawValues = nullForRawValues;
            }

            @Override
            public String getKey() {
                return key;
            }

            public boolean isRawValue() {
                return type instanceof FixedValue || type instanceof VariableValue;
            }

            @Override
            public Object getValue() {
                if (valueComputed == false) {
                    if (type == null) {
                        cachedValue = null;
                    } else if (type instanceof Mutation mutation) {
                        cachedValue = mutation.object();
                    } else {
                        if (nullForRawValues && isRawValue()) {
                            cachedValue = null;
                        } else {
                            cachedValue = convertTypeToValue(type, values);
                        }
                    }
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

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            for (Entry<String, Object> entry : entrySet()) {
                builder.field(entry.getKey());
                if (entry.getValue() instanceof ToXContent toXContent) {
                    toXContent.toXContent(builder, params);
                } else {
                    builder.value(entry.getValue());
                }
            }
            return builder.endObject();
        }

    }

    public static class ESONArray extends AbstractList<Object> implements Type, List<Object>, ToXContent {

        private final int keyArrayIndex;
        private final ArrayEntry arrEntry;
        private final List<KeyEntry> keyArray;
        private final Values values;
        private List<Type> materializedList;

        public ESONArray(int keyArrayIndex, List<KeyEntry> keyArray, Values values) {
            this.keyArrayIndex = keyArrayIndex;
            this.arrEntry = (ArrayEntry) keyArray.get(keyArrayIndex);
            this.keyArray = keyArray;
            this.values = values;
        }

        private void ensureMaterializedList() {
            if (materializedList == null) {
                materializedList = new ArrayList<>(arrEntry.elementCount);

                int currentIndex = keyArrayIndex + 1;
                for (int i = 0; i < arrEntry.elementCount; i++) {
                    KeyEntry entry = keyArray.get(currentIndex);
                    if (entry instanceof FieldEntry fieldEntry) {
                        materializedList.add(fieldEntry.type);
                        currentIndex++;
                    } else {
                        if (entry instanceof ObjectEntry) {
                            materializedList.add(new ESONObject(currentIndex, keyArray, values));
                        } else {
                            materializedList.add(new ESONArray(currentIndex, keyArray, values));
                        }
                        currentIndex = skipContainer(keyArray, entry, currentIndex);
                    }
                }
            }
        }

        @Override
        public Object get(int index) {
            // TODO: Can implement this without materializing
            ensureMaterializedList();
            Type type = materializedList.get(index);
            if (type == null) {
                return null;
            } else if (type instanceof Mutation mutation) {
                return mutation.object();
            }

            return convertTypeToValue(type, values);
        }

        @Override
        public void add(int index, Object element) {
            ensureMaterializedList();
            materializedList.add(index, new Mutation(element));
            arrEntry.mutationArray = materializedList;
        }

        @Override
        public Object set(int index, Object element) {
            ensureMaterializedList();
            Object oldValue = get(index);
            materializedList.set(index, new Mutation(element));
            arrEntry.mutationArray = materializedList;
            return oldValue;
        }

        @Override
        public Object remove(int index) {
            ensureMaterializedList();
            Object oldValue = get(index);
            materializedList.remove(index);
            arrEntry.mutationArray = materializedList;
            return oldValue;
        }

        @Override
        public boolean add(Object element) {
            ensureMaterializedList();
            boolean result = materializedList.add(new Mutation(element));
            arrEntry.mutationArray = materializedList;
            return result;
        }

        @Override
        public void clear() {
            // TODO: Can optimize
            ensureMaterializedList();
            materializedList.clear();
            arrEntry.mutationArray = materializedList;
        }

        @Override
        public int size() {
            if (materializedList == null) {
                return arrEntry.elementCount;
            } else {
                return materializedList.size();
            }
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

        public Iterator<Object> iteratorNullInsteadOfRawValues() {
            if (materializedList == null) {
                return new Iterator<Object>() {
                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public Object next() {
                        return null;
                    }
                };
            } else {
                Iterator<Type> typeIterator = materializedList.iterator();
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return typeIterator.hasNext();
                    }

                    @Override
                    public Object next() {
                        Type next = typeIterator.next();
                        if (next instanceof VariableValue || next instanceof FixedValue) {
                            return null;
                        } else {
                            return next;
                        }
                    }
                };
            }
        }
    }

    private static Object convertTypeToValue(Type type, Values values) {
        if (type == null) {
            return null;
        }
        return switch (type) {
            case ESONObject obj -> obj;
            case ESONArray arr -> arr;
            case FixedValue val -> val.getValue(values);
            case VariableValue val -> val.getValue(values);
            case NullValue nullVal -> null;
            case Mutation mutation -> mutation.object();
            default -> throw new IllegalStateException("Unknown type: " + type);
        };
    }

    private static int skipContainer(List<KeyEntry> keyArray, KeyEntry entry, int containerIndex) {
        int index = containerIndex + 1;
        final int fieldCount;
        if (entry instanceof ObjectEntry objEntry) {
            fieldCount = objEntry.fieldCount;
        } else {
            fieldCount = ((ArrayEntry) entry).elementCount;
        }

        for (int i = 0; i < fieldCount; i++) {
            KeyEntry fieldKeyEntry = keyArray.get(index);
            if (fieldKeyEntry instanceof FieldEntry) {
                index++;
            } else {
                index = skipContainer(keyArray, fieldKeyEntry, index);
            }
        }

        return index;
    }

    public static ESONObject flatten(ESONObject original) {
        List<KeyEntry> flatKeyArray = new ArrayList<>(original.getKeyArray().size());

        // Start flattening from the root object
        flattenObject(original, null, flatKeyArray);

        // Return new ESONObject with flattened structure
        return new ESONObject(0, flatKeyArray, original.objectValues());
    }

    /**
     * Recursively flattens an ESONObject into the flat key array
     */
    private static void flattenObject(ESONObject obj, String objectFieldName, List<KeyEntry> flatKeyArray) {
        // Create new ObjectEntry for this object
        ObjectEntry newObjEntry = new ObjectEntry(objectFieldName);
        flatKeyArray.add(newObjEntry);

        // Check if object has mutations
        boolean hasMutations = obj.objEntry.hasMutations();

        if (hasMutations == false) {
            // No mutations - just copy the entries directly from original key array
            int currentIndex = obj.keyArrayIndex + 1;
            int fieldCount = 0;

            for (int i = 0; i < obj.objEntry.fieldCount; i++) {
                KeyEntry entry = obj.keyArray.get(currentIndex);

                if (entry instanceof FieldEntry fieldEntry) {
                    // Copy field entry as-is
                    flatKeyArray.add(fieldEntry);
                    currentIndex++;
                    fieldCount++;
                } else if (entry instanceof ObjectEntry) {
                    // Nested object - create new ESONObject and flatten recursively
                    ESONObject nestedObj = new ESONObject(currentIndex, obj.keyArray, obj.values);
                    flattenObject(nestedObj, entry.key(), flatKeyArray);
                    // TODO: Remove Need to skip container
                    currentIndex = skipContainer(obj.keyArray, entry, currentIndex);
                    fieldCount++;
                } else if (entry instanceof ArrayEntry) {
                    // Nested array - create new ESONArray and flatten recursively
                    ESONArray nestedArr = new ESONArray(currentIndex, obj.keyArray, obj.values);
                    flattenArray(nestedArr, entry.key(), flatKeyArray);
                    // TODO: Remove Need to skip container
                    currentIndex = skipContainer(obj.keyArray, entry, currentIndex);
                    fieldCount++;
                }
            }

            newObjEntry.fieldCount = fieldCount;
        } else {
            // Has mutations - need to iterate through materialized map
            obj.ensureMaterializedMap();

            int fieldCount = 0;
            for (Map.Entry<String, Type> entry : obj.objEntry.mutationMap.entrySet()) {
                String key = entry.getKey();
                Type type = entry.getValue();

                switch (type) {
                    case Mutation mutation -> {
                        handleObject(flatKeyArray, mutation.object(), key);
                        fieldCount++;
                    }
                    case ESONObject nestedObj -> {
                        // Nested object - flatten recursively
                        flattenObject(nestedObj, key, flatKeyArray);
                        fieldCount++;
                    }
                    case ESONArray nestedArr -> {
                        // Nested array - flatten recursively
                        flattenArray(nestedArr, key, flatKeyArray);
                        fieldCount++;
                    }
                    case null, default -> {
                        // Regular type (FixedValue, VariableValue, NullValue) - create field entry
                        flatKeyArray.add(new FieldEntry(key, type));
                        fieldCount++;
                    }
                }
            }

            newObjEntry.fieldCount = fieldCount;
        }
    }

    private static void handleObject(List<KeyEntry> flatKeyArray, Object object, String key) {
        if (object instanceof Map<?, ?> map) {
            ObjectEntry objectEntry = new ObjectEntry(key);
            flatKeyArray.add(objectEntry);
            objectEntry.fieldCount = map.size();
            for (Map.Entry<?, ?> entry1 : map.entrySet()) {
                Object value = entry1.getValue();
                handleObject(flatKeyArray, value, entry1.getKey().toString());
            }
        } else if (object instanceof List<?> list) {
            ArrayEntry arrayEntry = new ArrayEntry(key);
            flatKeyArray.add(arrayEntry);
            arrayEntry.elementCount = list.size();
            for (Object value : list) {
                handleObject(flatKeyArray, value, null);
            }
        } else {
            flatKeyArray.add(new FieldEntry(key, ensureOneLevelMutation(object)));
        }
    }

    private static Mutation ensureOneLevelMutation(Object value) {
        final Mutation valueMutation;
        if (value instanceof Mutation m) {
            valueMutation = m;
        } else {
            valueMutation = new Mutation(value);
        }
        return valueMutation;
    }

    /**
     * Recursively flattens an ESONArray into the flat key array
     */
    private static void flattenArray(ESONArray arr, String arrayFieldName, List<KeyEntry> flatKeyArray) {
        // Create new ArrayEntry for this array
        ArrayEntry newArrEntry = new ArrayEntry(arrayFieldName);
        flatKeyArray.add(newArrEntry);

        // Check if array has mutations
        boolean hasMutations = arr.arrEntry.hasMutations();

        if (hasMutations == false) {
            // No mutations - just copy the entries directly from original key array
            int currentIndex = arr.keyArrayIndex + 1;
            int elementCount = 0;

            for (int i = 0; i < arr.arrEntry.elementCount; i++) {
                KeyEntry entry = arr.keyArray.get(currentIndex);

                if (entry instanceof FieldEntry fieldEntry) {
                    // Copy field entry as-is (array element)
                    flatKeyArray.add(fieldEntry);
                    currentIndex++;
                    elementCount++;
                } else if (entry instanceof ObjectEntry) {
                    // Nested object - create new ESONObject and flatten recursively
                    ESONObject nestedObj = new ESONObject(currentIndex, arr.keyArray, arr.values);
                    flattenObject(nestedObj, null, flatKeyArray);
                    currentIndex = skipContainer(arr.keyArray, entry, currentIndex);
                    elementCount++;
                } else if (entry instanceof ArrayEntry) {
                    // Nested array - create new ESONArray and flatten recursively
                    ESONArray nestedArr = new ESONArray(currentIndex, arr.keyArray, arr.values);
                    flattenArray(nestedArr, null, flatKeyArray);
                    currentIndex = skipContainer(arr.keyArray, entry, currentIndex);
                    elementCount++;
                }
            }

            newArrEntry.elementCount = elementCount;
        } else {
            int elementCount = 0;
            for (Type type : arr.arrEntry.mutationArray) {
                switch (type) {
                    case Mutation mutation -> {
                        // This is a mutated element - create new FieldEntry with mutation
                        flatKeyArray.add(new FieldEntry(null, mutation));
                        elementCount++;
                    }
                    case ESONObject nestedObj -> {
                        // Nested object - flatten recursively
                        flattenObject(nestedObj, null, flatKeyArray);
                        elementCount++;
                    }
                    case ESONArray nestedArr -> {
                        // Nested array - flatten recursively
                        flattenArray(nestedArr, null, flatKeyArray);
                        elementCount++;
                    }
                    case null, default -> {
                        // Regular type (FixedValue, VariableValue, NullValue) - create field entry
                        flatKeyArray.add(new FieldEntry(null, type));
                        elementCount++;
                    }
                }
            }

            newArrEntry.elementCount = elementCount;
        }
    }

    public void writeToStream(ESONObject object, StreamOutput out) throws IOException {
        out.writeByte((byte) 'E');
        out.writeByte((byte) 'S');
        out.writeByte((byte) 'O');
        out.writeByte((byte) 'N');
        // TODO: How to write size
        // TODO: Assert flattened or support transition to flattened
        for (KeyEntry entry : object.getKeyArray()) {
            if (entry instanceof ObjectEntry) {

            } else if (entry instanceof ArrayEntry) {

            } else {

            }
            entry.writeTo(out);
        }
    }
}
