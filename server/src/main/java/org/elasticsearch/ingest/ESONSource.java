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
        private final List<ESONEntry> keyArray;

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

        private static void parseObject(XContentParser parser, BytesStreamOutput bytes, List<ESONEntry> keyArray, String objectFieldName)
            throws IOException {
            ESONEntry.ObjectEntry objEntry = new ESONEntry.ObjectEntry(objectFieldName);
            keyArray.add(objEntry);

            int count = 0;
            String fieldName;
            while ((fieldName = parser.nextFieldName()) != null) {
                parseValue(parser, fieldName, bytes, keyArray);
                count++;
            }

            objEntry.fieldCount = count;
        }

        private static void parseArray(XContentParser parser, BytesStreamOutput bytes, List<ESONEntry> keyArray, String arrayFieldName)
            throws IOException {
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

            arrEntry.elementCount = count;
        }

        private static void parseValue(XContentParser parser, String fieldName, BytesStreamOutput bytes, List<ESONEntry> keyArray)
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

        private static Value parseSimpleValue(XContentParser parser, BytesStreamOutput bytes, XContentParser.Token token)
            throws IOException {
            long position = bytes.position();

            return switch (token) {
                case VALUE_STRING -> {
                    XContentString.UTF8Bytes stringBytes = parser.optimizedText().bytes();
                    bytes.write(stringBytes.bytes(), stringBytes.offset(), stringBytes.length());
                    yield new VariableValue((int) position, stringBytes.length(), ESONEntry.STRING);
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
                            bytes.write(numberBytes);
                            yield new VariableValue((int) position, numberBytes.length, type);
                        }
                    };
                }
                case VALUE_BOOLEAN -> parser.booleanValue() ? ConstantValue.TRUE : ConstantValue.FALSE;
                case VALUE_NULL -> ConstantValue.NULL;
                case VALUE_EMBEDDED_OBJECT -> {
                    byte[] binaryValue = parser.binaryValue();
                    bytes.write(binaryValue);
                    yield new VariableValue((int) position, binaryValue.length, ESONEntry.BINARY);
                }
                default -> throw new IllegalArgumentException("Unexpected token: " + token);
            };
        }
    }

    public interface Value {
        byte type();
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
    }

    public record VariableValue(int position, int length, byte type) implements Value {
        public Object getValue(Values source) {
            return switch (type) {
                case ESONEntry.STRING -> source.readString(position, length);
                case ESONEntry.BINARY -> source.readByteArray(position, length);
                case ESONEntry.BIG_INTEGER -> new BigInteger(source.readString(position, length));
                case ESONEntry.BIG_DECIMAL -> new BigDecimal(source.readString(position, length));
                default -> throw new IllegalArgumentException("Invalid value type: " + type);
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
            switch (type) {
                case ESONEntry.STRING -> builder.utf8Value(bytes, offset, length);
                case ESONEntry.BINARY -> builder.value(bytes, offset, length);
                // TODO: Improve?
                case ESONEntry.BIG_INTEGER -> builder.value(new BigInteger(new String(bytes, offset, length, StandardCharsets.UTF_8)));
                case ESONEntry.BIG_DECIMAL -> builder.value(new BigDecimal(new String(bytes, offset, length, StandardCharsets.UTF_8)));
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

    public static class ESONObject implements Value, Map<String, Object>, ToXContent {
        private final int keyArrayIndex;
        private final ESONEntry.ObjectEntry objEntry;
        private final List<ESONEntry> keyArray;
        private final Values values;
        private Map<String, Value> materializedMap;

        public ESONObject(int keyArrayIndex, List<ESONEntry> keyArray, Values values) {
            this.keyArrayIndex = keyArrayIndex;
            this.objEntry = (ESONEntry.ObjectEntry) keyArray.get(keyArrayIndex);
            this.keyArray = keyArray;
            this.values = values;
        }

        public List<ESONEntry> getKeyArray() {
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
                    ESONEntry entry = keyArray.get(currentIndex);
                    if (entry instanceof ESONEntry.FieldEntry fieldEntry) {
                        materializedMap.put(fieldEntry.key(), fieldEntry.value);
                        currentIndex++;
                    } else {
                        if (entry instanceof ESONEntry.ObjectEntry) {
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
            Value type = materializedMap.get(key);
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
            Value type = materializedMap.remove(key);
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
                        private final Iterator<Map.Entry<String, Value>> mapIterator = materializedMap.entrySet().iterator();

                        @Override
                        public boolean hasNext() {
                            return mapIterator.hasNext();
                        }

                        @Override
                        public Entry<String, Object> next() {
                            Map.Entry<String, Value> mapEntry = mapIterator.next();
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

        @Override
        public byte type() {
            return ESONEntry.TYPE_OBJECT;
        }

        private class LazyEntry implements Entry<String, Object> {
            private final String key;
            private final Value type;
            private final boolean nullForRawValues;
            private Object cachedValue;
            private boolean valueComputed = false;

            LazyEntry(String key, Value type, boolean nullForRawValues) {
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

    public static class ESONArray extends AbstractList<Object> implements Value, List<Object>, ToXContent {

        private final int keyArrayIndex;
        private final ESONEntry.ArrayEntry arrEntry;
        private final List<ESONEntry> keyArray;
        private final Values values;
        private List<Value> materializedList;

        public ESONArray(int keyArrayIndex, List<ESONEntry> keyArray, Values values) {
            this.keyArrayIndex = keyArrayIndex;
            this.arrEntry = (ESONEntry.ArrayEntry) keyArray.get(keyArrayIndex);
            this.keyArray = keyArray;
            this.values = values;
        }

        private void ensureMaterializedList() {
            if (materializedList == null) {
                materializedList = new ArrayList<>(arrEntry.elementCount);

                int currentIndex = keyArrayIndex + 1;
                for (int i = 0; i < arrEntry.elementCount; i++) {
                    ESONEntry entry = keyArray.get(currentIndex);
                    if (entry instanceof ESONEntry.FieldEntry fieldEntry) {
                        materializedList.add(fieldEntry.value);
                        currentIndex++;
                    } else {
                        if (entry instanceof ESONEntry.ObjectEntry) {
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
            Value type = materializedList.get(index);
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
                Iterator<Value> typeIterator = materializedList.iterator();
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return typeIterator.hasNext();
                    }

                    @Override
                    public Object next() {
                        Value next = typeIterator.next();
                        if (next instanceof VariableValue || next instanceof FixedValue) {
                            return null;
                        } else {
                            return next;
                        }
                    }
                };
            }
        }

        @Override
        public byte type() {
            return ESONEntry.TYPE_ARRAY;
        }
    }

    private static Object convertTypeToValue(Value type, Values values) {
        if (type == null) {
            return null;
        }
        return switch (type) {
            case ESONObject obj -> obj;
            case ESONArray arr -> arr;
            case FixedValue val -> val.getValue(values);
            case VariableValue val -> val.getValue(values);
            case ConstantValue constantValue -> constantValue.getValue();
            case Mutation mutation -> mutation.object();
            default -> throw new IllegalStateException("Unknown type: " + type);
        };
    }

    private static int skipContainer(List<ESONEntry> keyArray, ESONEntry entry, int containerIndex) {
        int index = containerIndex + 1;
        final int fieldCount;
        if (entry instanceof ESONEntry.ObjectEntry objEntry) {
            fieldCount = objEntry.fieldCount;
        } else {
            fieldCount = ((ESONEntry.ArrayEntry) entry).elementCount;
        }

        for (int i = 0; i < fieldCount; i++) {
            ESONEntry fieldESONEntry = keyArray.get(index);
            if (fieldESONEntry instanceof ESONEntry.FieldEntry) {
                index++;
            } else {
                index = skipContainer(keyArray, fieldESONEntry, index);
            }
        }

        return index;
    }

    public static ESONObject flatten(ESONObject original) {
        List<ESONEntry> flatKeyArray = new ArrayList<>(original.getKeyArray().size());

        // Start flattening from the root object
        flattenObject(original, null, flatKeyArray);

        // Return new ESONObject with flattened structure
        return new ESONObject(0, flatKeyArray, original.objectValues());
    }

    /**
     * Recursively flattens an ESONObject into the flat key array
     */
    private static void flattenObject(ESONObject obj, String objectFieldName, List<ESONEntry> flatKeyArray) {
        // Create new ObjectEntry for this object
        ESONEntry.ObjectEntry newObjEntry = new ESONEntry.ObjectEntry(objectFieldName);
        flatKeyArray.add(newObjEntry);

        // Check if object has mutations
        boolean hasMutations = obj.objEntry.hasMutations();

        if (hasMutations == false) {
            // No mutations - just copy the entries directly from original key array
            int currentIndex = obj.keyArrayIndex + 1;
            int fieldCount = 0;

            for (int i = 0; i < obj.objEntry.fieldCount; i++) {
                ESONEntry entry = obj.keyArray.get(currentIndex);

                if (entry instanceof ESONEntry.FieldEntry fieldEntry) {
                    // Copy field entry as-is
                    flatKeyArray.add(fieldEntry);
                    currentIndex++;
                    fieldCount++;
                } else if (entry instanceof ESONEntry.ObjectEntry) {
                    // Nested object - create new ESONObject and flatten recursively
                    ESONObject nestedObj = new ESONObject(currentIndex, obj.keyArray, obj.values);
                    flattenObject(nestedObj, entry.key(), flatKeyArray);
                    // TODO: Remove Need to skip container
                    currentIndex = skipContainer(obj.keyArray, entry, currentIndex);
                    fieldCount++;
                } else if (entry instanceof ESONEntry.ArrayEntry) {
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
            for (Map.Entry<String, Value> entry : obj.objEntry.mutationMap.entrySet()) {
                String key = entry.getKey();
                Value type = entry.getValue();

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
                        flatKeyArray.add(new ESONEntry.FieldEntry(key, type));
                        fieldCount++;
                    }
                }
            }

            newObjEntry.fieldCount = fieldCount;
        }
    }

    private static void handleObject(List<ESONEntry> flatKeyArray, Object object, String key) {
        if (object instanceof Map<?, ?> map) {
            ESONEntry.ObjectEntry objectEntry = new ESONEntry.ObjectEntry(key);
            flatKeyArray.add(objectEntry);
            objectEntry.fieldCount = map.size();
            for (Map.Entry<?, ?> entry1 : map.entrySet()) {
                Object value = entry1.getValue();
                handleObject(flatKeyArray, value, entry1.getKey().toString());
            }
        } else if (object instanceof List<?> list) {
            ESONEntry.ArrayEntry arrayEntry = new ESONEntry.ArrayEntry(key);
            flatKeyArray.add(arrayEntry);
            arrayEntry.elementCount = list.size();
            for (Object value : list) {
                handleObject(flatKeyArray, value, null);
            }
        } else {
            flatKeyArray.add(new ESONEntry.FieldEntry(key, ensureOneLevelMutation(object)));
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
    private static void flattenArray(ESONArray arr, String arrayFieldName, List<ESONEntry> flatKeyArray) {
        // Create new ArrayEntry for this array
        ESONEntry.ArrayEntry newArrEntry = new ESONEntry.ArrayEntry(arrayFieldName);
        flatKeyArray.add(newArrEntry);

        // Check if array has mutations
        boolean hasMutations = arr.arrEntry.hasMutations();

        if (hasMutations == false) {
            // No mutations - just copy the entries directly from original key array
            int currentIndex = arr.keyArrayIndex + 1;
            int elementCount = 0;

            for (int i = 0; i < arr.arrEntry.elementCount; i++) {
                ESONEntry entry = arr.keyArray.get(currentIndex);

                if (entry instanceof ESONEntry.FieldEntry fieldEntry) {
                    // Copy field entry as-is (array element)
                    flatKeyArray.add(fieldEntry);
                    currentIndex++;
                    elementCount++;
                } else if (entry instanceof ESONEntry.ObjectEntry) {
                    // Nested object - create new ESONObject and flatten recursively
                    ESONObject nestedObj = new ESONObject(currentIndex, arr.keyArray, arr.values);
                    flattenObject(nestedObj, null, flatKeyArray);
                    currentIndex = skipContainer(arr.keyArray, entry, currentIndex);
                    elementCount++;
                } else if (entry instanceof ESONEntry.ArrayEntry) {
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
            for (Value type : arr.arrEntry.mutationArray) {
                switch (type) {
                    case Mutation mutation -> {
                        // This is a mutated element - create new FieldEntry with mutation
                        flatKeyArray.add(new ESONEntry.FieldEntry(null, mutation));
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
                        flatKeyArray.add(new ESONEntry.FieldEntry(null, type));
                        elementCount++;
                    }
                }
            }

            newArrEntry.elementCount = elementCount;
        }
    }
}
