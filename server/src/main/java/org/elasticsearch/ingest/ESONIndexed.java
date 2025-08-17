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
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;
import java.io.UncheckedIOException;
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

public class ESONIndexed {

    public static ESONObject fromFlat(ESONFlat esonFlat) {
        return new ESONObject(0, esonFlat);
    }

    public static class ESONObject implements ESONSource.Value, Map<String, Object>, ToXContent {
        private final int keyArrayIndex;
        private final ESONEntry.ObjectEntry objEntry;
        private final ESONFlat esonFlat;
        private Map<String, ESONSource.Value> materializedMap;

        public ESONObject(int keyArrayIndex, ESONFlat esonFlat) {
            this.keyArrayIndex = keyArrayIndex;
            this.esonFlat = esonFlat;
            this.objEntry = (ESONEntry.ObjectEntry) esonFlat.keys().get(keyArrayIndex);
        }

        public ESONFlat esonFlat() {
            return esonFlat;
        }

        private void ensureMaterializedMap() {
            if (materializedMap == null) {
                materializedMap = new HashMap<>(objEntry.offsetOrCount());

                int currentIndex = keyArrayIndex + 1;
                for (int i = 0; i < objEntry.offsetOrCount(); i++) {
                    ESONEntry entry = esonFlat.keys().get(currentIndex);
                    if (entry instanceof ESONEntry.FieldEntry fieldEntry) {
                        materializedMap.put(fieldEntry.key(), fieldEntry.value());
                        currentIndex++;
                    } else {
                        if (entry instanceof ESONEntry.ObjectEntry) {
                            materializedMap.put(entry.key(), new ESONObject(currentIndex, esonFlat));
                        } else {
                            materializedMap.put(entry.key(), new ESONArray(currentIndex, esonFlat));
                        }
                        currentIndex = skipContainer(esonFlat.keys(), entry, currentIndex);
                    }
                }
            }
        }

        @Override
        public int size() {
            if (materializedMap == null) {
                return objEntry.offsetOrCount();
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
            ESONSource.Value type = materializedMap.get(key);
            if (type == null) {
                return null;
            } else if (type instanceof ESONSource.Mutation mutation) {
                return mutation.object();
            }
            return convertTypeToValue(type, esonFlat.values());
        }

        @Override
        public Object put(String key, Object value) {
            ensureMaterializedMap();
            Object oldValue = get(key);
            materializedMap.put(key, new ESONSource.Mutation(value));
            objEntry.mutationMap = materializedMap;
            return oldValue;
        }

        @Override
        public Object remove(Object key) {
            ensureMaterializedMap();
            ESONSource.Value type = materializedMap.remove(key);
            objEntry.mutationMap = materializedMap;
            if (type == null) {
                return null;
            } else if (type instanceof ESONSource.Mutation mutation) {
                return mutation.object();
            }
            return convertTypeToValue(type, esonFlat.values());
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
                        private final Iterator<Map.Entry<String, ESONSource.Value>> mapIterator = materializedMap.entrySet().iterator();

                        @Override
                        public boolean hasNext() {
                            return mapIterator.hasNext();
                        }

                        @Override
                        public Entry<String, Object> next() {
                            Map.Entry<String, ESONSource.Value> mapEntry = mapIterator.next();
                            return new ESONObject.LazyEntry(mapEntry.getKey(), mapEntry.getValue(), nullForRawValues);
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

        public class LazyEntry implements Entry<String, Object> {
            private final String key;
            private final ESONSource.Value type;
            private final boolean nullForRawValues;
            private Object cachedValue;
            private boolean valueComputed = false;

            LazyEntry(String key, ESONSource.Value type, boolean nullForRawValues) {
                this.key = key;
                this.type = type;
                this.nullForRawValues = nullForRawValues;
            }

            @Override
            public String getKey() {
                return key;
            }

            public boolean isRawValue() {
                return type instanceof ESONSource.FixedValue || type instanceof ESONSource.VariableValue;
            }

            public boolean isUTF8Bytes() {
                return type.type() == ESONEntry.STRING;
            }

            public XContentString utf8Bytes() {
                if (type instanceof ESONSource.VariableValue varValue && varValue.type() == ESONEntry.STRING) {
                    BytesRef bytesRef = ESONSource.Values.readByteSlice(esonFlat.values().data(), varValue.position());
                    return new Text(new XContentString.UTF8Bytes(bytesRef.bytes, bytesRef.offset, bytesRef.length), bytesRef.length);
                }
                throw new IllegalArgumentException();

            }

            @Override
            public Object getValue() {
                if (valueComputed == false) {
                    if (type == null) {
                        cachedValue = null;
                    } else if (type instanceof ESONSource.Mutation mutation) {
                        cachedValue = mutation.object();
                    } else {
                        if (nullForRawValues && isRawValue()) {
                            cachedValue = null;
                        } else {
                            cachedValue = convertTypeToValue(type, esonFlat.values());
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

    public static class ESONArray extends AbstractList<Object> implements ESONSource.Value, List<Object>, ToXContent {

        private final int keyArrayIndex;
        private final ESONEntry.ArrayEntry arrEntry;
        private final ESONFlat esonFlat;
        private List<ESONSource.Value> materializedList;

        public ESONArray(int keyArrayIndex, ESONFlat esonFlat) {
            this.keyArrayIndex = keyArrayIndex;
            this.esonFlat = esonFlat;
            this.arrEntry = (ESONEntry.ArrayEntry) esonFlat.keys().get(keyArrayIndex);
        }

        private void ensureMaterializedList() {
            if (materializedList == null) {
                materializedList = new ArrayList<>(arrEntry.offsetOrCount());

                int currentIndex = keyArrayIndex + 1;
                for (int i = 0; i < arrEntry.offsetOrCount(); i++) {
                    ESONEntry entry = esonFlat.keys().get(currentIndex);
                    if (entry instanceof ESONEntry.FieldEntry fieldEntry) {
                        materializedList.add(fieldEntry.value());
                        currentIndex++;
                    } else {
                        if (entry instanceof ESONEntry.ObjectEntry) {
                            materializedList.add(new ESONObject(currentIndex, esonFlat));
                        } else {
                            materializedList.add(new ESONArray(currentIndex, esonFlat));
                        }
                        currentIndex = skipContainer(esonFlat.keys(), entry, currentIndex);
                    }
                }
            }
        }

        @Override
        public Object get(int index) {
            // TODO: Can implement this without materializing
            ensureMaterializedList();
            ESONSource.Value type = materializedList.get(index);
            if (type == null) {
                return null;
            } else if (type instanceof ESONSource.Mutation mutation) {
                return mutation.object();
            }

            return convertTypeToValue(type, esonFlat.values());
        }

        @Override
        public void add(int index, Object element) {
            ensureMaterializedList();
            materializedList.add(index, new ESONSource.Mutation(element));
            arrEntry.mutationArray = materializedList;
        }

        @Override
        public Object set(int index, Object element) {
            ensureMaterializedList();
            Object oldValue = get(index);
            materializedList.set(index, new ESONSource.Mutation(element));
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
            boolean result = materializedList.add(new ESONSource.Mutation(element));
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
                return arrEntry.offsetOrCount();
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
                Iterator<ESONSource.Value> typeIterator = materializedList.iterator();
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return typeIterator.hasNext();
                    }

                    @Override
                    public Object next() {
                        ESONSource.Value next = typeIterator.next();
                        if (next instanceof ESONSource.VariableValue || next instanceof ESONSource.FixedValue) {
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

    private static Object convertTypeToValue(ESONSource.Value type, ESONSource.Values values) {
        if (type == null) {
            return null;
        }
        return switch (type) {
            case ESONIndexed.ESONObject obj -> obj;
            case ESONIndexed.ESONArray arr -> arr;
            case ESONSource.FixedValue val -> val.getValue(values);
            case ESONSource.VariableValue val -> val.getValue(values);
            case ESONSource.ConstantValue constantValue -> constantValue.getValue();
            case ESONSource.Mutation mutation -> mutation.object();
            default -> throw new IllegalStateException("Unknown type: " + type);
        };
    }

    private static int skipContainer(List<ESONEntry> keyArray, ESONEntry entry, int containerIndex) {
        int index = containerIndex + 1;
        final int fieldCount = entry.offsetOrCount();

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

    public static ESONFlat flatten(ESONIndexed.ESONObject original) {
        // TODO: Add a better estimate of the size to be added
        BytesStreamOutput newValuesOut = new BytesStreamOutput(128);
        List<ESONEntry> flatKeyArray = new ArrayList<>(original.esonFlat.keys().size());
        BytesReference originalData = original.esonFlat.values().data();

        try {
            // Start flattening from the root object
            flattenObject(original, null, flatKeyArray, originalData.length(), newValuesOut);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        // Return new ESONObject with flattened structure
        BytesReference data = CompositeBytesReference.of(originalData, newValuesOut.bytes());
        return new ESONFlat(flatKeyArray, new ESONSource.Values(data));
    }

    /**
     * Recursively flattens an ESONObject into the flat key array
     */
    private static void flattenObject(
        ESONObject obj,
        String objectFieldName,
        List<ESONEntry> flatKeyArray,
        int newOffset,
        BytesStreamOutput newValuesOut
    ) throws IOException {
        // Create new ObjectEntry for this object
        ESONEntry.ObjectEntry newObjEntry = new ESONEntry.ObjectEntry(objectFieldName);
        flatKeyArray.add(newObjEntry);

        // Check if object has mutations
        boolean hasMutations = obj.objEntry.hasMutations();

        if (hasMutations == false) {
            // No mutations - just copy the entries directly from original key array
            int currentIndex = obj.keyArrayIndex + 1;
            int fieldCount = 0;

            for (int i = 0; i < obj.objEntry.offsetOrCount(); i++) {
                ESONEntry entry = obj.esonFlat.keys().get(currentIndex);

                if (entry instanceof ESONEntry.FieldEntry fieldEntry) {
                    // Copy field entry as-is
                    flatKeyArray.add(fieldEntry);
                    currentIndex++;
                    fieldCount++;
                } else if (entry instanceof ESONEntry.ObjectEntry) {
                    // Nested object - create new ESONObject and flatten recursively
                    ESONObject nestedObj = new ESONObject(currentIndex, obj.esonFlat);
                    flattenObject(nestedObj, entry.key(), flatKeyArray, newOffset, newValuesOut);
                    // TODO: Remove Need to skip container
                    currentIndex = skipContainer(obj.esonFlat.keys(), entry, currentIndex);
                    fieldCount++;
                } else if (entry instanceof ESONEntry.ArrayEntry) {
                    // Nested array - create new ESONArray and flatten recursively
                    ESONArray nestedArr = new ESONArray(currentIndex, obj.esonFlat);
                    flattenArray(nestedArr, entry.key(), flatKeyArray, newOffset, newValuesOut);
                    // TODO: Remove Need to skip container
                    currentIndex = skipContainer(obj.esonFlat.keys(), entry, currentIndex);
                    fieldCount++;
                }
            }

            newObjEntry.offsetOrCount(fieldCount);
        } else {
            // Has mutations - need to iterate through materialized map
            obj.ensureMaterializedMap();

            int fieldCount = 0;
            for (Map.Entry<String, ESONSource.Value> entry : obj.objEntry.mutationMap.entrySet()) {
                String key = entry.getKey();
                ESONSource.Value type = entry.getValue();

                switch (type) {
                    case ESONSource.Mutation mutation -> {
                        handleObject(flatKeyArray, mutation.object(), key, newOffset, newValuesOut);
                        fieldCount++;
                    }
                    case ESONObject nestedObj -> {
                        // Nested object - flatten recursively
                        flattenObject(nestedObj, key, flatKeyArray, newOffset, newValuesOut);
                        fieldCount++;
                    }
                    case ESONArray nestedArr -> {
                        // Nested array - flatten recursively
                        flattenArray(nestedArr, key, flatKeyArray, newOffset, newValuesOut);
                        fieldCount++;
                    }
                    case null -> {
                        flatKeyArray.add(new ESONEntry.FieldEntry(key, ESONSource.ConstantValue.NULL));
                        fieldCount++;
                    }
                    default -> {
                        // Regular type (FixedValue, VariableValue, NullValue) - create field entry
                        flatKeyArray.add(new ESONEntry.FieldEntry(key, type));
                        fieldCount++;
                    }
                }
            }

            newObjEntry.offsetOrCount(fieldCount);
        }
    }

    private static void handleObject(List<ESONEntry> flatKeyArray, Object object, String key, int newOffset, BytesStreamOutput newValuesOut)
        throws IOException {
        Object obj = unwrapObject(object);
        if (obj instanceof Map<?, ?> map) {
            flatKeyArray.add(new ESONEntry.ObjectEntry(key, map.size()));
            for (Map.Entry<?, ?> entry1 : map.entrySet()) {
                Object value = entry1.getValue();
                handleObject(flatKeyArray, value, entry1.getKey().toString(), newOffset, newValuesOut);
            }
        } else if (obj instanceof List<?> list) {
            flatKeyArray.add(new ESONEntry.ArrayEntry(key, list.size()));
            for (Object value : list) {
                handleObject(flatKeyArray, value, null, newOffset, newValuesOut);
            }
        } else {
            flatKeyArray.add(mutationToValue(newOffset, key, newValuesOut, obj));
        }
    }

    /**
     * Recursively flattens an ESONArray into the flat key array
     */
    private static void flattenArray(
        ESONArray arr,
        String arrayFieldName,
        List<ESONEntry> flatKeyArray,
        int newOffset,
        BytesStreamOutput newValuesOut
    ) throws IOException {
        // Create new ArrayEntry for this array
        ESONEntry.ArrayEntry newArrEntry = new ESONEntry.ArrayEntry(arrayFieldName);
        flatKeyArray.add(newArrEntry);

        // Check if array has mutations
        boolean hasMutations = arr.arrEntry.hasMutations();

        if (hasMutations == false) {
            // No mutations - just copy the entries directly from original key array
            int currentIndex = arr.keyArrayIndex + 1;
            int elementCount = 0;

            for (int i = 0; i < arr.arrEntry.offsetOrCount(); i++) {
                ESONEntry entry = arr.esonFlat.keys().get(currentIndex);

                if (entry instanceof ESONEntry.FieldEntry fieldEntry) {
                    // Copy field entry as-is (array element)
                    flatKeyArray.add(fieldEntry);
                    currentIndex++;
                    elementCount++;
                } else if (entry instanceof ESONEntry.ObjectEntry) {
                    // Nested object - create new ESONObject and flatten recursively
                    ESONObject nestedObj = new ESONObject(currentIndex, arr.esonFlat);
                    flattenObject(nestedObj, null, flatKeyArray, newOffset, newValuesOut);
                    currentIndex = skipContainer(arr.esonFlat.keys(), entry, currentIndex);
                    elementCount++;
                } else if (entry instanceof ESONEntry.ArrayEntry) {
                    // Nested array - create new ESONArray and flatten recursively
                    ESONArray nestedArr = new ESONArray(currentIndex, arr.esonFlat);
                    flattenArray(nestedArr, null, flatKeyArray, newOffset, newValuesOut);
                    currentIndex = skipContainer(arr.esonFlat.keys(), entry, currentIndex);
                    elementCount++;
                }
            }

            newArrEntry.offsetOrCount(elementCount);
        } else {
            int elementCount = 0;
            for (ESONSource.Value type : arr.arrEntry.mutationArray) {
                switch (type) {
                    case ESONSource.Mutation mutation -> {
                        // This is a mutated element - create new FieldEntry with mutation
                        flatKeyArray.add(mutationToValue(newOffset, null, newValuesOut, mutation.object()));
                        elementCount++;
                    }
                    case ESONObject nestedObj -> {
                        // Nested object - flatten recursively
                        flattenObject(nestedObj, null, flatKeyArray, newOffset, newValuesOut);
                        elementCount++;
                    }
                    case ESONArray nestedArr -> {
                        // Nested array - flatten recursively
                        flattenArray(nestedArr, null, flatKeyArray, newOffset, newValuesOut);
                        elementCount++;
                    }
                    case null -> {
                        flatKeyArray.add(new ESONEntry.FieldEntry(null, ESONSource.ConstantValue.NULL));
                        elementCount++;
                    }
                    default -> {
                        // Regular type (FixedValue, VariableValue, NullValue) - create field entry
                        flatKeyArray.add(new ESONEntry.FieldEntry(null, type));
                        elementCount++;
                    }
                }
            }

            newArrEntry.offsetOrCount(elementCount);
        }
    }

    private static ESONEntry.FieldEntry mutationToValue(int newOffset, String fieldName, BytesStreamOutput newValuesOut, Object obj)
        throws IOException {
        int position = newOffset + Math.toIntExact(newValuesOut.position());
        ESONEntry.FieldEntry value;
        if (obj == null) {
            value = new ESONEntry.FieldEntry(fieldName, ESONSource.ConstantValue.NULL);
        } else if (obj instanceof Number num) {
            value = switch (num) {
                case Byte byteValue -> {
                    newValuesOut.writeInt(byteValue.intValue());
                    yield new ESONEntry.FieldEntry(fieldName, ESONEntry.TYPE_INT, position);
                }
                case Short shortValue -> {
                    newValuesOut.writeInt(shortValue);
                    yield new ESONEntry.FieldEntry(fieldName, ESONEntry.TYPE_INT, position);
                }
                case Integer intValue -> {
                    newValuesOut.writeInt(intValue);
                    yield new ESONEntry.FieldEntry(fieldName, ESONEntry.TYPE_INT, position);
                }
                case Long longValue -> {
                    newValuesOut.writeLong(longValue);
                    yield new ESONEntry.FieldEntry(fieldName, ESONEntry.TYPE_LONG, position);
                }
                case Float floatValue -> {
                    newValuesOut.writeFloat(floatValue);
                    yield new ESONEntry.FieldEntry(fieldName, ESONEntry.TYPE_FLOAT, position);
                }
                case Double doubleValue -> {
                    newValuesOut.writeDouble(doubleValue);
                    yield new ESONEntry.FieldEntry(fieldName, ESONEntry.TYPE_DOUBLE, position);
                }
                case BigInteger bigInteger -> {
                    byte[] numberBytes = bigInteger.toString().getBytes(StandardCharsets.UTF_8);
                    newValuesOut.writeVInt(numberBytes.length);
                    newValuesOut.write(numberBytes);
                    yield new ESONEntry.FieldEntry(fieldName, ESONEntry.BIG_INTEGER, position);
                }
                case BigDecimal bigDecimal -> {
                    byte[] numberBytes = bigDecimal.toString().getBytes(StandardCharsets.UTF_8);
                    newValuesOut.writeVInt(numberBytes.length);
                    newValuesOut.write(numberBytes);
                    yield new ESONEntry.FieldEntry(fieldName, ESONEntry.BIG_DECIMAL, position);
                }
                default -> {
                    byte[] utf8Bytes = num.toString().getBytes(StandardCharsets.UTF_8);
                    newValuesOut.writeVInt(utf8Bytes.length);
                    newValuesOut.write(utf8Bytes);
                    yield new ESONEntry.FieldEntry(fieldName, ESONEntry.STRING, position);
                }
            };
        } else if (obj instanceof Boolean bool) {
            value = new ESONEntry.FieldEntry(fieldName, bool ? ESONSource.ConstantValue.TRUE : ESONSource.ConstantValue.FALSE);
        } else if (obj instanceof byte[] bytes) {
            newValuesOut.writeVInt(bytes.length);
            newValuesOut.writeBytes(bytes);
            value = new ESONEntry.FieldEntry(fieldName, ESONEntry.BINARY, position);
        } else {
            String str = obj.toString();
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            newValuesOut.writeVInt(bytes.length);
            newValuesOut.writeBytes(bytes);
            value = new ESONEntry.FieldEntry(fieldName, ESONEntry.STRING, position);
        }

        return value;
    }

    private static Object unwrapObject(Object value) {
        while (value instanceof ESONSource.Mutation m) {
            value = m.object();
        }
        return value;
    }
}
