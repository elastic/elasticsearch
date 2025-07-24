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

        private final BytesStreamOutput bytes;
        // Key array to store all entries in a flat structure
        private final List<KeyEntry> keyArray;

        public Builder() {
            this(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        }

        public Builder(Recycler<BytesRef> refRecycler) {
            this.bytes = new BytesStreamOutput(128);
            this.keyArray = new ArrayList<>();
        }

        public ESONObject parse(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Expected START_OBJECT but got " + token);
            }

            // Create a deferred Values supplier
            DeferredValuesSupplier deferredSupplier = new DeferredValuesSupplier();

            // Parse the root object and return its index in the key array
            int rootIndex = parseObject(parser, bytes, keyArray);

            // Now populate the deferred supplier with the final bytes
            BytesReference finalBytes = bytes.bytes();
            Values values = new Values(finalBytes);
            deferredSupplier.setValues(values);

            return new ESONObject(rootIndex, keyArray, deferredSupplier);
        }

        // Parse object and add entries to key array, returns the starting index
        private static int parseObject(XContentParser parser, BytesStreamOutput bytes, List<KeyEntry> keyArray) throws IOException {
            int startIndex = keyArray.size();

            // First, add a placeholder object entry that we'll update with count
            ObjectEntry objEntry = new ObjectEntry(0);
            keyArray.add(objEntry);

            int count = 0;
            String fieldName;
            while ((fieldName = parser.nextFieldName()) != null) {
                // Add key entry with the field name and parse its value
                Type type = parseValue(parser, bytes, keyArray);
                keyArray.add(new FieldEntry(fieldName, type));
                count++;
            }

            // Update the object entry with the actual count
            objEntry.fieldCount = count;

            return startIndex;
        }

        // Parse array and add entries to key array, returns the starting index
        private static int parseArray(XContentParser parser, BytesStreamOutput bytes, List<KeyEntry> keyArray) throws IOException {
            int startIndex = keyArray.size();

            // First, add a placeholder array entry that we'll update with count
            ArrayEntry arrEntry = new ArrayEntry(0);
            keyArray.add(arrEntry);

            int count = 0;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                Type type = switch (token) {
                    case START_OBJECT -> new ContainerType(parseObject(parser, bytes, keyArray));
                    case START_ARRAY -> new ContainerType(parseArray(parser, bytes, keyArray));
                    default -> parseSimpleValue(parser, bytes, token);
                };
                // For arrays, add entries with null keys
                keyArray.add(new FieldEntry(null, type));
                count++;
            }

            // Update the array entry with the actual count
            arrEntry.elementCount = count;

            return startIndex;
        }

        private static Type parseValue(XContentParser parser, BytesStreamOutput bytes, List<KeyEntry> keyArray) throws IOException {
            XContentParser.Token token = parser.nextToken();

            return switch (token) {
                case START_OBJECT -> new ContainerType(parseObject(parser, bytes, keyArray));
                case START_ARRAY -> new ContainerType(parseArray(parser, bytes, keyArray));
                default -> parseSimpleValue(parser, bytes, token);
            };
        }

        // Parse non-container values and return type
        private static Type parseSimpleValue(XContentParser parser, BytesStreamOutput bytes, XContentParser.Token token)
            throws IOException {
            long position = bytes.position();

            return switch (token) {
                case VALUE_STRING -> {
                    byte[] stringBytes = parser.text().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    bytes.write(stringBytes);
                    yield new VariableValue((int) position, stringBytes.length, ValueType.STRING);
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
                        default -> throw new IllegalArgumentException("Unsupported number type: " + numberType);
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

    // Helper class to defer Values creation until after parsing is complete
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

    // Base interface for all entries in the key array
    public interface KeyEntry {}

    // Object entry - indicates start of an object with field count
    public static class ObjectEntry implements KeyEntry {
        public int fieldCount;

        public ObjectEntry(int fieldCount) {
            this.fieldCount = fieldCount;
        }
    }

    // Array entry - indicates start of an array with element count
    public static class ArrayEntry implements KeyEntry {
        public int elementCount;

        public ArrayEntry(int elementCount) {
            this.elementCount = elementCount;
        }
    }

    // Field entry - represents a field in an object or element in an array
    public static class FieldEntry implements KeyEntry {
        public final String key; // null for array elements
        public final Type type;

        public FieldEntry(String key, Type type) {
            this.key = key;
            this.type = type;
        }
    }

    // Type hierarchy
    public interface Type {}

    // Represents a mutation/update to a field
    public record Mutation(Object object) implements Type {}

    // Represents a container (object or array) by its index in the key array
    public record ContainerType(int keyArrayIndex) implements Type {}

    // Represents a null value
    public enum NullValue implements Type {
        INSTANCE
    }

    // Main object implementation
    public static class ESONObject implements Type, Map<String, Object>, ToXContent {
        private final int keyArrayIndex;
        private final List<KeyEntry> keyArray;
        private final Supplier<Values> objectValues;

        // Lazily built map for mutations and materialized values
        private Map<String, Object> materializedMap;

        public ESONObject(int keyArrayIndex, List<KeyEntry> keyArray, Supplier<Values> objectValues) {
            this.keyArrayIndex = keyArrayIndex;
            this.keyArray = keyArray;
            this.objectValues = objectValues;
        }

        private void ensureMaterializedMap() {
            if (materializedMap == null) {
                materializedMap = new HashMap<>();
                // Iterate through the key array and populate the map
                ObjectEntry objEntry = (ObjectEntry) keyArray.get(keyArrayIndex);
                int currentIndex = keyArrayIndex + 1;

                for (int i = 0; i < objEntry.fieldCount; i++) {
                    FieldEntry fieldEntry = (FieldEntry) keyArray.get(currentIndex);
                    materializedMap.put(fieldEntry.key, fieldEntry.type);
                    currentIndex++;

                    // Skip nested containers
                    if (fieldEntry.type instanceof ContainerType container) {
                        currentIndex = skipContainer(container.keyArrayIndex);
                    }
                }
            }
        }

        private int skipContainer(int containerIndex) {
            KeyEntry entry = keyArray.get(containerIndex);
            int index = containerIndex + 1;

            if (entry instanceof ObjectEntry objEntry) {
                for (int i = 0; i < objEntry.fieldCount; i++) {
                    FieldEntry fieldEntry = (FieldEntry) keyArray.get(index);
                    index++;
                    if (fieldEntry.type instanceof ContainerType container) {
                        index = skipContainer(container.keyArrayIndex);
                    }
                }
            } else if (entry instanceof ArrayEntry arrEntry) {
                for (int i = 0; i < arrEntry.elementCount; i++) {
                    FieldEntry fieldEntry = (FieldEntry) keyArray.get(index);
                    index++;
                    if (fieldEntry.type instanceof ContainerType container) {
                        index = skipContainer(container.keyArrayIndex);
                    }
                }
            }

            return index;
        }

        @Override
        public int size() {
            ObjectEntry objEntry = (ObjectEntry) keyArray.get(keyArrayIndex);
            return objEntry.fieldCount;
        }

        @Override
        public boolean isEmpty() {
            return size() == 0;
        }

        @Override
        public boolean containsKey(Object key) {
            if (materializedMap != null && materializedMap.containsKey(key)) {
                return true;
            }

            // Linear search through the key array
            ObjectEntry objEntry = (ObjectEntry) keyArray.get(keyArrayIndex);
            int currentIndex = keyArrayIndex + 1;

            for (int i = 0; i < objEntry.fieldCount; i++) {
                FieldEntry fieldEntry = (FieldEntry) keyArray.get(currentIndex);
                if (key.equals(fieldEntry.key)) {
                    return true;
                }
                currentIndex++;

                // Skip nested containers
                if (fieldEntry.type instanceof ContainerType container) {
                    currentIndex = skipContainer(container.keyArrayIndex);
                }
            }

            return false;
        }

        @Override
        public boolean containsValue(Object value) {
            throw new UnsupportedOperationException("containsValue not supported");
        }

        @Override
        public Object get(Object key) {
            // Check materialized map first
            if (materializedMap != null && materializedMap.containsKey(key)) {
                Object value = materializedMap.get(key);
                if (value instanceof Type type) {
                    return convertTypeToValue(type);
                }
                return value;
            }

            // Search in key array
            ObjectEntry objEntry = (ObjectEntry) keyArray.get(keyArrayIndex);
            int currentIndex = keyArrayIndex + 1;

            for (int i = 0; i < objEntry.fieldCount; i++) {
                FieldEntry fieldEntry = (FieldEntry) keyArray.get(currentIndex);
                if (key.equals(fieldEntry.key)) {
                    return convertTypeToValue(fieldEntry.type);
                }
                currentIndex++;

                // Skip nested containers
                if (fieldEntry.type instanceof ContainerType container) {
                    currentIndex = skipContainer(container.keyArrayIndex);
                }
            }

            return null;
        }

        @Override
        public Object put(String key, Object value) {
            ensureMaterializedMap();
            Object oldValue = get(key);
            materializedMap.put(key, value);
            return oldValue;
        }

        @Override
        public Object remove(Object key) {
            ensureMaterializedMap();
            Object oldValue = get(key);
            materializedMap.remove(key);
            return oldValue;
        }

        @Override
        public void putAll(Map<? extends String, ?> m) {
            ensureMaterializedMap();
            for (Entry<? extends String, ?> entry : m.entrySet()) {
                materializedMap.put(entry.getKey(), entry.getValue());
            }
        }

        @Override
        public void clear() {
            ensureMaterializedMap();
            materializedMap.clear();
        }

        @Override
        public Set<String> keySet() {
            ensureMaterializedMap();
            Set<String> keys = new java.util.HashSet<>();

            // Add keys from key array
            ObjectEntry objEntry = (ObjectEntry) keyArray.get(keyArrayIndex);
            int currentIndex = keyArrayIndex + 1;

            for (int i = 0; i < objEntry.fieldCount; i++) {
                FieldEntry fieldEntry = (FieldEntry) keyArray.get(currentIndex);
                keys.add(fieldEntry.key);
                currentIndex++;

                // Skip nested containers
                if (fieldEntry.type instanceof ContainerType container) {
                    currentIndex = skipContainer(container.keyArrayIndex);
                }
            }

            // Add/override with materialized keys
            keys.addAll(materializedMap.keySet());

            return keys;
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
            return new AbstractSet<>() {
                @Override
                public Iterator<Entry<String, Object>> iterator() {
                    return new Iterator<>() {
                        private final ObjectEntry objEntry = (ObjectEntry) keyArray.get(keyArrayIndex);
                        private int currentIndex = keyArrayIndex + 1;
                        private int remaining = objEntry.fieldCount;
                        private final Iterator<Entry<String, Object>> materializedIterator;

                        {
                            if (materializedMap != null) {
                                materializedIterator = materializedMap.entrySet().iterator();
                            } else {
                                materializedIterator = null;
                            }
                        }

                        @Override
                        public boolean hasNext() {
                            return remaining > 0 || (materializedIterator != null && materializedIterator.hasNext());
                        }

                        @Override
                        public Entry<String, Object> next() {
                            if (remaining > 0) {
                                FieldEntry fieldEntry = (FieldEntry) keyArray.get(currentIndex);
                                currentIndex++;
                                remaining--;

                                // Skip nested containers for next iteration
                                if (fieldEntry.type instanceof ContainerType container) {
                                    currentIndex = skipContainer(container.keyArrayIndex);
                                }

                                return new AbstractMap.SimpleEntry<>(fieldEntry.key, convertTypeToValue(fieldEntry.type));
                            } else if (materializedIterator != null && materializedIterator.hasNext()) {
                                return materializedIterator.next();
                            }
                            throw new java.util.NoSuchElementException();
                        }
                    };
                }

                @Override
                public int size() {
                    return ESONObject.this.size();
                }
            };
        }

        private Object convertTypeToValue(Type type) {
            if (type == null) {
                return null;
            }
            return switch (type) {
                case ContainerType container -> {
                    KeyEntry entry = keyArray.get(container.keyArrayIndex);
                    if (entry instanceof ObjectEntry) {
                        yield new ESONObject(container.keyArrayIndex, keyArray, objectValues);
                    } else if (entry instanceof ArrayEntry) {
                        yield new ESONArray(container.keyArrayIndex, keyArray, objectValues);
                    } else {
                        throw new IllegalStateException("Invalid container type at index: " + container.keyArrayIndex);
                    }
                }
                case FixedValue val -> val.getValue(objectValues.get());
                case VariableValue val -> val.getValue(objectValues.get());
                case NullValue.INSTANCE -> null;
                case Mutation mutation -> mutation.object();
                default -> throw new IllegalStateException("Unknown type: " + type);
            };
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

    // Array implementation
    public static class ESONArray extends AbstractList<Object> implements Type, List<Object>, ToXContent {
        private final int keyArrayIndex;
        private final List<KeyEntry> keyArray;
        private final Supplier<Values> arrayValues;

        // Lazily built list for mutations and materialized values
        private List<Object> materializedList;

        public ESONArray(int keyArrayIndex, List<KeyEntry> keyArray, Supplier<Values> arrayValues) {
            this.keyArrayIndex = keyArrayIndex;
            this.keyArray = keyArray;
            this.arrayValues = arrayValues;
        }

        private void ensureMaterializedList() {
            if (materializedList == null) {
                materializedList = new ArrayList<>();
                ArrayEntry arrEntry = (ArrayEntry) keyArray.get(keyArrayIndex);
                int currentIndex = keyArrayIndex + 1;

                for (int i = 0; i < arrEntry.elementCount; i++) {
                    FieldEntry fieldEntry = (FieldEntry) keyArray.get(currentIndex);
                    materializedList.add(fieldEntry.type);
                    currentIndex++;

                    // Skip nested containers
                    if (fieldEntry.type instanceof ContainerType container) {
                        currentIndex = skipContainer(container.keyArrayIndex);
                    }
                }
            }
        }

        private int skipContainer(int containerIndex) {
            KeyEntry entry = keyArray.get(containerIndex);
            int index = containerIndex + 1;

            if (entry instanceof ObjectEntry objEntry) {
                for (int i = 0; i < objEntry.fieldCount; i++) {
                    FieldEntry fieldEntry = (FieldEntry) keyArray.get(index);
                    index++;
                    if (fieldEntry.type instanceof ContainerType container) {
                        index = skipContainer(container.keyArrayIndex);
                    }
                }
            } else if (entry instanceof ArrayEntry arrEntry) {
                for (int i = 0; i < arrEntry.elementCount; i++) {
                    FieldEntry fieldEntry = (FieldEntry) keyArray.get(index);
                    index++;
                    if (fieldEntry.type instanceof ContainerType container) {
                        index = skipContainer(container.keyArrayIndex);
                    }
                }
            }

            return index;
        }

        @Override
        public Object get(int index) {
            if (materializedList != null) {
                Object value = materializedList.get(index);
                if (value instanceof Type type) {
                    return convertTypeToValue(type);
                }
                return value;
            }

            ArrayEntry arrEntry = (ArrayEntry) keyArray.get(keyArrayIndex);
            if (index < 0 || index >= arrEntry.elementCount) {
                throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + arrEntry.elementCount);
            }

            int currentIndex = keyArrayIndex + 1;
            for (int i = 0; i < index; i++) {
                FieldEntry fieldEntry = (FieldEntry) keyArray.get(currentIndex);
                currentIndex++;
                if (fieldEntry.type instanceof ContainerType container) {
                    currentIndex = skipContainer(container.keyArrayIndex);
                }
            }

            FieldEntry fieldEntry = (FieldEntry) keyArray.get(currentIndex);
            return convertTypeToValue(fieldEntry.type);
        }

        @Override
        public void add(int index, Object element) {
            ensureMaterializedList();
            materializedList.add(index, element);
        }

        @Override
        public Object set(int index, Object element) {
            ensureMaterializedList();
            Object oldValue = get(index);
            materializedList.set(index, element);
            return oldValue;
        }

        @Override
        public Object remove(int index) {
            ensureMaterializedList();
            Object oldValue = get(index);
            materializedList.remove(index);
            return oldValue;
        }

        @Override
        public int size() {
            if (materializedList != null) {
                return materializedList.size();
            }
            ArrayEntry arrEntry = (ArrayEntry) keyArray.get(keyArrayIndex);
            return arrEntry.elementCount;
        }

        private Object convertTypeToValue(Type type) {
            if (type == null) {
                return null;
            }
            return switch (type) {
                case ContainerType container -> {
                    KeyEntry entry = keyArray.get(container.keyArrayIndex);
                    if (entry instanceof ObjectEntry) {
                        yield new ESONObject(container.keyArrayIndex, keyArray, arrayValues);
                    } else if (entry instanceof ArrayEntry) {
                        yield new ESONArray(container.keyArrayIndex, keyArray, arrayValues);
                    } else {
                        throw new IllegalStateException("Invalid container type at index: " + container.keyArrayIndex);
                    }
                }
                case FixedValue val -> val.getValue(arrayValues.get());
                case VariableValue val -> val.getValue(arrayValues.get());
                case NullValue.INSTANCE -> null;
                case Mutation mutation -> mutation.object();
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

    // Value types
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

    // Values reader
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
