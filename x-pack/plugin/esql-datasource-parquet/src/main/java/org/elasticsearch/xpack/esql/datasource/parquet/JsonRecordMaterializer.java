/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;

/**
 * Renders one composite Parquet column per record as a JSON document, driven by parquet-mr's own record
 * assembly.
 * <p>
 * The point of going through {@link RecordMaterializer} rather than decoding leaves directly is
 * correlation. A list of structs is several leaf columns whose values only mean something paired up, and
 * the pairing is carried by repetition and definition levels rather than by position: two leaves of the
 * same list can break pages on different rows, so "the n-th value of {@code sku}" and "the n-th value of
 * {@code price}" are not in general the same element. parquet-mr already solves that, and this class
 * borrows the solution instead of restating it.
 * <p>
 * <b>Field order.</b> Each group converter buffers its direct children's rendered values and emits them in
 * schema order when the group closes. Buffering is what makes the order canonical: parquet-mr calls no
 * converter at all for an absent value, so a group only learns which of its fields were missing once its
 * {@code end()} runs — by which time anything emitted as it arrived is already written. The buffer holds one
 * rendered value per direct child, so it is bounded by the schema's breadth and depth and not by the data:
 * a nested group hands its parent a single finished string.
 * <p>
 * <b>Shapes.</b> The converter tree is built by walking the schema, so nesting is structural rather than
 * enumerated. Lists are required to use the standard three-level encoding; the two-level backward-compatible
 * form, where the repeated node is itself the element, is rejected rather than guessed at — telling the two
 * apart depends on the repeated node's name under the Parquet specification's backward-compatibility rules,
 * which this class deliberately does not implement.
 */
final class JsonRecordMaterializer extends RecordMaterializer<BytesRef> {

    private final GroupConverter root;
    private String record;
    private boolean present;

    /**
     * @param schema a message holding exactly one top-level field, the composite column to render
     */
    JsonRecordMaterializer(MessageType schema) {
        if (schema.getFieldCount() != 1) {
            throw new IllegalArgumentException(
                "expected a message with exactly one top-level field, found " + schema.getFieldCount() + ": " + schema
            );
        }
        Converter child = converterFor(schema.getType(0), value -> {
            record = value;
            present = true;
        });
        this.root = new GroupConverter() {
            @Override
            public Converter getConverter(int fieldIndex) {
                if (fieldIndex != 0) {
                    throw new IllegalArgumentException("root has one field, asked for index " + fieldIndex);
                }
                return child;
            }

            @Override
            public void start() {
                record = null;
                present = false;
            }

            @Override
            public void end() {}
        };
    }

    @Override
    public BytesRef getCurrentRecord() {
        // An absent top-level group is a null value rather than the four characters "null": no converter
        // under the root ever fired, so nothing set present.
        return present ? new BytesRef(record) : null;
    }

    @Override
    public GroupConverter getRootConverter() {
        return root;
    }

    private static Converter converterFor(Type type, Consumer<String> sink) {
        if (type.isPrimitive()) {
            return new ScalarConverter(sink);
        }
        GroupType group = type.asGroupType();
        LogicalTypeAnnotation annotation = group.getLogicalTypeAnnotation();
        if (annotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
            return new ListConverter(group, sink);
        }
        if (annotation instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation
            || annotation instanceof LogicalTypeAnnotation.MapKeyValueTypeAnnotation) {
            return new MapConverter(group, sink);
        }
        return new StructConverter(group, sink);
    }

    /** A leaf. Renders the value as a JSON scalar and hands it to the enclosing group's slot. */
    private static final class ScalarConverter extends PrimitiveConverter {
        private final Consumer<String> sink;

        ScalarConverter(Consumer<String> sink) {
            this.sink = sink;
        }

        @Override
        public void addBinary(Binary value) {
            sink.accept(quote(value.toStringUsingUTF8()));
        }

        @Override
        public void addBoolean(boolean value) {
            sink.accept(Boolean.toString(value));
        }

        @Override
        public void addDouble(double value) {
            sink.accept(Double.toString(value));
        }

        @Override
        public void addFloat(float value) {
            sink.accept(Float.toString(value));
        }

        @Override
        public void addInt(int value) {
            sink.accept(Integer.toString(value));
        }

        @Override
        public void addLong(long value) {
            sink.accept(Long.toString(value));
        }
    }

    /**
     * A group with named fields. Buffers one rendered value per field and writes them in schema order at
     * {@code end()}, so an absent field becomes an explicit null in its declared position rather than a
     * missing key or a key out of order.
     */
    private static final class StructConverter extends GroupConverter {
        private final GroupType type;
        private final Converter[] children;
        private final String[] slots;
        private final Consumer<String> sink;

        StructConverter(GroupType type, Consumer<String> sink) {
            this.type = type;
            this.sink = sink;
            int n = type.getFieldCount();
            this.children = new Converter[n];
            this.slots = new String[n];
            for (int i = 0; i < n; i++) {
                int slot = i;
                children[i] = converterFor(type.getType(i), value -> slots[slot] = value);
            }
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return children[fieldIndex];
        }

        @Override
        public void start() {
            Arrays.fill(slots, null);
        }

        @Override
        public void end() {
            StringBuilder sb = new StringBuilder("{");
            for (int i = 0; i < slots.length; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(quote(type.getType(i).getName())).append(':').append(slots[i] == null ? "null" : slots[i]);
            }
            sink.accept(sb.append('}').toString());
        }
    }

    /**
     * The {@code LIST}-annotated group. Its single child is the repeated node; each repetition of that node
     * contributes one element, and a repetition carrying no element contributes a JSON null — which is how a
     * null inside an array arrives, and the thing a multivalue column cannot represent.
     */
    private static final class ListConverter extends GroupConverter {
        private final RepeatedElementConverter repeated;
        private final List<String> elements = new ArrayList<>();
        private final Consumer<String> sink;

        ListConverter(GroupType type, Consumer<String> sink) {
            this.sink = sink;
            this.repeated = new RepeatedElementConverter(requireThreeLevel(type), elements);
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            if (fieldIndex != 0) {
                throw new IllegalArgumentException("a LIST group has one field, asked for index " + fieldIndex);
            }
            return repeated;
        }

        @Override
        public void start() {
            elements.clear();
        }

        @Override
        public void end() {
            sink.accept(join('[', elements, ']'));
        }
    }

    /** One repetition of a list's repeated node: zero or one element, the zero case being a null entry. */
    private static final class RepeatedElementConverter extends GroupConverter {
        private final Converter element;
        private final List<String> elements;
        private String value;

        RepeatedElementConverter(GroupType repeated, List<String> elements) {
            this.elements = elements;
            this.element = converterFor(repeated.getType(0), v -> value = v);
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            if (fieldIndex != 0) {
                throw new IllegalArgumentException("a list's repeated node has one field, asked for index " + fieldIndex);
            }
            return element;
        }

        @Override
        public void start() {
            value = null;
        }

        @Override
        public void end() {
            elements.add(value == null ? "null" : value);
        }
    }

    /**
     * The {@code MAP}-annotated group. Keys come from the data rather than the schema, so the rendered
     * object's key set differs from row to row — the property that makes a map unlike a struct and the
     * reason a fixed-shape block cannot hold one.
     */
    private static final class MapConverter extends GroupConverter {
        private final KeyValueConverter keyValue;
        private final List<String> entries = new ArrayList<>();
        private final Consumer<String> sink;

        MapConverter(GroupType type, Consumer<String> sink) {
            this.sink = sink;
            if (type.getFieldCount() != 1) {
                throw new IllegalArgumentException("a MAP group has one repeated field, found " + type.getFieldCount() + ": " + type);
            }
            GroupType kv = type.getType(0).asGroupType();
            if (kv.getFieldCount() != 2) {
                throw new IllegalArgumentException("a map's key_value has two fields, found " + kv.getFieldCount() + ": " + kv);
            }
            this.keyValue = new KeyValueConverter(kv, entries);
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            if (fieldIndex != 0) {
                throw new IllegalArgumentException("a MAP group has one field, asked for index " + fieldIndex);
            }
            return keyValue;
        }

        @Override
        public void start() {
            entries.clear();
        }

        @Override
        public void end() {
            sink.accept(join('{', entries, '}'));
        }
    }

    /** One map entry. The key is rendered as a JSON member name; an absent value becomes an explicit null. */
    private static final class KeyValueConverter extends GroupConverter {
        private final Converter keyConverter;
        private final Converter valueConverter;
        private final List<String> entries;
        private String key;
        private String value;

        KeyValueConverter(GroupType kv, List<String> entries) {
            this.entries = entries;
            // The key is already a quoted JSON string by the time the scalar converter is done with it,
            // which is exactly the form a member name needs.
            this.keyConverter = converterFor(kv.getType(0), k -> key = k);
            this.valueConverter = converterFor(kv.getType(1), v -> value = v);
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return switch (fieldIndex) {
                case 0 -> keyConverter;
                case 1 -> valueConverter;
                default -> throw new IllegalArgumentException("a map's key_value has two fields, asked for index " + fieldIndex);
            };
        }

        @Override
        public void start() {
            key = null;
            value = null;
        }

        @Override
        public void end() {
            entries.add(key + ":" + (value == null ? "null" : value));
        }
    }

    /**
     * Rejects the two-level list encoding. In the three-level form the {@code LIST} group holds one repeated
     * group which holds the element; in the two-level form the repeated node is the element itself, and
     * distinguishing them needs the specification's naming rules rather than the annotation.
     */
    private static GroupType requireThreeLevel(GroupType list) {
        if (list.getFieldCount() != 1) {
            throw new IllegalArgumentException("a LIST group has one repeated field, found " + list.getFieldCount() + ": " + list);
        }
        Type repeated = list.getType(0);
        if (repeated.isPrimitive() || repeated.asGroupType().getFieldCount() != 1) {
            throw new UnsupportedOperationException(
                "only the three-level LIST encoding is supported; [" + list.getName() + "] uses the two-level form: " + list
            );
        }
        return repeated.asGroupType();
    }

    private static String join(char open, List<String> parts, char close) {
        StringBuilder sb = new StringBuilder().append(open);
        for (int i = 0; i < parts.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(parts.get(i));
        }
        return sb.append(close).toString();
    }

    /** Renders a JSON string literal, escaping the characters the grammar requires to be escaped. */
    private static String quote(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 2).append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }
}
