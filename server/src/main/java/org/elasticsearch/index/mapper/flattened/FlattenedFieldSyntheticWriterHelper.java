/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A helper class that reconstructs the field including keys and values
 * mapped by {@link FlattenedFieldMapper}. It looks at key/value pairs,
 * parses them and uses the {@link XContentBuilder} to reconstruct the
 * original object.
 *
 * Example: given the following set of sorted key value pairs read from
 * doc values:
 *
 * "id": "AAAb12"
 * "package.id": "102938"
 * "root.asset_code": "abc"
 * "root.from": "New York"
 * "root.object.value": "10"
 * "root.object.items": "102202929"
 * "root.object.items": "290092911"
 * "root.to": "Atlanta"
 * "root.to.state": "Georgia"
 *
 * it turns them into the corresponding object using {@link XContentBuilder}.
 * If, for instance JSON is used, it generates the following after deserialization:
 *
 * `{
 *     "id": "AAAb12",
 *     "package": {
 *         "id": "102938"
 *     },
 *     "root": {
 *         "asset_code": "abc",
 *         "from": "New York",
 *         "object": {
 *             "items": ["102202929", "290092911"],
 *         },
 *         "to": "Atlanta",
 *         "to.state": "Georgia"
 *     }
 * }`
 *
 */
public class FlattenedFieldSyntheticWriterHelper {

    private static final String PATH_SEPARATOR = ".";
    private static final String PATH_SEPARATOR_PATTERN = "\\.";

    /**
     * A producer that yields the fields of the current document in ascending path order. Each field carries its
     * key path and the full document-order list of values (with {@code null} elements representing JSON nulls).
     */
    @FunctionalInterface
    public interface KeyedValueProducer {
        /**
         * Returns the next field in ascending path order, or {@code null} when exhausted.
         */
        OrderedField next() throws IOException;
    }

    /**
     * A single field as presented to the writer: a parsed key path and its document-order value list. The list may
     * contain {@code null} elements representing JSON null slots, and is never empty.
     */
    public record OrderedField(FlattenedKey key, List<String> values) {}

    private record Prefix(List<String> parts) {

        Prefix() {
            this(new ArrayList<>());
        }

        Prefix(String[] keyAsArray) {
            this(List.of(keyAsArray).subList(0, keyAsArray.length - 1));
        }

        private static int numObjectsToEnd(final List<String> curr, final List<String> next) {
            int i = 0;
            for (; i < Math.min(curr.size(), next.size()); i++) {
                if (curr.get(i).equals(next.get(i)) == false) {
                    break;
                }
            }
            return Math.max(0, curr.size() - i);
        }

        private Prefix diff(final Prefix other) {
            return diff(this.parts, other.parts);
        }

        private static Prefix diff(final List<String> a, final List<String> b) {
            assert a.size() >= b.size();
            int i = 0;
            for (; i < b.size(); i++) {
                if (a.get(i).equals(b.get(i)) == false) {
                    break;
                }
            }
            final List<String> diff = new ArrayList<>();
            for (; i < a.size(); i++) {
                diff.add(a.get(i));
            }
            return new Prefix(diff);
        }
    }

    /**
     * Represents a flattened field key as a parsed path: full dotted path, prefix segments, and leaf name.
     * Used internally to reconstruct nested object structure during writing.
     */
    record FlattenedKey(String fullPath, Prefix prefix, String leaf) {

        FlattenedKey(final String fullPath) {
            // Splitting with a negative limit includes trailing empty strings.
            // This is needed in case the path has trailing path separators.
            this(fullPath, fullPath.split(PATH_SEPARATOR_PATTERN, -1));
        }

        private FlattenedKey(final String fullPath, final String[] parts) {
            this(fullPath, new Prefix(parts), parts[parts.length - 1]);
        }

        static FlattenedKey fromBytesRef(final BytesRef keyValue) {
            return keyValue == null ? null : new FlattenedKey(FlattenedFieldParser.extractKey(keyValue).utf8ToString());
        }

        public boolean pathEquals(final FlattenedKey other) {
            return prefix.equals(other.prefix) && leaf.equals(other.leaf);
        }
    }

    /**
     * A {@link KeyedValueProducer} that decodes the legacy flattened-field representation: a sorted, deduplicated
     * {@link SortedKeyedValues} stream paired with a {@link SortedOffsetValues} sidecar. The sidecar maps each
     * field's sorted-unique value list back to the original document-order array (with {@code -1} for null slots and
     * repeated ordinals for duplicate values). When no sidecar entry exists for a field, values are returned in their
     * sorted order.
     */
    public static class OffsetKeyedValueProducer implements KeyedValueProducer {

        private final SortedKeyedValues sortedKeyedValues;
        private final SortedOffsetValues sortedOffsetValues;

        private boolean initialized = false;
        private FlattenedKey peekKey;
        private String peekValue;
        private FlattenedFieldArrayContext.KeyedOffsetField peekOffsets;

        public OffsetKeyedValueProducer(final SortedKeyedValues sortedKeyedValues, final SortedOffsetValues sortedOffsetValues) {
            this.sortedKeyedValues = sortedKeyedValues;
            this.sortedOffsetValues = sortedOffsetValues;
        }

        public void reset() {
            initialized = false;
            peekKey = null;
            peekValue = null;
            peekOffsets = null;
        }

        private void advanceKey() throws IOException {
            BytesRef raw = sortedKeyedValues.next();
            if (raw != null) {
                peekKey = FlattenedKey.fromBytesRef(raw);
                peekValue = FlattenedFieldParser.extractValue(raw).utf8ToString();
            } else {
                peekKey = null;
                peekValue = null;
            }
        }

        private FlattenedFieldArrayContext.KeyedOffsetField consumeOffsets() throws IOException {
            var ret = peekOffsets;
            peekOffsets = sortedOffsetValues.next();
            return ret;
        }

        private FlattenedKey consumeValues(List<String> values) throws IOException {
            var curr = peekKey;
            values.add(peekValue);
            advanceKey();

            // Gather all values with the same path into a list so they can be written to a field together.
            while (peekKey != null && curr.pathEquals(peekKey)) {
                values.add(peekValue);
                advanceKey();
            }

            return curr;
        }

        @Override
        public OrderedField next() throws IOException {
            if (initialized == false) {
                initialized = true;
                advanceKey();
                peekOffsets = sortedOffsetValues.next();
            }
            if (peekKey == null && peekOffsets == null) {
                return null;
            }

            if (peekKey == null) {
                // Offsets without values: all-null array.
                var offsets = consumeOffsets();
                return new OrderedField(new FlattenedKey(offsets.fieldName()), nullList(offsets.offsets().length));
            }

            if (peekOffsets == null) {
                // Values without offsets: return sorted values directly (single-valued or no-null multi-valued).
                List<String> values = new ArrayList<>();
                FlattenedKey key = consumeValues(values);
                return new OrderedField(key, values);
            }

            int comparison = peekOffsets.fieldName().compareTo(peekKey.fullPath());
            if (comparison < 0) {
                // Offset precedes next value: all-null array.
                var offsets = consumeOffsets();
                return new OrderedField(new FlattenedKey(offsets.fieldName()), nullList(offsets.offsets().length));
            } else if (comparison > 0) {
                // Value precedes next offset: return sorted values directly.
                List<String> values = new ArrayList<>();
                FlattenedKey key = consumeValues(values);
                return new OrderedField(key, values);
            } else {
                // Matching path: expand the sorted-unique value list into document order using the offset permutation.
                var offsets = consumeOffsets();
                List<String> values = new ArrayList<>();
                FlattenedKey key = consumeValues(values);
                assert offsets.fieldName().equals(key.fullPath());
                List<String> ordered = new ArrayList<>(offsets.offsets().length);
                for (int ord : offsets.offsets()) {
                    ordered.add(ord == -1 ? null : values.get(ord));
                }
                return new OrderedField(key, ordered);
            }
        }

        private static List<String> nullList(int length) {
            List<String> list = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                list.add(null);
            }
            return list;
        }
    }

    private interface FlattenedPathXContentWriter {
        void writeValue(XContentBuilder b, OrderedField value, OrderedField next) throws IOException;
    }

    /**
     * {@link FlattenedPathXContentWriter} implementation that splits each key on
     * {@code .} and opens {@link XContentBuilder} object scopes to reconstruct nested structure from sorted path segments.
     * <p>
     * It keeps track of which object levels are open and how far into the current path each field belongs, and uses
     * {@code next} to close the right number of objects when the prefix of the following key changes.
     * <p>
     * When a leaf already had a scalar value and a later key extends that path (e.g. {@code foo} then {@code foo.bar}),
     * a nested object under {@code foo} would collide with the scalar. The writer then emits a single dotted field
     * name in the current object (for example {@code foo.bar}) instead of opening another object under {@code foo}.
     */
    private static class NestedFlattenedPathXContentWriter implements FlattenedPathXContentWriter {
        Prefix openObjects = new Prefix();
        String lastScalarSingleLeaf = null;

        @Override
        public void writeValue(XContentBuilder b, OrderedField value, OrderedField next) throws IOException {
            FlattenedKey kv = value.key();
            // startPrefix is the suffix of the path that is within the currently open object, not including the leaf.
            // For example, if the path is foo.bar.baz.qux, and openObjects is [foo], then startPrefix is bar.baz, and leaf is qux.
            var startPrefix = kv.prefix.diff(openObjects);
            if (startPrefix.parts.isEmpty() == false && startPrefix.parts.getFirst().equals(lastScalarSingleLeaf)) {
                // We have encountered a key with an object value, which already has a scalar value. Instead of creating an object for the
                // key, we concatenate the key and all child keys going down to the current leaf into a single field name. For example:
                // Assume the current object contains "foo": 10 and "foo.bar": 20. Since key-value pairs are sorted, "foo" is processed
                // first. When writing the field "foo", `lastScalarSingleLeaf` is set to "foo" because it has a scalar value. Next, when
                // processing "foo.bar", we check if `lastScalarSingleLeaf` ("foo") is a prefix of "foo.bar". Since it is, this indicates a
                // conflict: a scalar value and an object share the same key ("foo"). To disambiguate, we create a concatenated key
                // "foo.bar" with the value 20 in the current object, rather than creating a nested object as usual.
                writeField(b, value.values(), concatPath(startPrefix, kv.leaf()));
            } else {
                // Since there is not an existing key in the object that is a prefix of the path, we can traverse down into the path
                // and open objects. After opening all objects in the path, write out the field with only the current leaf as the key.
                // Finally, save the current leaf in `lastScalarSingleLeaf`, in case there is a future object within the recently opened
                // object which has the same key as the current leaf.
                startObject(b, startPrefix.parts, openObjects.parts);
                writeField(b, value.values(), kv.leaf());
                lastScalarSingleLeaf = kv.leaf();
            }

            List<String> nextPrefixParts = next == null ? List.of() : next.key().prefix.parts;
            int numObjectsToEnd = Prefix.numObjectsToEnd(openObjects.parts, nextPrefixParts);
            endObject(b, numObjectsToEnd, openObjects.parts);
        }
    }

    /**
     * {@link FlattenedPathXContentWriter} implementation that writes each key with its full dotted path as a single
     * top-level field name, without opening nested objects.
     * <p>
     * Unlike {@link NestedFlattenedPathXContentWriter}, it does not split paths on {@code .} or track open object
     * scopes; {@code next} is ignored because flattening is stateless.
     */
    private static class FlatFlattenedPathXContentWriter implements FlattenedPathXContentWriter {
        @Override
        public void writeValue(XContentBuilder b, OrderedField value, OrderedField next) throws IOException {
            writeField(b, value.values(), value.key().fullPath());
        }
    }

    @FunctionalInterface
    public interface SortedKeyedValues {
        BytesRef next() throws IOException;
    }

    @FunctionalInterface
    public interface SortedOffsetValues {
        SortedOffsetValues NONE = () -> null;

        FlattenedFieldArrayContext.KeyedOffsetField next() throws IOException;
    }

    private final KeyedValueProducer producer;
    private final List<Map.Entry<String, SourceLoader.SyntheticFieldLoader>> sortedSubFields;

    public FlattenedFieldSyntheticWriterHelper(
        final KeyedValueProducer producer,
        final List<Map.Entry<String, SourceLoader.SyntheticFieldLoader>> sortedSubFields
    ) {
        this.producer = producer;
        this.sortedSubFields = sortedSubFields;
    }

    private static String concatPath(Prefix prefix, String leaf) {
        StringBuilder builder = new StringBuilder();
        for (String part : prefix.parts) {
            builder.append(part).append(PATH_SEPARATOR);
        }
        builder.append(leaf);
        return builder.toString();
    }

    /**
     * Writes all key-value pairs from doc values using a flat output structure, merging in
     * the sub-field loaders supplied at construction time in alphabetical order.
     * <p>
     * Both the doc-values key stream and the sub-field list are in ascending lexicographic order;
     * the two streams are merged in a single pass so all keys appear in sorted order. The flat
     * writer is stateless (no object-scope tracking), making interleaving safe at any point.
     */
    public void writeFlattened(final XContentBuilder b) throws IOException {
        FlatFlattenedPathXContentWriter flatWriter = new FlatFlattenedPathXContentWriter();
        int subIdx = 0;
        OrderedField curr = producer.next();
        while (curr != null || subIdx < sortedSubFields.size()) {
            String currKey = curr != null ? curr.key().fullPath() : null;
            String nextSubKey = subIdx < sortedSubFields.size() ? sortedSubFields.get(subIdx).getKey() : null;
            if (currKey == null || (nextSubKey != null && nextSubKey.compareTo(currKey) <= 0)) {
                sortedSubFields.get(subIdx).getValue().write(b);
                subIdx++;
            } else {
                flatWriter.writeValue(b, curr, null);
                curr = producer.next();
            }
        }
    }

    /**
     * Writes all key-value pairs from doc values by reconstructing nested objects, then appends
     * the sub-field loaders supplied at construction time at the top level after all objects have
     * been closed.
     */
    public void writeNested(final XContentBuilder b) throws IOException {
        NestedFlattenedPathXContentWriter writer = new NestedFlattenedPathXContentWriter();
        OrderedField curr = producer.next();
        OrderedField next = producer.next();
        while (curr != null) {
            writer.writeValue(b, curr, next);
            curr = next;
            next = producer.next();
        }
        for (Map.Entry<String, SourceLoader.SyntheticFieldLoader> entry : sortedSubFields) {
            entry.getValue().write(b);
        }
    }

    private static void endObject(final XContentBuilder b, int numObjectsToClose, List<String> openObjects) throws IOException {
        for (int i = 0; i < numObjectsToClose; i++) {
            b.endObject();
            openObjects.removeLast();
        }
    }

    private static void startObject(final XContentBuilder b, final List<String> objects, List<String> openObjects) throws IOException {
        for (final String object : objects) {
            b.startObject(object);
            openObjects.add(object);
        }
    }

    /**
     * A {@link KeyedValueProducer} for the document-order (columnar) path. Iterates per-key slot lists in ascending
     * key order, preserving document order, duplicates, and nulls without sorting or deduplication. Ignored values
     * (from {@code ignore_above}) are tail-appended per key in sorted order.
     */
    public static final class ArrayOrderKeyedValueProducer implements KeyedValueProducer {

        private final List<OrderedField> fields;
        private int idx = 0;

        public ArrayOrderKeyedValueProducer(TreeMap<String, List<String>> slotsByKey, TreeMap<String, List<String>> ignoredByKey) {
            TreeSet<String> allKeys = new TreeSet<>(slotsByKey.keySet());
            allKeys.addAll(ignoredByKey.keySet());
            fields = new ArrayList<>(allKeys.size());
            for (String key : allKeys) {
                List<String> values = new ArrayList<>();
                values.addAll(slotsByKey.getOrDefault(key, Collections.emptyList()));
                // ignoredByKey values come from a TreeSet iteration so they are already sorted.
                values.addAll(ignoredByKey.getOrDefault(key, Collections.emptyList()));
                fields.add(new OrderedField(new FlattenedKey(key), values));
            }
        }

        @Override
        public OrderedField next() {
            return idx < fields.size() ? fields.get(idx++) : null;
        }
    }

    private static void writeField(XContentBuilder b, List<String> values, String leaf) throws IOException {
        if (values.size() > 1) {
            b.startArray(leaf);
            for (String v : values) {
                if (v == null) {
                    b.nullValue();
                } else {
                    b.value(v);
                }
            }
            b.endArray();
        } else {
            String v = values.getFirst();
            if (v == null) {
                b.field(leaf);
                b.nullValue();
            } else {
                b.field(leaf, v);
            }
        }
    }
}
