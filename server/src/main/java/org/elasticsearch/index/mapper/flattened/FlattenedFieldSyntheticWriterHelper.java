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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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

    private static class KeyValue {
        private final String fullPath;
        private final String value;
        private final Prefix prefix;
        private final String leaf;

        private KeyValue(final String fullPath, final String value, final Prefix prefix, final String leaf) {
            this.fullPath = fullPath;
            this.value = value;
            this.prefix = prefix;
            this.leaf = leaf;
        }

        KeyValue(final BytesRef keyValue) {
            this(FlattenedFieldParser.extractKey(keyValue).utf8ToString(), FlattenedFieldParser.extractValue(keyValue).utf8ToString());
        }

        private KeyValue(final String fullPath, final String value) {
            // Splitting with a negative limit includes trailing empty strings.
            // This is needed in case the provide path has trailing path separators.
            this(fullPath, fullPath.split(PATH_SEPARATOR_PATTERN, -1), value);
        }

        private KeyValue(String fullPath, final String[] key, final String value) {
            this(fullPath, value, new Prefix(key), key[key.length - 1]);
        }

        private static KeyValue fromBytesRef(final BytesRef keyValue) {
            return keyValue == null ? null : new KeyValue(keyValue);
        }

        public String fullPath() {
            return fullPath;
        }

        public String leaf() {
            return this.leaf;
        }

        public String value() {
            assert this.value != null;
            return this.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.value, this.prefix, this.leaf);
        }

        public boolean pathEquals(final KeyValue other) {
            return prefix.equals(other.prefix) && leaf.equals(other.leaf);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            KeyValue other = (KeyValue) obj;
            return Objects.equals(this.value, other.value)
                && Objects.equals(this.prefix, other.prefix)
                && Objects.equals(this.leaf, other.leaf);
        }
    }

    private record KeyValueWithOffset(KeyValue key, List<String> values, int[] offsets) {}

    /**
     * Merges two lexicographically sorted sources (flattened key/value pairs from doc values and per-field array
     * offset metadata) into a single sequence of {@link KeyValueWithOffset} records for synthetic field writing.
     * <p>
     * Doc values list each path once per stored value; consecutive entries with the same path are collapsed into one
     * step with a list of values. When offset metadata is present for the same path, it is paired with those values so
     * {@link FlattenedFieldSyntheticWriterHelper#writeField} can rebuild arrays including {@code null} slots.
     * Paths that appear only in the offset stream (all-null arrays) are emitted with a null value list and non-null offsets alone.
     */
    private static class KeyValueProducer {
        private final SortedKeyedValues sortedKeyedValues;
        private final SortedOffsetValues sortedOffsetValues;

        private KeyValue peekValue;
        private FlattenedFieldArrayContext.KeyedOffsetField peekOffsets;

        KeyValueProducer(final SortedKeyedValues sortedKeyedValues, final SortedOffsetValues sortedOffsetValues) {
            this.sortedKeyedValues = sortedKeyedValues;
            this.sortedOffsetValues = sortedOffsetValues;

            try {
                peekValue = KeyValue.fromBytesRef(sortedKeyedValues.next());
                peekOffsets = sortedOffsetValues.next();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private FlattenedFieldArrayContext.KeyedOffsetField consumeOffsets() throws IOException {
            var ret = peekOffsets;
            peekOffsets = sortedOffsetValues.next();
            return ret;
        }

        private KeyValue consumeValue(List<String> values) throws IOException {
            var curr = peekValue;
            var next = KeyValue.fromBytesRef(sortedKeyedValues.next());
            values.add(curr.value());

            // Gather all values with the same path into a list so they can be written to a field together.
            while (next != null && curr.pathEquals(next)) {
                curr = next;
                next = KeyValue.fromBytesRef(sortedKeyedValues.next());
                values.add(curr.value());
            }

            peekValue = next;
            return curr;
        }

        public KeyValueWithOffset next() throws IOException {
            if (peekValue == null && peekOffsets == null) {
                return null;
            }

            if (peekValue == null) {
                // We have consumed all values but there are still offsets, indicating null values
                var offsets = consumeOffsets();
                return new KeyValueWithOffset(new KeyValue(offsets.fieldName(), null), null, offsets.offsets());
            }

            if (peekOffsets == null) {
                // We have consumed all offsets and only single-valued values remain
                List<String> values = new ArrayList<>();
                var keyValue = consumeValue(values);
                return new KeyValueWithOffset(keyValue, values, null);
            }

            int comparison = peekOffsets.fieldName().compareTo(peekValue.fullPath);
            if (comparison < 0) {
                // Current offset is not associated with current value, and the current offset is lexicographically first
                // Offset with no associated value, must be all null
                var offsets = consumeOffsets();
                return new KeyValueWithOffset(new KeyValue(offsets.fieldName(), null), null, offsets.offsets());
            } else if (comparison > 0) {
                // Current offset is not associated with current value, and the current value is lexicographically first
                List<String> values = new ArrayList<>();
                var keyValue = consumeValue(values);
                return new KeyValueWithOffset(keyValue, values, null);
            } else {
                // Current offset is associated with current value
                var offsets = consumeOffsets();
                List<String> values = new ArrayList<>();
                var keyValue = consumeValue(values);
                assert offsets.fieldName().equals(keyValue.fullPath());
                return new KeyValueWithOffset(keyValue, values, offsets.offsets());
            }
        }
    }

    private interface FlattenedPathXContentWriter {
        void writeValue(XContentBuilder b, KeyValueWithOffset value, KeyValueWithOffset next) throws IOException;
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
        public void writeValue(XContentBuilder b, KeyValueWithOffset value, KeyValueWithOffset next) throws IOException {
            // startPrefix is the suffix of the path that is within the currently open object, not including the leaf.
            // For example, if the path is foo.bar.baz.qux, and openObjects is [foo], then startPrefix is bar.baz, and leaf is qux.
            var startPrefix = value.key.prefix.diff(openObjects);
            if (startPrefix.parts.isEmpty() == false && startPrefix.parts.getFirst().equals(lastScalarSingleLeaf)) {
                // We have encountered a key with an object value, which already has a scalar value. Instead of creating an object for the
                // key, we concatenate the key and all child keys going down to the current leaf into a single field name. For example:
                // Assume the current object contains "foo": 10 and "foo.bar": 20. Since key-value pairs are sorted, "foo" is processed
                // first. When writing the field "foo", `lastScalarSingleLeaf` is set to "foo" because it has a scalar value. Next, when
                // processing "foo.bar", we check if `lastScalarSingleLeaf` ("foo") is a prefix of "foo.bar". Since it is, this indicates a
                // conflict: a scalar value and an object share the same key ("foo"). To disambiguate, we create a concatenated key
                // "foo.bar" with the value 20 in the current object, rather than creating a nested object as usual.
                writeField(b, value.values, concatPath(startPrefix, value.key.leaf()), value.offsets);
            } else {
                // Since there is not an existing key in the object that is a prefix of the path, we can traverse down into the path
                // and open objects. After opening all objects in the path, write out the field with only the current leaf as the key.
                // Finally, save the current leaf in `lastScalarSingleLeaf`, in case there is a future object within the recently opened
                // object which has the same key as the current leaf.
                startObject(b, startPrefix.parts, openObjects.parts);
                writeField(b, value.values, value.key.leaf(), value.offsets);
                lastScalarSingleLeaf = value.key.leaf();
            }

            int numObjectsToEnd = Prefix.numObjectsToEnd(openObjects.parts, next == null ? List.of() : next.key.prefix.parts);
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
        public void writeValue(XContentBuilder b, KeyValueWithOffset value, KeyValueWithOffset next) throws IOException {
            writeField(b, value.values, value.key.fullPath(), value.offsets);
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

    private final SortedKeyedValues sortedKeyedValues;
    private final SortedOffsetValues sortedOffsetValues;
    private final List<Map.Entry<String, SourceLoader.SyntheticFieldLoader>> sortedSubFields;

    public FlattenedFieldSyntheticWriterHelper(
        final SortedKeyedValues sortedKeyedValues,
        final SortedOffsetValues sortedOffsetValues,
        final List<Map.Entry<String, SourceLoader.SyntheticFieldLoader>> sortedSubFields
    ) {
        this.sortedKeyedValues = sortedKeyedValues;
        this.sortedOffsetValues = sortedOffsetValues;
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
        KeyValueProducer producer = new KeyValueProducer(sortedKeyedValues, sortedOffsetValues);
        FlatFlattenedPathXContentWriter flatWriter = new FlatFlattenedPathXContentWriter();
        int subIdx = 0;
        KeyValueWithOffset curr = producer.next();
        while (curr != null || subIdx < sortedSubFields.size()) {
            String currKey = curr != null ? curr.key.fullPath() : null;
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
        KeyValueProducer producer = new KeyValueProducer(sortedKeyedValues, sortedOffsetValues);
        NestedFlattenedPathXContentWriter writer = new NestedFlattenedPathXContentWriter();
        KeyValueWithOffset curr = producer.next();
        KeyValueWithOffset next = producer.next();
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

    private static boolean sanityCheckOffsetsMatchValues(List<String> values, int[] offsetToOrd) {
        if (values == null) {
            // no values, offsets must be all-null
            for (int i = 0; i < offsetToOrd.length; i++) {
                // -1 represents a null value. See FieldArrayContext#encodeOffsetArray
                if (offsetToOrd[i] != -1) {
                    return false;
                }
            }
            return true;
        }

        Set<Integer> offsets = Arrays.stream(offsetToOrd).boxed().collect(Collectors.toSet());
        for (int i = 0; i < values.size(); i++) {
            if (offsets.contains(i) == false) {
                // We found a value that is not referenced by any offset, which should not be possible.
                // This usually indicates we are using the wrong offsets array for the values.
                return false;
            }
        }
        return true;
    }

    private static void writeField(XContentBuilder b, List<String> values, String leaf, int[] offsetToOrd) throws IOException {
        if (offsetToOrd != null) {
            assert sanityCheckOffsetsMatchValues(values, offsetToOrd);
            if (offsetToOrd.length == 1) {
                b.field(leaf);
            } else {
                b.startArray(leaf);
            }
            for (int offset : offsetToOrd) {
                if (offset == -1) {
                    b.nullValue();
                } else {
                    b.value(values.get(offset));
                }
            }
            if (offsetToOrd.length > 1) {
                b.endArray();
            }
        } else if (values.size() > 1) {
            b.field(leaf, values);
        } else {
            // NOTE: here we make the assumption that fields with just one value are not arrays.
            // Flattened fields have no mappings, and even if we had mappings, there is no way
            // in Elasticsearch to distinguish single valued fields from multi-valued fields.
            // As a result, there is no way to know, after reading a single value, if that value
            // is the value for a single-valued field or a multi-valued field (array) with just
            // one value (array of size 1).
            b.field(leaf, values.getFirst());
        }
    }
}
