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
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
 *         "to": "Atlanta"
 *     }
 * }`
 *
 */
public class FlattenedFieldSyntheticWriterHelper {

    private record Prefix(List<String> prefix) {

        Prefix() {
            this(Collections.emptyList());
        }

        Prefix(String[] keyAsArray) {
            this(List.of(keyAsArray).subList(0, keyAsArray.length - 1));
        }

        private Prefix shared(final Prefix other) {
            return shared(this.prefix, other.prefix);
        }

        private static Prefix shared(final List<String> curr, final List<String> next) {
            final List<String> shared = new ArrayList<>();
            for (int i = 0; i < Math.min(curr.size(), next.size()); i++) {
                if (curr.get(i).equals(next.get(i))) {
                    shared.add(curr.get(i));
                }
            }

            return new Prefix(shared);
        }

        private Prefix diff(final Prefix other) {
            return diff(this.prefix, other.prefix);
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

        @Override
        public int hashCode() {
            return Objects.hash(this.prefix);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Prefix other = (Prefix) obj;
            return Objects.equals(this.prefix, other.prefix);
        }
    }

    private static class KeyValue {

        public static final KeyValue EMPTY = new KeyValue(null, new Prefix(), null);
        private final String value;
        private final Prefix prefix;
        private final String leaf;

        private KeyValue(final String value, final Prefix prefix, final String leaf) {
            this.value = value;
            this.prefix = prefix;
            this.leaf = leaf;
        }

        KeyValue(final BytesRef keyValue) {
            this(
                FlattenedFieldParser.extractKey(keyValue).utf8ToString().split("\\."),
                FlattenedFieldParser.extractValue(keyValue).utf8ToString()
            );
        }

        private KeyValue(final String[] key, final String value) {
            this(value, new Prefix(key), key[key.length-1]);
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

    public interface SortedKeyedValues {
        BytesRef next() throws IOException;
    }

    private final SortedKeyedValues sortedKeyedValues;

    public FlattenedFieldSyntheticWriterHelper(final SortedKeyedValues sortedKeyedValues) {
        this.sortedKeyedValues = sortedKeyedValues;
    }

    private String concatPath(Prefix prefix, String leaf) {
        StringBuilder builder = new StringBuilder();
        for (String part : prefix.prefix) {
            builder.append(part).append(".");
        }
        builder.append(leaf);
        return builder.toString();
    }

    public void write(final XContentBuilder b) throws IOException {
        KeyValue curr = new KeyValue(sortedKeyedValues.next());
        final List<String> values = new ArrayList<>();
        List<String> openObjects = new ArrayList<>();
        String leafInSameObjectWithSamePrefix = null;
        KeyValue next;

        do {
            values.add(curr.value());
            BytesRef nextValue = sortedKeyedValues.next();
            next = nextValue == null ? KeyValue.EMPTY : new KeyValue(nextValue);

            // curr prefix is at least as long as openObjects
            var startPrefix = curr.prefix.diff(new Prefix(openObjects));

            boolean scalarObjectMismatch = startPrefix.prefix.isEmpty() == false
                && startPrefix.prefix.getFirst().equals(leafInSameObjectWithSamePrefix);
            if (scalarObjectMismatch == false) {
                startObject(b, startPrefix.prefix, openObjects);
            }
            if (curr.pathEquals(next) == false) {
                if (scalarObjectMismatch) {
                    writeField(b, values, concatPath(startPrefix, curr.leaf()));
                } else {
                    leafInSameObjectWithSamePrefix = curr.leaf();
                    writeField(b, values, curr.leaf());
                }
            }

            var openPrefix = new Prefix(openObjects);
            var endPrefix = openPrefix.diff(openPrefix.shared(next.prefix));
            int numObjectsToClose = endPrefix.prefix.size();
            endObject(b, numObjectsToClose, openObjects);

            curr = next;
        } while (next.equals(KeyValue.EMPTY) == false);
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

    private static void writeField(XContentBuilder b, List<String> values, String leaf) throws IOException {
        if (values.size() > 1) {
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
        values.clear();
    }
}
