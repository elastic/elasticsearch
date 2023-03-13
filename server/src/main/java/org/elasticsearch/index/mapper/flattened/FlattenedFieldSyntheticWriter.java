/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
class FlattenedFieldSyntheticWriter {
    private final SortedSetDocValues dv;

    FlattenedFieldSyntheticWriter(final SortedSetDocValues dv) {
        this.dv = dv;
    }

    void write(final XContentBuilder b) throws IOException {
        BytesRef currKeyValue = dv.lookupOrd(dv.nextOrd());
        String currKey = FlattenedFieldParser.extractKey(currKeyValue).utf8ToString();
        String currValue = FlattenedFieldParser.extractValue(currKeyValue).utf8ToString();
        List<String> currPrefix = prefix(currKey);
        List<String> prevPrefix = Collections.emptyList();
        final List<String> values = new ArrayList<>();
        values.add(currValue);
        for (int i = 1; i < dv.docValueCount(); i++) {
            BytesRef nextKeyValue = dv.lookupOrd(dv.nextOrd());
            String nextKey = FlattenedFieldParser.extractKey(nextKeyValue).utf8ToString();
            List<String> nextPrefix = prefix(nextKey);
            List<String> startPrefix = prefixDiff(currPrefix, commonPrefix(currPrefix, prevPrefix));
            List<String> endPrefix = prefixDiff(currPrefix, commonPrefix(currPrefix, nextPrefix));
            Collections.reverse(endPrefix);
            List<String> currSuffix = suffix(currKey);
            List<String> nextSuffix = suffix(nextKey);
            startObject(b, startPrefix);
            if (currSuffix.equals(nextSuffix) == false) {
                writeObject(b, values, currSuffix);
            }
            endObject(b, endPrefix);
            values.add(FlattenedFieldParser.extractValue(nextKeyValue).utf8ToString());
            currKey = nextKey;
            prevPrefix = currPrefix;
            currPrefix = nextPrefix;
        }
        if (values.isEmpty() == false) {
            List<String> startPrefix = prefixDiff(currPrefix, commonPrefix(currPrefix, prevPrefix));
            Collections.reverse(currPrefix);
            List<String> currSuffix = suffix(currKey);
            startObject(b, startPrefix);
            writeObject(b, values, currSuffix);
            endObject(b, currPrefix);
        }
    }

    private static List<String> prefix(final String key) {
        final String[] keyAsArray = key.split("\\.");
        return new ArrayList<>(Arrays.asList(keyAsArray).subList(0, keyAsArray.length - 1));
    }

    private static List<String> suffix(final String key) {
        final String[] keyAsArray = key.split("\\.");
        return new ArrayList<>(Arrays.asList(keyAsArray).subList(keyAsArray.length - 1, keyAsArray.length));
    }

    private static List<String> commonPrefix(final List<String> curr, final List<String> next) {
        List<String> common = new ArrayList<>();
        for (int i = 0; i < Math.min(curr.size(), next.size()); i++) {
            if (curr.get(i).equals(next.get(i))) {
                common.add(curr.get(i));
            }
        }

        return common;
    }

    private static void writeObject(final XContentBuilder b, final List<String> values, final List<String> currSuffix) throws IOException {
        for (int i = 0; i < currSuffix.size() - 1; i++) {
            b.startObject(currSuffix.get(i));
        }
        writeField(b, values, currSuffix);
        for (int i = 0; i < currSuffix.size() - 1; i++) {
            b.endObject();
        }
        values.clear();
    }

    private static List<String> prefixDiff(final List<String> a, final List<String> b) {
        if (a.size() > b.size()) {
            return prefixDiff(b, a);
        }
        final List<String> diff = new ArrayList<>();
        if (a.isEmpty()) {
            diff.addAll(b);
            return diff;
        }
        int i = 0;
        for (; i < a.size(); i++) {
            if (a.get(i).equals(b.get(i)) == false) {
                break;
            }
        }
        for (; i < b.size(); i++) {
            diff.add(b.get(i));
        }
        return diff;
    }

    private static void endObject(final XContentBuilder b, final List<String> objects) throws IOException {
        for (final String ignored : objects) {
            b.endObject();
        }
    }

    private static void startObject(final XContentBuilder b, final List<String> objects) throws IOException {
        for (final String object : objects) {
            b.startObject(object);
        }
    }

    private static void writeField(XContentBuilder b, List<String> values, List<String> currSuffix) throws IOException {
        if (values.size() > 1) {
            b.field(currSuffix.get(currSuffix.size() - 1), values);
        } else {
            // NOTE: here we make the assumption that fields with just one value are not arrays.
            // Flattened fields have no mappings, and even if we had mappings, there is no way
            // in Elasticsearch to distinguish single valued fields from multi-valued fields.
            // As a result, there is no way to know, after reading a single value, if that value
            // is the value for a single-valued field or a multi-valued field (array) with just
            // one value (array of size 1).
            b.field(currSuffix.get(currSuffix.size() - 1), values.get(0));
        }
    }
}
