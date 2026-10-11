/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.Counter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;

/**
 * Checks that {@link CompositeSourceColumn} applies the same scalar-or-array rule as {@link CompositeSyntheticFieldLoader#write}: zero
 * values write nothing, one value writes a scalar, more than one writes an array, and layers are written in order. These are the rules
 * the {@code columnar_stored} direct source path relies on to stay byte-for-byte with the loader path.
 */
public class CompositeSourceColumnTests extends ESTestCase {

    public void testAbsentFieldWritesNothing() throws IOException {
        // Two docs: doc 0 has one value, doc 1 has none. Doc 1 must produce an empty object, exactly as the loader emits {} for a
        // valueless document.
        CompositeSourceColumn column = keywordColumn("f", new int[] { 0, 1, 1 }, "a");
        assertEquals("{\"f\":\"a\"}", render(column, 0));
        assertEquals("{}", render(column, 1));
    }

    public void testSingleValueWritesScalar() throws IOException {
        CompositeSourceColumn column = keywordColumn("f", new int[] { 0, 1 }, "a");
        assertEquals("{\"f\":\"a\"}", render(column, 0));
    }

    public void testMultipleValuesWriteArrayPreservingOrderAndDuplicates() throws IOException {
        CompositeSourceColumn column = keywordColumn("f", new int[] { 0, 3 }, "b", "a", "b");
        assertEquals("{\"f\":[\"b\",\"a\",\"b\"]}", render(column, 0));
    }

    public void testLayersAreWrittenInOrderAndCountsAreSummed() throws IOException {
        // A primary layer with one value and a fallback layer with one value sum to two, so the field is an array and the fallback value
        // trails the primary value — the documented columnar_stored behaviour for e.g. an ignore_above original.
        BytesRefSourceColumn primary = bytesRefColumn(new int[] { 0, 1 }, "primary");
        BytesRefSourceColumn fallback = bytesRefColumn(new int[] { 0, 1 }, "fallback");
        CompositeSourceColumn column = new CompositeSourceColumn("f", "f", primary, fallback);
        assertEquals("{\"f\":[\"primary\",\"fallback\"]}", render(column, 0));
    }

    private static CompositeSourceColumn keywordColumn(String name, int[] starts, String... values) {
        return new CompositeSourceColumn(name, name, bytesRefColumn(starts, values));
    }

    private static BytesRefSourceColumn bytesRefColumn(int[] starts, String... values) {
        BytesRefArray array = new BytesRefArray(Counter.newCounter());
        for (String value : values) {
            array.append(new BytesRef(value));
        }
        return new BytesRefSourceColumn(starts, array);
    }

    private static String render(CompositeSourceColumn column, int doc) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            column.write(doc, builder);
            builder.endObject();
            return Strings.toString(builder);
        }
    }
}
