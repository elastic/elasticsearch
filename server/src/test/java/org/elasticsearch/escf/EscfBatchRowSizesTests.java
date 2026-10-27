/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.sourcebatch.ArrayReader;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceRow;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

/**
 * Holds the column-major row sizes ({@link EscfBatch#rowSizesInBytes()} and {@link EscfColumn#sizeInBytes(int)}) to the
 * row-by-row formula they replaced, kept here as {@link #oracleRowSize} so the new code is not checked against itself. The
 * documents are encoded through {@link EscfEncoder} with every value kind it produces, so every column kind (including unions
 * and columnar arrays of each child kind) is sized, both on whole batches and on random slices.
 */
public class EscfBatchRowSizesTests extends ESTestCase {

    private static final String[] STRINGS = { "", "plain", "héllo wörld", "✓ 日本語 🚀", "a somewhat longer ascii string value" };

    /** The sizing formula of the former {@code EscfRow#sizeInBytes()}, applied through the unchanged {@link SourceRow} getters. */
    private static int oracleRowSize(EscfBatch batch, int doc) {
        final SourceRow row = batch.row(doc);
        int size = 0;
        for (int col = 0; col < batch.columnCount(); col++) {
            size += oracleCellSize(batch, row, col);
        }
        return size;
    }

    private static int oracleCellSize(EscfBatch batch, SourceRow row, int col) {
        final byte type = row.getTypeByte(col);
        if (type == SourceValueType.STRING
            || type == SourceValueType.BINARY
            || type == SourceValueType.UNION_ARRAY
            || type == SourceValueType.KEY_VALUE) {
            return row.getBinaryValue(col).length;
        }
        if (type == SourceValueType.FIXED_ARRAY) {
            return batch.column(col).kind() == EscfColumnKind.ARRAY
                ? oracleArraySize(row.getArrayValue(col))
                : row.getBinaryValue(col).length;
        }
        return Math.max(SourceValueType.elemDataSize(type), 0);
    }

    private static int oracleArraySize(ArrayReader array) {
        int size = 0;
        while (array.next()) {
            byte type = array.type();
            size += type == SourceValueType.STRING ? array.textValue().bytes().length() : Math.max(SourceValueType.elemDataSize(type), 0);
        }
        return size;
    }

    /** Pins the formula on hand-picked documents, so a change to it shows up as a concrete number and not only as a mismatch. */
    public void testKnownSizes() throws IOException {
        assertKnownSize("{\"s\":\"héllo\"}", 6);
        assertKnownSize("{\"s\":\"\"}", 0);
        assertKnownSize("{\"l\":1}", 8);
        assertKnownSize("{\"d\":1.5}", 8);
        assertKnownSize("{\"b\":true}", 0);
        assertKnownSize("{\"n\":null}", 0);
        assertKnownSize("{\"e\":{}}", 0);
        assertKnownSize("{}", 0);
        assertKnownSize("{\"a\":[1,2,3]}", 24);
        assertKnownSize("{\"a\":[1.5,2.5]}", 16);
        assertKnownSize("{\"a\":[\"a\",\"\",\"日本\"]}", 7);
        assertKnownSize("{\"o\":{\"k\":7,\"s\":\"ab\",\"i\":{\"z\":2.5}}}", 8 + 2 + 8);
    }

    private static void assertKnownSize(String json, int expected) throws IOException {
        try (EscfBatch batch = EscfEncoder.encode(List.of(new BytesArray(json)), XContentType.JSON)) {
            assertThat(json, oracleRowSize(batch, 0), equalTo(expected));
            assertThat(json, batch.rowSizesInBytes(), equalTo(new int[] { expected }));
            assertThat(json, batch.row(0).sizeInBytes(), equalTo(expected));
        }
    }

    public void testRandomDocuments() throws IOException {
        for (int iteration = 0; iteration < 10; iteration++) {
            final int docCount = randomIntBetween(1, 150);
            final List<BytesReference> sources = new ArrayList<>(docCount);
            for (int d = 0; d < docCount; d++) {
                sources.add(toJson(randomDocument()));
            }
            try (EscfBatch batch = EscfEncoder.encode(sources, XContentType.JSON)) {
                assertSizesMatchOracle(batch, sources);
                for (int s = 0; s < 5; s++) {
                    final int from = randomIntBetween(0, docCount);
                    final int to = randomIntBetween(from, docCount);
                    final SourceBatch slice = batch.slice(from, to);
                    assertSizesMatchOracle((EscfBatch) slice, sources.subList(from, to));
                    assertThat(
                        "slice [" + from + ", " + to + ")",
                        slice.rowSizesInBytes(),
                        equalTo(Arrays.copyOfRange(batch.rowSizesInBytes(), from, to))
                    );
                }
            }
        }
    }

    /**
     * Documents of one field whose kind changes from row to row, so the column is a union of every scalar and inline type. The
     * first two documents pin a string and a long, so the leaf {@code u} is a union whatever the random values are.
     */
    public void testUnionColumn() throws IOException {
        final int docCount = randomIntBetween(2, 100);
        final List<BytesReference> sources = new ArrayList<>(docCount);
        sources.add(toJson(Map.of("u", randomFrom(STRINGS))));
        sources.add(toJson(Map.of("u", randomLong())));
        for (int d = 2; d < docCount; d++) {
            final Map<String, Object> doc = new LinkedHashMap<>();
            if (randomBoolean()) {
                doc.put("u", randomUnionValue());
            }
            sources.add(toJson(doc));
        }
        try (EscfBatch batch = EscfEncoder.encode(sources, XContentType.JSON)) {
            boolean unionSeen = false;
            for (int col = 0; col < batch.columnCount(); col++) {
                if (batch.schema().getFullPath(col).equals("u")) {
                    assertThat(batch.column(col).kind(), equalTo(EscfColumnKind.UNION));
                    unionSeen = true;
                }
            }
            assertTrue("leaf [u] must be a union column", unionSeen);
            assertSizesMatchOracle(batch, sources);
        }
    }

    /** Many docs with a single sparse column, so the validity bitset spans several 64-bit words. */
    public void testSparseWideColumn() throws IOException {
        final int docCount = randomIntBetween(200, 600);
        final List<BytesReference> sources = new ArrayList<>(docCount);
        for (int d = 0; d < docCount; d++) {
            final Map<String, Object> doc = new LinkedHashMap<>();
            if (rarely()) {
                doc.put("v", randomFrom(STRINGS));
            }
            sources.add(toJson(doc));
        }
        try (EscfBatch batch = EscfEncoder.encode(sources, XContentType.JSON)) {
            assertSizesMatchOracle(batch, sources);
            final int from = randomIntBetween(0, docCount / 2);
            final int to = randomIntBetween(from, docCount);
            assertSizesMatchOracle((EscfBatch) batch.slice(from, to), sources.subList(from, to));
        }
    }

    private static void assertSizesMatchOracle(EscfBatch batch, List<BytesReference> sources) {
        final int[] sizes = batch.rowSizesInBytes();
        assertThat(sizes.length, equalTo(batch.docCount()));
        for (int d = 0; d < batch.docCount(); d++) {
            final String json = sources.get(d).utf8ToString();
            final SourceRow row = batch.row(d);
            for (int col = 0; col < batch.columnCount(); col++) {
                final EscfColumn column = batch.column(col);
                assertThat(
                    "column [" + batch.schema().getFullPath(col) + "] kind [" + EscfColumnKind.name(column.kind()) + "] of " + json,
                    column.sizeInBytes(d),
                    equalTo(oracleCellSize(batch, row, col))
                );
            }
            final int expected = oracleRowSize(batch, d);
            assertThat(json, sizes[d], equalTo(expected));
            assertThat(json, row.sizeInBytes(), equalTo(expected));
        }
    }

    /**
     * A document over a fixed set of fields, each left out at random so the columns are sparse, and some of them of a kind
     * that changes between documents so the column promotes to a union.
     */
    private static Map<String, Object> randomDocument() {
        final Map<String, Object> doc = new LinkedHashMap<>();
        maybePut(doc, "s", () -> randomFrom(STRINGS));
        maybePut(doc, "l", ESTestCase::randomLong);
        maybePut(doc, "d", ESTestCase::randomDouble);
        maybePut(doc, "b", ESTestCase::randomBoolean);
        maybePut(doc, "n", () -> null);
        maybePut(doc, "e", Map::of);
        maybePut(doc, "arr_s", () -> randomList(0, 6, () -> randomFrom(STRINGS)));
        maybePut(doc, "arr_l", () -> randomList(0, 6, ESTestCase::randomLong));
        maybePut(doc, "arr_d", () -> randomList(0, 6, ESTestCase::randomDouble));
        maybePut(doc, "arr_mixed", () -> Arrays.asList(1L, "two", 3.5, true, null));
        maybePut(doc, "arr_nested", () -> List.of(List.of(1L, 2L), List.of(3L)));
        maybePut(doc, "arr_obj", () -> List.of(Map.of("x", 1L), Map.of("y", "two")));
        maybePut(doc, "arr_promote", () -> randomBoolean() ? List.of(1L, 2L) : List.of("x", "yy"));
        maybePut(doc, "promote", EscfBatchRowSizesTests::randomUnionValue);
        maybePut(doc, "obj", () -> {
            final Map<String, Object> obj = new LinkedHashMap<>();
            maybePut(obj, "k", ESTestCase::randomLong);
            maybePut(obj, "s", () -> randomFrom(STRINGS));
            maybePut(obj, "inner", () -> Map.of("z", randomDouble()));
            return obj;
        });
        return doc;
    }

    /** A value of a different type on every call, so a column that holds it across documents becomes a union. */
    private static Object randomUnionValue() {
        return switch (randomIntBetween(0, 7)) {
            case 0 -> randomFrom(STRINGS);
            case 1 -> randomLong();
            case 2 -> randomDouble();
            case 3 -> randomBoolean();
            case 4 -> null;
            case 5 -> randomList(0, 4, ESTestCase::randomLong);
            case 6 -> Arrays.asList(1L, "two", 3.5, true, null);
            default -> Map.of("x", randomLong());
        };
    }

    private static void maybePut(Map<String, Object> doc, String field, Supplier<Object> value) {
        if (randomBoolean()) {
            doc.put(field, value.get());
        }
    }

    private static BytesReference toJson(Map<String, Object> doc) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent)) {
            builder.map(doc);
            return BytesReference.bytes(builder);
        }
    }
}
