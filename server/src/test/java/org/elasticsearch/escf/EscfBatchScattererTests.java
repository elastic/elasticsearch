/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.sourcebatch.SourceRowToXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EscfBatchScattererTests extends ESTestCase {

    /**
     * Scalars: LONG, DOUBLE, STRING, BOOL columns with all rows present.
     * All rows go to partition 0 (partitionCount=1).
     */
    public void testSinglePartitionScalars() throws IOException {
        List<BytesReference> docs = List.of(
            json("{\"i\":1,\"d\":1.5,\"s\":\"hello\",\"b\":true}"),
            json("{\"i\":2,\"d\":2.5,\"s\":\"world\",\"b\":false}"),
            json("{\"i\":3,\"d\":3.5,\"s\":\"foo\",\"b\":true}")
        );
        try (EscfBatch source = encode(docs)) {
            int[] selectors = { 0, 0, 0 };
            assertRoundTrip(source, selectors, 1);
        }
    }

    /**
     * Two-partition split: alternating rows go to partition 0 and 1.
     */
    public void testTwoPartitionSplit() throws IOException {
        List<BytesReference> docs = List.of(json("{\"x\":10}"), json("{\"x\":20}"), json("{\"x\":30}"), json("{\"x\":40}"));
        try (EscfBatch source = encode(docs)) {
            int[] selectors = { 0, 1, 0, 1 };
            assertRoundTrip(source, selectors, 2);
        }
    }

    /**
     * Three-partition scatter with mixed selectors.
     */
    public void testThreePartitionScatter() throws IOException {
        List<BytesReference> docs = List.of(
            json("{\"a\":1,\"b\":\"alpha\"}"),
            json("{\"a\":2,\"b\":\"beta\"}"),
            json("{\"a\":3,\"b\":\"gamma\"}"),
            json("{\"a\":4,\"b\":\"delta\"}"),
            json("{\"a\":5,\"b\":\"epsilon\"}"),
            json("{\"a\":6,\"b\":\"zeta\"}")
        );
        try (EscfBatch source = encode(docs)) {
            int[] selectors = { 0, 1, 2, 0, 1, 2 };
            assertRoundTrip(source, selectors, 3);
        }
    }

    /**
     * Absent fields: some rows have a field, others don't. Sparse columns must scatter correctly.
     */
    public void testSparseColumns() throws IOException {
        List<BytesReference> docs = List.of(
            json("{\"a\":1}"),        // b absent
            json("{\"a\":2,\"b\":99}"),
            json("{\"a\":3}"),        // b absent
            json("{\"b\":77}")        // a absent
        );
        try (EscfBatch source = encode(docs)) {
            int[] selectors = { 0, 1, 1, 0 };
            assertRoundTrip(source, selectors, 2);
        }
    }

    /**
     * Nested objects: several leaf columns under a common parent path.
     */
    public void testNestedObjects() throws IOException {
        List<BytesReference> docs = List.of(
            json("{\"user\":{\"name\":\"alice\",\"age\":30}}"),
            json("{\"user\":{\"name\":\"bob\",\"age\":25}}"),
            json("{\"user\":{\"name\":\"carol\",\"age\":35}}")
        );
        try (EscfBatch source = encode(docs)) {
            int[] selectors = { 0, 1, 0 };
            assertRoundTrip(source, selectors, 2);
        }
    }

    /**
     * Fixed long arrays: present, empty, and absent rows all scattered correctly.
     */
    public void testLongArrays() throws IOException {
        List<BytesReference> docs = List.of(
            json("{\"ids\":[1,2,3]}"),    // present, non-empty
            json("{\"ids\":[]}"),         // present, empty []
            json("{}"),                    // absent (no ids field)
            json("{\"ids\":[10,20]}")     // present, non-empty
        );
        try (EscfBatch source = encode(docs)) {
            int[] selectors = { 0, 0, 1, 1 };
            assertRoundTrip(source, selectors, 2);
        }
    }

    /**
     * Empty arrays [] must be distinct from absent in the destination batch.
     * Both go to the same partition; assert the distinction is preserved.
     */
    public void testEmptyArrayVsAbsent() throws IOException {
        List<BytesReference> docs = List.of(
            json("{\"nums\":[]}"),    // present, empty
            json("{}"),               // nums absent
            json("{\"nums\":[1]}"),   // present, non-empty
            json("{\"nums\":[]}")     // present, empty (another one)
        );
        try (EscfBatch source = encode(docs)) {
            int[] selectors = { 0, 0, 0, 0 };
            EscfBatch[] parts = scatter(source, selectors, 1);
            try {
                assertNotNull(parts[0]);
                assertEquals(4, parts[0].docCount());
                // Row 0: empty array -> {"nums":[]}
                assertEquals(asMap("{\"nums\":[]}"), reconstruct(parts[0], 0));
                // Row 1: absent -> {}
                assertEquals(asMap("{}"), reconstruct(parts[0], 1));
                // Row 2: non-empty -> {"nums":[1]}
                assertEquals(asMap("{\"nums\":[1]}"), reconstruct(parts[0], 2));
                // Row 3: empty again -> {"nums":[]}
                assertEquals(asMap("{\"nums\":[]}"), reconstruct(parts[0], 3));
            } finally {
                closeAll(parts);
            }
        }
    }

    /**
     * A partition that receives only empty arrays: the column kind must be ARRAY
     * (not UNION) because hintArray is applied up front.
     */
    public void testPartitionWithOnlyEmptyArraysKind() throws IOException {
        List<BytesReference> docs = List.of(
            json("{\"nums\":[1,2]}"),  // -> partition 1
            json("{\"nums\":[]}"),     // -> partition 0
            json("{\"nums\":[]}"),     // -> partition 0
            json("{\"nums\":[3,4]}")   // -> partition 1
        );
        try (EscfBatch source = encode(docs)) {
            int[] selectors = { 1, 0, 0, 1 };
            EscfBatch[] parts = scatter(source, selectors, 2);
            try {
                assertNotNull(parts[0]);
                assertNotNull(parts[1]);
                // partition 0 received only empty arrays; kind should still be ARRAY
                assertEquals(EscfColumnKind.ARRAY, columnKind(parts[0], "nums"));
                // partition 1 received non-empty arrays
                assertEquals(EscfColumnKind.ARRAY, columnKind(parts[1], "nums"));
            } finally {
                closeAll(parts);
            }
        }
    }

    /**
     * Fixed double arrays.
     */
    public void testDoubleArrays() throws IOException {
        List<BytesReference> docs = List.of(json("{\"vals\":[1.5,2.5]}"), json("{\"vals\":[3.5]}"), json("{\"vals\":[]}"));
        try (EscfBatch source = encode(docs)) {
            int[] selectors = { 0, 1, 0 };
            assertRoundTrip(source, selectors, 2);
        }
    }

    /**
     * Fixed string arrays.
     */
    public void testStringArrays() throws IOException {
        List<BytesReference> docs = List.of(json("{\"tags\":[\"a\",\"b\",\"c\"]}"), json("{\"tags\":[\"x\"]}"), json("{\"tags\":[]}"));
        try (EscfBatch source = encode(docs)) {
            int[] selectors = { 0, 0, 1 };
            assertRoundTrip(source, selectors, 2);
        }
    }

    /**
     * Explicit nulls promote a column to UNION. Both partitions stay UNION (kind-preserving scatter).
     */
    public void testUnionNulls() throws IOException {
        List<BytesReference> docs = List.of(json("{\"v\":42}"), json("{\"v\":null}"), json("{\"v\":99}"), json("{\"v\":null}"));
        try (EscfBatch source = encode(docs)) {
            assertEquals(EscfColumnKind.UNION, columnKind(source, "v"));
            int[] selectors = { 0, 1, 0, 1 };
            EscfBatch[] parts = scatter(source, selectors, 2);
            try {
                assertRoundTripRows(source, selectors, parts);
                assertEquals(EscfColumnKind.UNION, columnKind(parts[0], "v"));
                assertEquals(EscfColumnKind.UNION, columnKind(parts[1], "v"));
            } finally {
                closeAll(parts);
            }
        }
    }

    /**
     * Mixed scalar types promote a column to UNION: long, double, string, bool.
     * Both partitions stay UNION regardless of which types they happen to receive.
     */
    public void testUnionMixedTypes() throws IOException {
        List<BytesReference> docs = List.of(
            json("{\"v\":1}"),
            json("{\"v\":\"hello\"}"),
            json("{\"v\":3.14}"),
            json("{\"v\":true}"),
            json("{\"v\":false}")
        );
        try (EscfBatch source = encode(docs)) {
            assertEquals(EscfColumnKind.UNION, columnKind(source, "v"));
            int[] selectors = { 0, 1, 0, 1, 0 };
            EscfBatch[] parts = scatter(source, selectors, 2);
            try {
                assertRoundTripRows(source, selectors, parts);
                // Partition 0: got long, double, bool values (homogeneous-ish) — must still be UNION.
                assertEquals(EscfColumnKind.UNION, columnKind(parts[0], "v"));
                // Partition 1: got string and bool — must be UNION.
                assertEquals(EscfColumnKind.UNION, columnKind(parts[1], "v"));
            } finally {
                closeAll(parts);
            }
        }
    }

    /**
     * A UNION column that has only LONG values in one partition stays UNION (not narrowed to LONG).
     * This is the key guarantee of the raw-bytes scatter design.
     */
    public void testUnionKindPreservedWhenHomogeneous() throws IOException {
        List<BytesReference> docs = List.of(
            json("{\"v\":1}"),       // long — goes to partition 0
            json("{\"v\":null}"),    // null — goes to partition 1, promotes to union in source
            json("{\"v\":2}"),       // long — goes to partition 0
            json("{\"v\":3}")        // long — goes to partition 0
        );
        try (EscfBatch source = encode(docs)) {
            assertEquals(EscfColumnKind.UNION, columnKind(source, "v"));
            int[] selectors = { 0, 1, 0, 0 };
            EscfBatch[] parts = scatter(source, selectors, 2);
            try {
                assertRoundTripRows(source, selectors, parts);
                // Partition 0 received only LONG values, but the source was UNION — must stay UNION.
                assertEquals(EscfColumnKind.UNION, columnKind(parts[0], "v"));
                // Partition 1 received only a null — also stays UNION.
                assertEquals(EscfColumnKind.UNION, columnKind(parts[1], "v"));
            } finally {
                closeAll(parts);
            }
        }
    }

    /**
     * Empty objects (key-value rows) in a UNION column. Destination stays UNION.
     */
    public void testUnionKeyValue() throws IOException {
        List<BytesReference> docs = List.of(
            json("{\"v\":{}}"),      // empty object -> key-value row
            json("{\"v\":42}"),
            json("{\"v\":{}}")
        );
        try (EscfBatch source = encode(docs)) {
            int[] selectors = { 0, 1, 0 };
            EscfBatch[] parts = scatter(source, selectors, 2);
            try {
                assertRoundTripRows(source, selectors, parts);
                assertEquals(EscfColumnKind.UNION, columnKind(parts[0], "v"));
                assertEquals(EscfColumnKind.UNION, columnKind(parts[1], "v"));
            } finally {
                closeAll(parts);
            }
        }
    }

    /**
     * Heterogeneous arrays (UNION_ARRAY) in a UNION column. Destination stays UNION.
     */
    public void testUnionHeterogeneousArrays() throws IOException {
        List<BytesReference> docs = List.of(json("{\"v\":[1,\"two\",3.0]}"), json("{\"v\":99}"), json("{\"v\":[\"a\",\"b\"]}"));
        try (EscfBatch source = encode(docs)) {
            int[] selectors = { 0, 1, 0 };
            EscfBatch[] parts = scatter(source, selectors, 2);
            try {
                assertRoundTripRows(source, selectors, parts);
                assertEquals(EscfColumnKind.UNION, columnKind(parts[0], "v"));
                assertEquals(EscfColumnKind.UNION, columnKind(parts[1], "v"));
            } finally {
                closeAll(parts);
            }
        }
    }

    /**
     * Boolean arrays ([true, false]) are stored as inline UNION_ARRAY payloads on a UNION column —
     * there is no EscfArrayColumn with a BOOL child. The raw-bytes scatter copies the payload
     * verbatim and values must round-trip.
     */
    public void testUnionBoolArray() throws IOException {
        List<BytesReference> docs = List.of(
            json("{\"v\":[true,false,true]}"),
            json("{\"v\":42}"),             // plain long in same column -> UNION
            json("{\"v\":[false]}")
        );
        try (EscfBatch source = encode(docs)) {
            assertEquals(EscfColumnKind.UNION, columnKind(source, "v"));
            int[] selectors = { 0, 1, 0 };
            EscfBatch[] parts = scatter(source, selectors, 2);
            try {
                assertRoundTripRows(source, selectors, parts);
                assertEquals(EscfColumnKind.UNION, columnKind(parts[0], "v"));
                assertEquals(EscfColumnKind.UNION, columnKind(parts[1], "v"));
            } finally {
                closeAll(parts);
            }
        }
    }

    /**
     * A UNION column promoted from a numeric (LONG) column has absent rows that occupy 8-byte payload
     * slots in the data buffer ({@code FixedNumericBuilder.promote} writes {@code offsets[i] = i*8}
     * for every row, absent or not). Scatter copies payloads verbatim using the offset vector, so
     * those stale 8-byte absent slots are reproduced correctly and values still round-trip.
     */
    public void testUnionPromotedFromNumericColumnAbsents() throws IOException {
        // Absent then non-null-then-string causes FixedNumericBuilder(LONG) → promote() → UNION,
        // leaving the first (absent) row's 8-byte slot in the data buffer.
        List<BytesReference> docs = List.of(
            json("{}"),                   // "v" absent — goes into LONG builder as absent, 0-byte slot
            json("{\"v\":42}"),           // LONG value
            json("{\"v\":\"hello\"}"),    // STRING — triggers promotion to UNION
            json("{\"v\":99}")            // LONG again, now in UNION
        );
        try (EscfBatch source = encode(docs)) {
            assertEquals(EscfColumnKind.UNION, columnKind(source, "v"));
            int[] selectors = { 0, 0, 1, 1 };
            EscfBatch[] parts = scatter(source, selectors, 2);
            try {
                assertRoundTripRows(source, selectors, parts);
                assertEquals(EscfColumnKind.UNION, columnKind(parts[0], "v"));
                assertEquals(EscfColumnKind.UNION, columnKind(parts[1], "v"));
            } finally {
                closeAll(parts);
            }
        }
    }

    /**
     * A LONG column stays LONG in all partitions that have at least one non-absent row.
     */
    public void testColumnKindPreservationLong() throws IOException {
        List<BytesReference> docs = List.of(json("{\"x\":1}"), json("{\"x\":2}"), json("{\"x\":3}"));
        try (EscfBatch source = encode(docs)) {
            int[] selectors = { 0, 1, 0 };
            EscfBatch[] parts = scatter(source, selectors, 2);
            try {
                assertEquals(EscfColumnKind.LONG, columnKind(parts[0], "x"));
                assertEquals(EscfColumnKind.LONG, columnKind(parts[1], "x"));
            } finally {
                closeAll(parts);
            }
        }
    }

    /**
     * An all-absent-in-partition column finishes with the source column's kind (via hintScalar).
     */
    public void testAllAbsentPartitionKind() throws IOException {
        List<BytesReference> docs = List.of(
            json("{\"x\":1}"),   // -> partition 0
            json("{}"),           // -> partition 1 (x absent)
            json("{\"x\":3}")    // -> partition 0
        );
        try (EscfBatch source = encode(docs)) {
            int[] selectors = { 0, 1, 0 };
            EscfBatch[] parts = scatter(source, selectors, 2);
            try {
                // partition 0 has real values
                assertEquals(EscfColumnKind.LONG, columnKind(parts[0], "x"));
                // partition 1 has only an absent row; hint makes it LONG
                assertEquals(EscfColumnKind.LONG, columnKind(parts[1], "x"));
            } finally {
                closeAll(parts);
            }
        }
    }

    /**
     * Zero-doc source produces an array of null batches. A zero-doc EscfBatch is constructed
     * directly via the package-private constructor since EscfEncoder throws for empty inputs.
     */
    public void testEmptySource() {
        // Build an empty batch directly: zero docs, empty schema, no columns.
        org.elasticsearch.sourcebatch.SourceSchema schema = new org.elasticsearch.sourcebatch.SourceSchema();
        try (EscfBatch source = new EscfBatch(schema, 0, new EscfColumnData[0], org.elasticsearch.core.Releasables.wrap())) {
            EscfBatch[] parts = scatter(source, new int[0], 3);
            try {
                assertEquals(3, parts.length);
                assertNull(parts[0]);
                assertNull(parts[1]);
                assertNull(parts[2]);
            } finally {
                closeAll(parts);
            }
        }
    }

    /**
     * partitionCount=1, all rows to the single partition.
     */
    public void testSinglePartition() throws IOException {
        List<BytesReference> docs = List.of(json("{\"a\":10}"), json("{\"a\":20}"));
        try (EscfBatch source = encode(docs)) {
            int[] selectors = { 0, 0 };
            EscfBatch[] parts = scatter(source, selectors, 1);
            try {
                assertNotNull(parts[0]);
                assertEquals(2, parts[0].docCount());
                assertRoundTripRows(source, selectors, parts);
            } finally {
                closeAll(parts);
            }
        }
    }

    /**
     * A partition that receives no rows is null.
     */
    public void testEmptyPartition() throws IOException {
        List<BytesReference> docs = List.of(json("{\"a\":1}"), json("{\"a\":2}"));
        try (EscfBatch source = encode(docs)) {
            int[] selectors = { 0, 0 };  // partition 1 gets nothing
            EscfBatch[] parts = scatter(source, selectors, 2);
            try {
                assertNotNull(parts[0]);
                assertNull(parts[1]);
                assertEquals(2, parts[0].docCount());
            } finally {
                closeAll(parts);
            }
        }
    }

    /**
     * All rows to one partition, the other is empty.
     */
    public void testAllRowsToOnePartition() throws IOException {
        List<BytesReference> docs = List.of(json("{\"v\":1}"), json("{\"v\":2}"), json("{\"v\":3}"));
        try (EscfBatch source = encode(docs)) {
            int[] selectors = { 2, 2, 2 };  // everything to partition 2
            EscfBatch[] parts = scatter(source, selectors, 3);
            try {
                assertNull(parts[0]);
                assertNull(parts[1]);
                assertNotNull(parts[2]);
                assertEquals(3, parts[2].docCount());
                for (int r = 0; r < 3; r++) {
                    assertEquals(reconstruct(source, r), reconstruct(parts[2], r));
                }
            } finally {
                closeAll(parts);
            }
        }
    }

    /**
     * Selector out of [0, partitionCount) throws IllegalArgumentException.
     */
    public void testSelectorOutOfRange() throws IOException {
        List<BytesReference> docs = List.of(json("{\"x\":1}"));
        try (EscfBatch source = encode(docs)) {
            expectThrows(IllegalArgumentException.class, () -> scatter(source, new int[] { 5 }, 3));
            expectThrows(IllegalArgumentException.class, () -> scatter(source, new int[] { -1 }, 3));
        }
    }

    /**
     * Selector array shorter than docCount throws IllegalArgumentException.
     */
    public void testSelectorArrayTooShort() throws IOException {
        List<BytesReference> docs = List.of(json("{\"x\":1}"), json("{\"x\":2}"));
        try (EscfBatch source = encode(docs)) {
            expectThrows(IllegalArgumentException.class, () -> scatter(source, new int[] { 0 }, 2));
        }
    }

    /**
     * partitionCount=0 or negative throws IllegalArgumentException.
     */
    public void testInvalidPartitionCount() throws IOException {
        List<BytesReference> docs = List.of(json("{\"x\":1}"));
        try (EscfBatch source = encode(docs)) {
            expectThrows(IllegalArgumentException.class, () -> scatter(source, new int[] { 0 }, 0));
            expectThrows(IllegalArgumentException.class, () -> scatter(source, new int[] { 0 }, -1));
        }
    }

    /**
     * String values large enough to straddle recycler page boundaries, exercising the straddling
     * path in the UNION cursor and the var-column cursor.
     */
    public void testLargeStringValues() throws IOException {
        // 32 KB strings will straddle 16 KB recycler pages
        String bigString = "x".repeat(32 * 1024);
        List<BytesReference> docs = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            docs.add(json("{\"s\":\"" + bigString + i + "\"}"));
        }
        try (EscfBatch source = encode(docs)) {
            int[] selectors = { 0, 1, 0, 1 };
            assertRoundTrip(source, selectors, 2);
        }
    }

    /**
     * UNION column with large string values, stressing the EscfUnionValuesCursor straddling path.
     */
    public void testUnionLargeValues() throws IOException {
        String bigString = "y".repeat(32 * 1024);
        List<BytesReference> docs = List.of(
            json("{\"v\":" + 1 + "}"),             // LONG triggers UNION promotion
            json("{\"v\":\"" + bigString + "\"}"),  // large string
            json("{\"v\":null}"),
            json("{\"v\":\"" + bigString + "2\"}")
        );
        try (EscfBatch source = encode(docs)) {
            int[] selectors = { 0, 1, 0, 1 };
            assertRoundTrip(source, selectors, 2);
        }
    }

    /**
     * The scatterer can be used for multiple scatter calls; scratch arrays are reused.
     */
    public void testReuse() throws IOException {
        Recycler<BytesRef> recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
        try (EscfBatchScatterer scatterer = new EscfBatchScatterer(recycler)) {
            for (int round = 0; round < 3; round++) {
                List<BytesReference> docs = List.of(json("{\"r\":" + round + "}"), json("{\"r\":" + (round + 10) + "}"));
                try (EscfBatch source = encode(docs)) {
                    int[] selectors = { 0, 1 };
                    EscfBatch[] parts = scatterer.scatter(source, selectors, 2);
                    try {
                        assertNotNull(parts[0]);
                        assertNotNull(parts[1]);
                        assertEquals(1, parts[0].docCount());
                        assertEquals(1, parts[1].docCount());
                        assertRoundTripRows(source, selectors, parts);
                    } finally {
                        closeAll(parts);
                    }
                }
            }
        }
    }

    /**
     * Scatter {@code source} and assert row-level round-trip: each destination row must match the
     * corresponding source row.
     */
    private void assertRoundTrip(EscfBatch source, int[] selectors, int partitionCount) throws IOException {
        EscfBatch[] parts = scatter(source, selectors, partitionCount);
        try {
            assertRoundTripRows(source, selectors, parts);
        } finally {
            closeAll(parts);
        }
    }

    /**
     * Core round-trip assertion. For each source row {@code r}, reconstructs the document from
     * {@code dest[selectors[r]]} at the appropriate per-partition row index and asserts equality.
     */
    private static void assertRoundTripRows(EscfBatch source, int[] selectors, EscfBatch[] parts) throws IOException {
        int[] counters = new int[parts.length];
        for (int r = 0; r < source.docCount(); r++) {
            int p = selectors[r];
            Map<String, Object> expected = reconstruct(source, r);
            Map<String, Object> actual = reconstruct(parts[p], counters[p]++);
            assertEquals("row " + r + " -> partition " + p, expected, actual);
        }
    }

    /** Scatter using a fresh recycler-backed scatterer, closing it afterwards. */
    private static EscfBatch[] scatter(EscfBatch source, int[] selectors, int partitionCount) {
        Recycler<BytesRef> recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
        try (EscfBatchScatterer scatterer = new EscfBatchScatterer(recycler)) {
            return scatterer.scatter(source, selectors, partitionCount);
        }
    }

    /**
     * Encode {@code sources} via a MockPageCacheRecycler-backed encoder so pages are leak-tracked.
     */
    private static EscfBatch encode(List<BytesReference> sources) throws IOException {
        Recycler<BytesRef> recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
        try (EscfEncoder encoder = new EscfEncoder(recycler)) {
            for (BytesReference source : sources) {
                encoder.addDocument(source, XContentType.JSON, 0);
            }
            return encoder.buildPartition(0);
        }
    }

    private static Map<String, Object> reconstruct(EscfBatch batch, int row) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            SourceRowToXContent.writeRow(batch.row(row), batch.schema(), builder);
            return XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        }
    }

    private static Map<String, Object> asMap(String jsonStr) {
        return XContentHelper.convertToMap(new BytesArray(jsonStr), false, XContentType.JSON).v2();
    }

    /**
     * Returns the kind byte of the column at {@code path} in {@code batch}, or throws if not found.
     * Paths use dot notation matching the schema's full path representation.
     */
    private static byte columnKind(EscfBatch batch, String path) {
        for (int c = 0; c < batch.columnCount(); c++) {
            if (path.equals(batch.schema().getFullPath(c))) {
                return batch.column(c).kind();
            }
        }
        throw new AssertionError("Column '" + path + "' not found in batch with " + batch.columnCount() + " columns");
    }

    private static BytesReference json(String json) {
        return new BytesArray(json);
    }

    private static void closeAll(EscfBatch[] batches) {
        for (EscfBatch b : batches) {
            if (b != null) {
                b.close();
            }
        }
    }
}
