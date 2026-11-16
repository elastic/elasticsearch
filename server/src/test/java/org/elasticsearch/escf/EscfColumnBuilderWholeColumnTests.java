/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.sourcebatch.SourceRowToXContent;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class EscfColumnBuilderWholeColumnTests extends ESTestCase {

    private static List<Map<String, Object>> allRows(EscfBatch batch) throws IOException {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int r = 0; r < batch.docCount(); r++) {
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                SourceRowToXContent.writeRow(batch.row(r), batch.schema(), builder);
                rows.add(XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2());
            }
        }
        return rows;
    }

    private static EscfBatch batchOf(String fieldName, EscfColumnData col, int docCount) {
        SourceSchema schema = new SourceSchema();
        schema.appendLeaf(fieldName, 0);
        return EscfBatch.of(schema, docCount, new EscfColumnData[] { col }, col);
    }

    private static BytesReference longData(long... values) {
        byte[] bytes = new byte[values.length * Long.BYTES];
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        for (long v : values) {
            buf.putLong(v);
        }
        return new BytesArray(bytes);
    }

    private static BytesReference doubleData(double... values) {
        byte[] bytes = new byte[values.length * Long.BYTES];
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        for (double v : values) {
            buf.putLong(Double.doubleToRawLongBits(v));
        }
        return new BytesArray(bytes);
    }

    public void testFixed64LongDense() throws IOException {
        try (
            EscfColumnData col = EscfColumnBuilder.buildFixed64Column(
                EscfColumnKind.LONG,
                2,
                null,
                longData(10_000_000_000L, 20_000_000_000L)
            );
            EscfBatch batch = batchOf("n", col, 2)
        ) {
            assertThat(allRows(batch), equalTo(List.of(Map.of("n", 10_000_000_000L), Map.of("n", 20_000_000_000L))));
        }
    }

    public void testFixed64LongWithValidity() throws IOException {
        FixedBitSet validity = new FixedBitSet(3);
        validity.set(0);
        validity.set(2);
        try (
            EscfColumnData col = EscfColumnBuilder.buildFixed64Column(
                EscfColumnKind.LONG,
                3,
                validity,
                longData(10_000_000_000L, 0L, 30_000_000_000L)
            );
            EscfBatch batch = batchOf("n", col, 3)
        ) {
            assertThat(allRows(batch), equalTo(List.of(Map.of("n", 10_000_000_000L), Map.of(), Map.of("n", 30_000_000_000L))));
        }
    }

    public void testFixed64Double() throws IOException {
        try (
            EscfColumnData col = EscfColumnBuilder.buildFixed64Column(EscfColumnKind.DOUBLE, 2, null, doubleData(1.5, -3.14));
            EscfBatch batch = batchOf("d", col, 2)
        ) {
            assertThat(allRows(batch), equalTo(List.of(Map.of("d", 1.5), Map.of("d", -3.14))));
        }
    }

    public void testFixed64WrongKindThrows() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> EscfColumnBuilder.buildFixed64Column(EscfColumnKind.STRING, 1, null, longData(1L))
        );
        assertThat(e.getMessage(), equalTo("kind must be LONG or DOUBLE; got STRING"));
    }

    public void testFixed64WrongLengthThrows() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> EscfColumnBuilder.buildFixed64Column(EscfColumnKind.LONG, 2, null, new BytesArray(new byte[7]))
        );
        assertThat(e.getMessage(), equalTo("data length 7 must equal docCount * 8 = 16"));
    }

    public void testStringColumnDense() throws IOException {
        byte[] aBytes = "alpha".getBytes(StandardCharsets.UTF_8);
        byte[] bBytes = "beta".getBytes(StandardCharsets.UTF_8);
        byte[] data = new byte[aBytes.length + bBytes.length];
        System.arraycopy(aBytes, 0, data, 0, aBytes.length);
        System.arraycopy(bBytes, 0, data, aBytes.length, bBytes.length);
        int[] offsets = { 0, aBytes.length, data.length };
        try (
            EscfColumnData col = EscfColumnBuilder.buildStringColumn(2, null, offsets, new BytesArray(data));
            EscfBatch batch = batchOf("s", col, 2)
        ) {
            assertThat(allRows(batch), equalTo(List.of(Map.of("s", "alpha"), Map.of("s", "beta"))));
        }
    }

    public void testStringColumnWithValidity() throws IOException {
        byte[] helloBytes = "hello".getBytes(StandardCharsets.UTF_8);
        int[] offsets = { 0, helloBytes.length, helloBytes.length };
        FixedBitSet validity = new FixedBitSet(2);
        validity.set(0);
        try (
            EscfColumnData col = EscfColumnBuilder.buildStringColumn(2, validity, offsets, new BytesArray(helloBytes));
            EscfBatch batch = batchOf("kw", col, 2)
        ) {
            assertThat(allRows(batch), equalTo(List.of(Map.of("kw", "hello"), Map.of())));
        }
    }

    public void testStringWrongOffsetsLengthThrows() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> EscfColumnBuilder.buildStringColumn(2, null, new int[2], new BytesArray(new byte[0]))
        );
        assertThat(e.getMessage(), equalTo("offsets.length 2 must equal docCount + 1 = 3"));
    }

    public void testArrayColumnLong() throws IOException {
        // Row 0: [10B, 20B], Row 1: [30B]; large values so Jackson parses them as Long not Integer
        int[] rowOffsets = { 0, 2, 3 };
        try (
            EscfColumnData child = EscfColumnBuilder.buildFixed64Column(
                EscfColumnKind.LONG,
                3,
                null,
                longData(10_000_000_000L, 20_000_000_000L, 30_000_000_000L)
            );
            EscfColumnData col = EscfColumnBuilder.buildArrayColumn(2, null, rowOffsets, child);
            EscfBatch batch = batchOf("n", col, 2)
        ) {
            assertThat(
                allRows(batch),
                equalTo(List.of(Map.of("n", List.of(10_000_000_000L, 20_000_000_000L)), Map.of("n", List.of(30_000_000_000L))))
            );
        }
    }

    public void testArrayColumnWrongChildKindThrows() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            try (EscfColumnData boolChild = EscfColumnBuilder.buildBoolColumn(1, null, null)) {
                EscfColumnBuilder.buildArrayColumn(1, null, new int[2], boolChild);
            }
        });
        assertThat(e.getMessage(), equalTo("array child kind must be LONG, DOUBLE, or STRING; got BOOL"));
    }

    public void testArrayColumnChildWithValidityThrows() {
        FixedBitSet validity = new FixedBitSet(1);
        validity.set(0);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            try (EscfColumnData child = EscfColumnBuilder.buildFixed64Column(EscfColumnKind.LONG, 1, validity, longData(1L))) {
                EscfColumnBuilder.buildArrayColumn(1, null, new int[2], child);
            }
        });
        assertThat(e.getMessage(), equalTo("array child column must have no validity bitset"));
    }

    public void testBoolColumnDense() throws IOException {
        FixedBitSet values = new FixedBitSet(3);
        values.set(0);
        values.set(2);
        try (EscfColumnData col = EscfColumnBuilder.buildBoolColumn(3, null, values); EscfBatch batch = batchOf("b", col, 3)) {
            assertThat(allRows(batch), equalTo(List.of(Map.of("b", true), Map.of("b", false), Map.of("b", true))));
        }
    }

    public void testBoolColumnAllFalse() throws IOException {
        try (EscfColumnData col = EscfColumnBuilder.buildBoolColumn(2, null, null); EscfBatch batch = batchOf("b", col, 2)) {
            assertThat(allRows(batch), equalTo(List.of(Map.of("b", false), Map.of("b", false))));
        }
    }
}
