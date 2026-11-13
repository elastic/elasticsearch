/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.index.IndexSource;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class PageToEscfBatchTests extends ComputeTestCase {

    /** Materializes each row of a batch to a JSON map for assertion. */
    private static List<Map<String, Object>> toMaps(EscfBatch batch) throws IOException {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int row = 0; row < batch.docCount(); row++) {
            IndexSource src = new IndexSource();
            src.setSourceRow(batch, row, XContentType.JSON);
            src.ensureInlineSource();
            rows.add(XContentHelper.convertToMap(src.bytes(), false, XContentType.JSON).v2());
        }
        return rows;
    }

    public void testLongColumn() throws IOException {
        try (LongBlock.Builder b = blockFactory().newLongBlockBuilder(2)) {
            b.appendLong(10_000_000_000L);
            b.appendLong(-10_000_000_000L);
            try (Page page = new Page(b.build())) {
                try (EscfBatch batch = PageToEscfBatch.convert(page, List.of("n"), List.of(DataType.LONG))) {
                    List<Map<String, Object>> rows = toMaps(batch);
                    assertThat(((Number) rows.get(0).get("n")).longValue(), equalTo(10_000_000_000L));
                    assertThat(((Number) rows.get(1).get("n")).longValue(), equalTo(-10_000_000_000L));
                }
            }
        }
    }

    public void testDoubleColumn() throws IOException {
        try (DoubleBlock.Builder b = blockFactory().newDoubleBlockBuilder(2)) {
            b.appendDouble(1.5);
            b.appendDouble(-3.14);
            try (Page page = new Page(b.build())) {
                try (EscfBatch batch = PageToEscfBatch.convert(page, List.of("d"), List.of(DataType.DOUBLE))) {
                    List<Map<String, Object>> rows = toMaps(batch);
                    assertThat(rows.get(0).get("d"), equalTo(1.5));
                    assertThat(rows.get(1).get("d"), equalTo(-3.14));
                }
            }
        }
    }

    public void testBooleanColumn() throws IOException {
        try (BooleanBlock.Builder b = blockFactory().newBooleanBlockBuilder(3)) {
            b.appendBoolean(true);
            b.appendBoolean(false);
            b.appendNull();
            try (Page page = new Page(b.build())) {
                try (EscfBatch batch = PageToEscfBatch.convert(page, List.of("flag"), List.of(DataType.BOOLEAN))) {
                    List<Map<String, Object>> rows = toMaps(batch);
                    assertThat(rows.get(0).get("flag"), equalTo(true));
                    assertThat(rows.get(1).get("flag"), equalTo(false));
                    assertThat(rows.get(2), not(hasKey("flag")));
                }
            }
        }
    }

    public void testMultiValuedBooleanColumn() throws IOException {
        try (BooleanBlock.Builder b = blockFactory().newBooleanBlockBuilder(4)) {
            b.beginPositionEntry();
            b.appendBoolean(true);
            b.appendBoolean(false);
            b.endPositionEntry();
            b.appendBoolean(true);
            try (Page page = new Page(b.build())) {
                try (EscfBatch batch = PageToEscfBatch.convert(page, List.of("flags"), List.of(DataType.BOOLEAN))) {
                    List<Map<String, Object>> rows = toMaps(batch);
                    assertThat(rows.get(0).get("flags"), equalTo(List.of(true, false)));
                    assertThat(rows.get(1).get("flags"), equalTo(true));
                }
            }
        }
    }

    public void testKeywordColumn() throws IOException {
        try (BytesRefBlock.Builder b = blockFactory().newBytesRefBlockBuilder(2)) {
            b.appendBytesRef(new BytesRef("hello"));
            b.appendNull();
            try (Page page = new Page(b.build())) {
                try (EscfBatch batch = PageToEscfBatch.convert(page, List.of("kw"), List.of(DataType.KEYWORD))) {
                    List<Map<String, Object>> rows = toMaps(batch);
                    assertThat(rows.get(0).get("kw"), equalTo("hello"));
                    assertThat(rows.get(1), not(hasKey("kw")));
                }
            }
        }
    }

    public void testMultiValuedKeyword() throws IOException {
        try (BytesRefBlock.Builder b = blockFactory().newBytesRefBlockBuilder(4)) {
            b.beginPositionEntry();
            b.appendBytesRef(new BytesRef("a"));
            b.appendBytesRef(new BytesRef("b"));
            b.endPositionEntry();
            b.appendBytesRef(new BytesRef("c"));
            try (Page page = new Page(b.build())) {
                try (EscfBatch batch = PageToEscfBatch.convert(page, List.of("tags"), List.of(DataType.KEYWORD))) {
                    List<Map<String, Object>> rows = toMaps(batch);
                    assertThat(rows.get(0).get("tags"), equalTo(List.of("a", "b")));
                    assertThat(rows.get(1).get("tags"), equalTo("c"));
                }
            }
        }
    }

    public void testDatetimeColumnUsesUtc() throws IOException {
        try (LongBlock.Builder b = blockFactory().newLongBlockBuilder(1)) {
            b.appendLong(1_750_000_000_000L);
            try (Page page = new Page(b.build())) {
                try (EscfBatch batch = PageToEscfBatch.convert(page, List.of("dt"), List.of(DataType.DATETIME))) {
                    List<Map<String, Object>> rows = toMaps(batch);
                    assertThat(rows.get(0).get("dt"), equalTo("2025-06-15T15:06:40.000Z"));
                }
            }
        }
    }

    public void testAllNullColumnOmitted() throws IOException {
        try (
            LongBlock.Builder longB = blockFactory().newLongBlockBuilder(2);
            LongBlock.Builder nullB = blockFactory().newLongBlockBuilder(2)
        ) {
            longB.appendLong(5_000_000_000L);
            longB.appendLong(6_000_000_000L);
            nullB.appendNull();
            nullB.appendNull();
            try (Page page = new Page(longB.build(), nullB.build())) {
                try (EscfBatch batch = PageToEscfBatch.convert(page, List.of("present", "absent"), List.of(DataType.LONG, DataType.LONG))) {
                    assertThat(batch.columnCount(), equalTo(1));
                    List<Map<String, Object>> rows = toMaps(batch);
                    assertThat(((Number) rows.get(0).get("present")).longValue(), equalTo(5_000_000_000L));
                    assertThat(rows.get(0), not(hasKey("absent")));
                }
            }
        }
    }

    public void testNullTypeColumnOmitted() throws IOException {
        try (LongBlock.Builder b = blockFactory().newLongBlockBuilder(1)) {
            b.appendLong(7_000_000_000L);
            try (Page page = new Page(b.build(), blockFactory().newConstantNullBlock(1))) {
                try (EscfBatch batch = PageToEscfBatch.convert(page, List.of("v", "n"), List.of(DataType.LONG, DataType.NULL))) {
                    assertThat(batch.columnCount(), equalTo(1));
                    List<Map<String, Object>> rows = toMaps(batch);
                    assertThat(((Number) rows.get(0).get("v")).longValue(), equalTo(7_000_000_000L));
                    assertThat(rows.get(0), not(hasKey("n")));
                }
            }
        }
    }

    public void testUnsignedLongBelowLongMax() throws IOException {
        // unsigned long = 42, stored as twos-complement encoded long
        long encoded = org.elasticsearch.xpack.esql.core.util.NumericUtils.asLongUnsigned(java.math.BigInteger.valueOf(42L));
        try (LongBlock.Builder b = blockFactory().newLongBlockBuilder(1)) {
            b.appendLong(encoded);
            try (Page page = new Page(b.build())) {
                try (EscfBatch batch = PageToEscfBatch.convert(page, List.of("ul"), List.of(DataType.UNSIGNED_LONG))) {
                    List<Map<String, Object>> rows = toMaps(batch);
                    assertThat(rows.get(0).get("ul"), equalTo("42"));
                }
            }
        }
    }

    public void testSchemaRegistersOnlyNonNullColumns() throws IOException {
        try (
            LongBlock.Builder aB = blockFactory().newLongBlockBuilder(1);
            LongBlock.Builder nullB = blockFactory().newLongBlockBuilder(1)
        ) {
            aB.appendLong(8_000_000_000L);
            nullB.appendNull();
            try (Page page = new Page(aB.build(), nullB.build())) {
                try (EscfBatch batch = PageToEscfBatch.convert(page, List.of("a", "b"), List.of(DataType.LONG, DataType.LONG))) {
                    assertThat(batch.columnCount(), equalTo(1));
                    assertThat(batch.schema().leafCount(), equalTo(1));
                    assertThat(batch.schema().getLeafName(0), equalTo("a"));
                }
            }
        }
    }
}
