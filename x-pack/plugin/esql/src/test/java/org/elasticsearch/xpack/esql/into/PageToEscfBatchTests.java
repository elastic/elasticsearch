/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.index.IndexSource;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleRangeBlockBuilder;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class PageToEscfBatchTests extends ComputeTestCase {
    private final MockBigArrays bigArrays;

    public PageToEscfBatchTests() {
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), ByteSizeValue.ofGb(1));
    }

    public void testLong() throws IOException {
        try (LongBlock.Builder b = blockFactory().newLongBlockBuilder(2)) {
            b.appendLong(10_000_000_000L);
            b.appendLong(-10_000_000_000L);
            assertThat(
                rows(List.of(DataType.LONG), b.build()),
                equalTo(List.of(Map.of("c0", 10_000_000_000L), Map.of("c0", -10_000_000_000L)))
            );
        }
    }

    public void testInteger() throws IOException {
        try (IntBlock.Builder b = blockFactory().newIntBlockBuilder(2)) {
            b.appendInt(42);
            b.appendInt(-1);
            assertThat(rows(List.of(DataType.INTEGER), b.build()), equalTo(List.of(Map.of("c0", 42), Map.of("c0", -1))));
        }
    }

    public void testDouble() throws IOException {
        try (DoubleBlock.Builder b = blockFactory().newDoubleBlockBuilder(2)) {
            b.appendDouble(1.5);
            b.appendDouble(-3.14);
            assertThat(rows(List.of(DataType.DOUBLE), b.build()), equalTo(List.of(Map.of("c0", 1.5), Map.of("c0", -3.14))));
        }
    }

    public void testMultiValuedLong() throws IOException {
        try (LongBlock.Builder b = blockFactory().newLongBlockBuilder(3)) {
            b.beginPositionEntry().appendLong(10_000_000_000L).appendLong(20_000_000_000L).endPositionEntry();
            b.appendLong(30_000_000_000L);
            b.appendNull();
            assertThat(
                rows(List.of(DataType.LONG), b.build()),
                equalTo(List.of(Map.of("c0", List.of(10_000_000_000L, 20_000_000_000L)), Map.of("c0", List.of(30_000_000_000L)), Map.of()))
            );
        }
    }

    public void testMultiValuedDouble() throws IOException {
        try (DoubleBlock.Builder b = blockFactory().newDoubleBlockBuilder(2)) {
            b.beginPositionEntry().appendDouble(1.1).appendDouble(2.2).endPositionEntry();
            b.appendDouble(3.3);
            assertThat(
                rows(List.of(DataType.DOUBLE), b.build()),
                equalTo(List.of(Map.of("c0", List.of(1.1, 2.2)), Map.of("c0", List.of(3.3))))
            );
        }
    }

    public void testBoolean() throws IOException {
        try (BooleanBlock.Builder b = blockFactory().newBooleanBlockBuilder(2)) {
            b.appendBoolean(true);
            b.appendBoolean(false);
            assertThat(rows(List.of(DataType.BOOLEAN), b.build()), equalTo(List.of(Map.of("c0", true), Map.of("c0", false))));
        }
    }

    public void testMultiValuedBoolean() throws IOException {
        try (BooleanBlock.Builder b = blockFactory().newBooleanBlockBuilder(2)) {
            b.beginPositionEntry().appendBoolean(true).appendBoolean(false).endPositionEntry();
            b.appendBoolean(true);
            assertThat(
                rows(List.of(DataType.BOOLEAN), b.build()),
                equalTo(List.of(Map.of("c0", List.of(true, false)), Map.of("c0", true)))
            );
        }
    }

    public void testKeyword() throws IOException {
        try (BytesRefBlock.Builder b = blockFactory().newBytesRefBlockBuilder(2)) {
            b.appendBytesRef(new BytesRef("hello"));
            b.appendNull();
            assertThat(rows(List.of(DataType.KEYWORD), b.build()), equalTo(List.of(Map.of("c0", "hello"), Map.of())));
        }
    }

    public void testMultiValuedKeyword() throws IOException {
        try (BytesRefBlock.Builder b = blockFactory().newBytesRefBlockBuilder(2)) {
            b.beginPositionEntry().appendBytesRef(new BytesRef("a")).appendBytesRef(new BytesRef("b")).endPositionEntry();
            b.appendBytesRef(new BytesRef("c"));
            assertThat(
                rows(List.of(DataType.KEYWORD), b.build()),
                equalTo(List.of(Map.of("c0", List.of("a", "b")), Map.of("c0", List.of("c"))))
            );
        }
    }

    public void testDatetimeUsesUtc() throws IOException {
        try (LongBlock.Builder b = blockFactory().newLongBlockBuilder(1)) {
            b.appendLong(1_750_000_000_000L);
            assertThat(rows(List.of(DataType.DATETIME), b.build()), equalTo(List.of(Map.of("c0", "2025-06-15T15:06:40.000Z"))));
        }
    }

    public void testDateNanos() throws IOException {
        try (LongBlock.Builder b = blockFactory().newLongBlockBuilder(1)) {
            b.appendLong(1_000_000_001L);
            assertThat(rows(List.of(DataType.DATE_NANOS), b.build()), equalTo(List.of(Map.of("c0", "1970-01-01T00:00:01.000000001Z"))));
        }
    }

    public void testIp() throws IOException {
        BytesRef ipBytes = new BytesRef(InetAddressPoint.encode(InetAddresses.forString("192.168.1.1")));
        try (BytesRefBlock.Builder b = blockFactory().newBytesRefBlockBuilder(1)) {
            b.appendBytesRef(ipBytes);
            assertThat(rows(List.of(DataType.IP), b.build()), equalTo(List.of(Map.of("c0", "192.168.1.1"))));
        }
    }

    public void testUnsignedLongBelowLongMax() throws IOException {
        long encoded = NumericUtils.asLongUnsigned(BigInteger.valueOf(42L));
        try (LongBlock.Builder b = blockFactory().newLongBlockBuilder(1)) {
            b.appendLong(encoded);
            assertThat(rows(List.of(DataType.UNSIGNED_LONG), b.build()), equalTo(List.of(Map.of("c0", 42))));
        }
    }

    public void testUnsignedLongAboveLongMax() throws IOException {
        BigInteger big = BigInteger.TWO.pow(63).add(BigInteger.ONE);
        long encoded = NumericUtils.asLongUnsigned(big);
        try (LongBlock.Builder b = blockFactory().newLongBlockBuilder(1)) {
            b.appendLong(encoded);
            assertThat(rows(List.of(DataType.UNSIGNED_LONG), b.build()), equalTo(List.of(Map.of("c0", big.toString()))));
        }
    }

    public void testDenseVectorPreservesShortestDecimal() throws IOException {
        try (FloatBlock.Builder b = blockFactory().newFloatBlockBuilder(1)) {
            b.appendFloat(1.1f);
            List<Map<String, Object>> maps = rows(List.of(DataType.DENSE_VECTOR), b.build());
            assertThat(maps, hasSize(1));
            assertThat(maps.getFirst().get("c0"), equalTo(1.1));
        }
    }

    public void testDoubleRangeRendersAsString() throws IOException {
        try (DoubleRangeBlockBuilder b = blockFactory().newDoubleRangeBlockBuilder(1)) {
            b.appendDoubleRange(1.0, 2.0);
            List<Map<String, Object>> maps = rows(List.of(DataType.DOUBLE_RANGE), b.build());
            assertThat(maps, equalTo(List.of(Map.of("c0", "1.0..2.0"))));
        }
    }

    public void testAllNullColumnsOmitted() throws IOException {
        try (
            LongBlock.Builder aB = blockFactory().newLongBlockBuilder(2);
            LongBlock.Builder nullB = blockFactory().newLongBlockBuilder(2)
        ) {
            aB.appendLong(5_000_000_000L);
            aB.appendLong(6_000_000_000L);
            nullB.appendNull();
            nullB.appendNull();
            assertThat(
                rows(List.of(DataType.LONG, DataType.LONG), aB.build(), nullB.build()),
                equalTo(List.of(Map.of("c0", 5_000_000_000L), Map.of("c0", 6_000_000_000L)))
            );
        }
        try (LongBlock.Builder b = blockFactory().newLongBlockBuilder(1)) {
            b.appendLong(7_000_000_000L);
            assertThat(
                rows(List.of(DataType.LONG, DataType.NULL), b.build(), blockFactory().newConstantNullBlock(1)),
                equalTo(List.of(Map.of("c0", 7_000_000_000L)))
            );
        }
    }

    public void testNullInterleaved() throws IOException {
        try (LongBlock.Builder b = blockFactory().newLongBlockBuilder(3)) {
            b.appendLong(10_000_000_000L);
            b.appendNull();
            b.beginPositionEntry().appendLong(20_000_000_000L).appendLong(30_000_000_000L).endPositionEntry();
            assertThat(
                rows(List.of(DataType.LONG), b.build()),
                equalTo(List.of(Map.of("c0", List.of(10_000_000_000L)), Map.of(), Map.of("c0", List.of(20_000_000_000L, 30_000_000_000L))))
            );
        }
    }

    public void testEmptyPage() throws IOException {
        assertThat(rows(List.of(DataType.LONG), blockFactory().newConstantNullBlock(0)), equalTo(List.of()));
    }

    private List<Map<String, Object>> rows(List<DataType> types, Block... blocks) throws IOException {
        Page page = new Page(blocks);
        List<String> names = new ArrayList<>(types.size());
        for (int i = 0; i < types.size(); i++) {
            names.add("c" + i);
        }
        try (EscfBatch batch = PageToEscfBatch.convert(page, names, types, bigArrays.bytesRefRecycler(), null).batch()) {
            return toMaps(batch);
        } finally {
            page.releaseBlocks();
        }
    }

    private static List<Map<String, Object>> toMaps(EscfBatch batch) throws IOException {
        List<Map<String, Object>> result = new ArrayList<>();
        for (int row = 0; row < batch.docCount(); row++) {
            IndexSource src = new IndexSource();
            src.setSourceRow(batch, row, XContentType.JSON);
            src.ensureInlineSource();
            result.add(XContentHelper.convertToMap(src.bytes(), false, XContentType.JSON).v2());
        }
        return result;
    }
}
