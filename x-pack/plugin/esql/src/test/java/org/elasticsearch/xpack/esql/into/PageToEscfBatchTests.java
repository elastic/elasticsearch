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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker.Durability;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.recycler.Recycler;
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
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
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
                equalTo(List.of(Map.of("c0", List.of(10_000_000_000L, 20_000_000_000L)), Map.of("c0", 30_000_000_000L), Map.of()))
            );
        }
    }

    public void testMultiValuedDouble() throws IOException {
        try (DoubleBlock.Builder b = blockFactory().newDoubleBlockBuilder(2)) {
            b.beginPositionEntry().appendDouble(1.1).appendDouble(2.2).endPositionEntry();
            b.appendDouble(3.3);
            assertThat(rows(List.of(DataType.DOUBLE), b.build()), equalTo(List.of(Map.of("c0", List.of(1.1, 2.2)), Map.of("c0", 3.3))));
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
            assertThat(rows(List.of(DataType.KEYWORD), b.build()), equalTo(List.of(Map.of("c0", List.of("a", "b")), Map.of("c0", "c"))));
        }
    }

    public void testPageSpanningKeywordsAreCopied() throws IOException {
        byte[] first = filled((byte) 'a', 20_000);
        byte[] second = filled((byte) 'b', 20_000);
        try (
            BytesRefBlock.Builder left = blockFactory().newBytesRefBlockBuilder(1);
            BytesRefBlock.Builder right = blockFactory().newBytesRefBlockBuilder(1)
        ) {
            left.appendBytesRef(new BytesRef(first));
            right.appendBytesRef(new BytesRef(second));
            assertThat(
                rows(List.of(DataType.KEYWORD, DataType.KEYWORD), left.build(), right.build()),
                equalTo(
                    List.of(
                        Map.of(
                            "c0",
                            new String(first, java.nio.charset.StandardCharsets.UTF_8),
                            "c1",
                            new String(second, java.nio.charset.StandardCharsets.UTF_8)
                        )
                    )
                )
            );
        }
        try (BytesRefBlock.Builder values = blockFactory().newBytesRefBlockBuilder(1)) {
            values.beginPositionEntry().appendBytesRef(new BytesRef(first)).appendBytesRef(new BytesRef(second)).endPositionEntry();
            assertThat(
                rows(List.of(DataType.KEYWORD), values.build()),
                equalTo(
                    List.of(
                        Map.of(
                            "c0",
                            List.of(
                                new String(first, java.nio.charset.StandardCharsets.UTF_8),
                                new String(second, java.nio.charset.StandardCharsets.UTF_8)
                            )
                        )
                    )
                )
            );
        }
    }

    private static byte[] filled(byte value, int length) {
        byte[] bytes = new byte[length];
        java.util.Arrays.fill(bytes, value);
        return bytes;
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

    public void testUnsignedLongAboveLongMaxIsRejected() {
        BigInteger big = BigInteger.TWO.pow(63).add(BigInteger.ONE);
        long encoded = NumericUtils.asLongUnsigned(big);
        try (LongBlock.Builder b = blockFactory().newLongBlockBuilder(1)) {
            b.appendLong(encoded);
            EsqlIllegalArgumentException e = expectThrows(
                EsqlIllegalArgumentException.class,
                () -> rows(List.of(DataType.UNSIGNED_LONG), b.build())
            );
            assertThat(e.getMessage(), containsString("unsigned_long"));
        }
    }

    public void testUnsignedLongArrayAboveLongMaxIsRejected() {
        BigInteger big = BigInteger.TWO.pow(63).add(BigInteger.ONE);
        long encoded = NumericUtils.asLongUnsigned(big);
        try (LongBlock.Builder b = blockFactory().newLongBlockBuilder(1)) {
            b.beginPositionEntry().appendLong(encoded).appendLong(NumericUtils.asLongUnsigned(BigInteger.valueOf(7))).endPositionEntry();
            EsqlIllegalArgumentException e = expectThrows(
                EsqlIllegalArgumentException.class,
                () -> rows(List.of(DataType.UNSIGNED_LONG), b.build())
            );
            assertThat(e.getMessage(), containsString("unsigned_long"));
        }
    }

    public void testStringColumnTripsOnTheNextRow() {
        BytesRef value = new BytesRef("abcdefghijklmnopq");
        int rows = 2;
        Recycler<BytesRef> recycler = bigArrays.bytesRefRecycler();
        long overhead = recycler.pageSize() + (long) (rows + 1) * Integer.BYTES;
        LimitingBreaker breaker = new LimitingBreaker(overhead + value.length);
        Page page = new Page(blockFactory().newConstantBytesRefBlockWith(value, rows));
        try {
            CircuitBreakingException e = expectThrows(
                CircuitBreakingException.class,
                () -> PageToEscfBatch.convert(page, List.of("c0"), List.of(DataType.KEYWORD), recycler, breaker)
            );
            assertThat(e.getBytesWanted(), equalTo((long) value.length));
            int acceptedRows = 0;
            for (long bytes : breaker.accepted()) {
                if (bytes == value.length) {
                    acceptedRows++;
                }
            }
            assertThat(acceptedRows, equalTo(1));
            assertThat(breaker.getUsed(), equalTo(0L));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testMixedStringCardinalityKeepsScalarRows() throws IOException {
        try (BytesRefBlock.Builder b = blockFactory().newBytesRefBlockBuilder(2)) {
            b.appendBytesRef(new BytesRef("c"));
            b.beginPositionEntry().appendBytesRef(new BytesRef("a")).appendBytesRef(new BytesRef("b")).endPositionEntry();
            assertThat(rows(List.of(DataType.KEYWORD), b.build()), equalTo(List.of(Map.of("c0", "c"), Map.of("c0", List.of("a", "b")))));
        }
    }

    public void testSourceBigIntegerIsRejected() {
        assertSourceRejected("{\"n\":" + oversizedInteger() + "}");
    }

    public void testSourceNestedBigIntegerIsRejected() {
        String big = oversizedInteger().toString();
        assertSourceRejected("{\"a\":{\"n\":" + big + "}}");
        assertSourceRejected("{\"a\":[1," + big + "]}");
        assertSourceRejected("{\"a\":[{\"n\":" + big + "}]}");
    }

    public void testMultiValueSourceIsRejected() {
        try (BytesRefBlock.Builder b = blockFactory().newBytesRefBlockBuilder(1)) {
            b.beginPositionEntry().appendBytesRef(new BytesRef("{\"n\":1}")).appendBytesRef(new BytesRef("{\"n\":2}")).endPositionEntry();
            EsqlIllegalArgumentException e = expectThrows(
                EsqlIllegalArgumentException.class,
                () -> rows(List.of(DataType.SOURCE), b.build())
            );
            assertThat(e.getMessage(), containsString("multi-value SOURCE"));
        }
    }

    private void assertSourceRejected(String json) {
        try (BytesRefBlock.Builder b = blockFactory().newBytesRefBlockBuilder(1)) {
            b.appendBytesRef(new BytesRef(json));
            EsqlIllegalArgumentException e = expectThrows(
                EsqlIllegalArgumentException.class,
                () -> rows(List.of(DataType.SOURCE), b.build())
            );
            assertThat(json, e.getMessage(), containsString("SOURCE"));
        }
    }

    private static BigInteger oversizedInteger() {
        return BigInteger.TWO.pow(63).add(BigInteger.ONE);
    }

    public void testSourceColumnIsAnObject() throws IOException {
        try (BytesRefBlock.Builder b = blockFactory().newBytesRefBlockBuilder(1)) {
            b.appendBytesRef(new BytesRef("{\"n\":1}"));
            assertThat(rows(List.of(DataType.SOURCE), b.build()), equalTo(List.of(Map.of("c0", Map.of("n", 1)))));
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

    public void testMultiValuedDenseVectorIsNumeric() throws IOException {
        try (FloatBlock.Builder b = blockFactory().newFloatBlockBuilder(1)) {
            b.beginPositionEntry().appendFloat(1.5f).appendFloat(2.5f).endPositionEntry();
            assertThat(rows(List.of(DataType.DENSE_VECTOR), b.build()), equalTo(List.of(Map.of("c0", List.of(1.5, 2.5)))));
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
                equalTo(List.of(Map.of("c0", 10_000_000_000L), Map.of(), Map.of("c0", List.of(20_000_000_000L, 30_000_000_000L))))
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
        try (EscfBatchBuilder builder = new EscfBatchBuilder(bigArrays.bytesRefRecycler())) {
            PageToEscfBatch.append(builder, page, names, types);
            if (page.getPositionCount() == 0) {
                return List.of();
            }
            try (EscfBatch batch = builder.buildPartition(0)) {
                return toMaps(batch);
            }
        } finally {
            page.releaseBlocks();
        }
    }

    private static final class LimitingBreaker implements CircuitBreaker {
        private final long limit;
        private final List<Long> accepted = new ArrayList<>();
        private long used;

        LimitingBreaker(long limit) {
            this.limit = limit;
        }

        List<Long> accepted() {
            return accepted;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            throw new CircuitBreakingException(fieldName, bytesNeeded, limit, Durability.TRANSIENT);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            if (used + bytes > limit) {
                circuitBreak(label, bytes);
            }
            used += bytes;
            accepted.add(bytes);
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used += bytes;
        }

        @Override
        public long getUsed() {
            return used;
        }

        @Override
        public long getLimit() {
            return limit;
        }

        @Override
        public double getOverhead() {
            return 1;
        }

        @Override
        public long getTrippedCount() {
            return 0;
        }

        @Override
        public String getName() {
            return "into-test";
        }

        @Override
        public Durability getDurability() {
            return Durability.TRANSIENT;
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {}
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
