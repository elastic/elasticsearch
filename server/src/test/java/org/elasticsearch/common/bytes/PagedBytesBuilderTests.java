/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.function.Function;

import static org.elasticsearch.common.util.PageCacheRecycler.BYTE_PAGE_SIZE;
import static org.hamcrest.Matchers.equalTo;

public class PagedBytesBuilderTests extends ESTestCase {
    private static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    private final PageCacheRecycler recycler = new MockPageCacheRecycler(Settings.EMPTY);

    public void testBreakOnBuild() {
        String label = randomAlphaOfLength(4);
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(0));
        expectThrows(CircuitBreakingException.class, () -> new PagedBytesBuilder(recycler, breaker, label, 0));
    }

    public void testAppendByte() {
        testAppendByte(newLimitedBreaker(ByteSizeValue.ofMb(50)));
    }

    public void testAppendByteCranky() {
        try {
            testAppendByte(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testAppendByte(CircuitBreaker breaker) {
        testAgainstOracle(breaker, builder -> {
            byte b = randomByte();
            builder.append(b);
            return new byte[] { b };
        });
    }

    public void testAppendBytes() {
        testAppendBytes(newLimitedBreaker(ByteSizeValue.ofMb(50)));
    }

    public void testAppendBytesCranky() {
        try {
            testAppendBytes(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testAppendBytes(CircuitBreaker breaker) {
        testAgainstOracle(breaker, builder -> {
            byte[] b = randomByteArrayOfLength(randomIntBetween(1, BYTE_PAGE_SIZE * 2));
            builder.append(b, 0, b.length);
            return b;
        });
    }

    public void testAppendInt() {
        testAppendInt(newLimitedBreaker(ByteSizeValue.ofMb(50)));
    }

    public void testAppendIntCranky() {
        try {
            testAppendInt(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testAppendInt(CircuitBreaker breaker) {
        testAgainstOracle(breaker, builder -> {
            int v = randomInt();
            builder.append(v);
            byte[] expected = new byte[Integer.BYTES];
            INT.set(expected, 0, v);
            return expected;
        });
    }

    public void testAppendManyInts() {
        testAppendManyInts(newLimitedBreaker(ByteSizeValue.ofMb(50)));
    }

    public void testAppendManyIntsCranky() {
        try {
            testAppendManyInts(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testAppendManyInts(CircuitBreaker breaker) {
        // 3 pages worth of ints — enough to shift to PAGED and span multiple pages
        int count = BYTE_PAGE_SIZE / Integer.BYTES * 3;
        int[] values = new int[count];
        for (int i = 0; i < count; i++) {
            values[i] = randomInt();
        }
        byte[] leading = randomBoolean() ? randomByteArrayOfLength(randomIntBetween(1, 100)) : new byte[0];
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            builder.append(leading, 0, leading.length);
            for (int v : values) {
                builder.append(v);
            }
            assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.PAGED));
            assertThat(builder.length(), equalTo(leading.length + count * Integer.BYTES));
            byte[] expected = new byte[leading.length + count * Integer.BYTES];
            System.arraycopy(leading, 0, expected, 0, leading.length);
            for (int i = 0; i < count; i++) {
                INT.set(expected, leading.length + i * Integer.BYTES, values[i]);
            }
            try (PagedBytes ref = builder.build()) {
                assertThat(toFlat(ref), equalTo(expected));
            }
        }
    }

    public void testAppendLong() {
        testAppendLong(newLimitedBreaker(ByteSizeValue.ofMb(50)));
    }

    public void testAppendLongCranky() {
        try {
            testAppendLong(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testAppendLong(CircuitBreaker breaker) {
        testAgainstOracle(breaker, builder -> {
            long v = randomLong();
            builder.append(v);
            byte[] expected = new byte[Long.BYTES];
            LONG.set(expected, 0, v);
            return expected;
        });
    }

    public void testAppendPagedBytesBuilder() {
        testAppendPagedBytesBuilder(newLimitedBreaker(ByteSizeValue.ofMb(50)));
    }

    public void testAppendPagedBytesBuilderCranky() {
        try {
            testAppendPagedBytesBuilder(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testAppendPagedBytesBuilder(CircuitBreaker breaker) {
        int pages = randomIntBetween(0, 10);
        byte[] expected = randomByteArrayOfLength(pages * BYTE_PAGE_SIZE + randomIntBetween(0, BYTE_PAGE_SIZE - 1));
        try (
            PagedBytesBuilder src = new PagedBytesBuilder(recycler, breaker, "src", 0);
            PagedBytesBuilder dst = new PagedBytesBuilder(recycler, breaker, "dst", 0)
        ) {
            src.append(expected, 0, expected.length);
            dst.append(src);
            assertThat(dst.length(), equalTo(expected.length));
            try (PagedBytes built = dst.build()) {
                assertThat(toFlat(built), equalTo(expected));
            }
        }
    }

    public void testAppendPagedBytesShort() {
        testAppendPagedBytesShort(newLimitedBreaker(ByteSizeValue.ofMb(50)));
    }

    public void testAppendPagedBytesShortCranky() {
        try {
            testAppendPagedBytesShort(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testAppendPagedBytesShort(CircuitBreaker breaker) {
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            byte[] expected = randomByteArrayOfLength(randomIntBetween(0, BYTE_PAGE_SIZE - 1));
            PagedBytes ref = PagedBytesTests.newPagedBytes(expected);
            builder.append(ref);
            assertThat(builder.length(), equalTo(expected.length));
            try (PagedBytes built = builder.build()) {
                assertThat(toFlat(built), equalTo(expected));
            }
        }
    }

    public void testAppendPagedBytesMultiPage() {
        testAppendPagedBytesMultiPage(newLimitedBreaker(ByteSizeValue.ofMb(50)));
    }

    public void testAppendPagedBytesMultiPageCranky() {
        try {
            testAppendPagedBytesMultiPage(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testAppendPagedBytesMultiPage(CircuitBreaker breaker) {
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            int pages = randomIntBetween(1, 10);
            byte[] expected = randomByteArrayOfLength(pages * BYTE_PAGE_SIZE + randomIntBetween(0, BYTE_PAGE_SIZE - 1));
            PagedBytes ref = PagedBytesTests.newPagedBytes(expected);
            builder.append(ref);
            assertThat(builder.length(), equalTo(expected.length));
            try (PagedBytes built = builder.build()) {
                assertThat(toFlat(built), equalTo(expected));
            }
        }
    }

    public void testAppendManyLongs() {
        testAppendManyLongs(newLimitedBreaker(ByteSizeValue.ofMb(50)));
    }

    public void testAppendManyLongsCranky() {
        try {
            testAppendManyLongs(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testAppendManyLongs(CircuitBreaker breaker) {
        // 3 pages worth of longs — enough to shift to PAGED and span multiple pages
        int count = BYTE_PAGE_SIZE / Long.BYTES * 3;
        long[] values = new long[count];
        for (int i = 0; i < count; i++) {
            values[i] = randomLong();
        }
        byte[] leading = randomBoolean() ? randomByteArrayOfLength(randomIntBetween(1, 100)) : new byte[0];
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            builder.append(leading, 0, leading.length);
            for (long v : values) {
                builder.append(v);
            }
            assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.PAGED));
            assertThat(builder.length(), equalTo(leading.length + count * Long.BYTES));
            byte[] expected = new byte[leading.length + count * Long.BYTES];
            System.arraycopy(leading, 0, expected, 0, leading.length);
            for (int i = 0; i < count; i++) {
                LONG.set(expected, leading.length + i * Long.BYTES, values[i]);
            }
            try (PagedBytes ref = builder.build()) {
                assertThat(toFlat(ref), equalTo(expected));
            }
        }
    }

    public void testAppendBytesRef() {
        testAppendBytesRef(newLimitedBreaker(ByteSizeValue.ofMb(50)));
    }

    public void testAppendBytesRefCranky() {
        try {
            testAppendBytesRef(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testAppendBytesRef(CircuitBreaker breaker) {
        testAgainstOracle(breaker, builder -> {
            byte[] b = randomByteArrayOfLength(randomIntBetween(1, BYTE_PAGE_SIZE * 2));
            builder.append(new BytesRef(b));
            return b;
        });
    }

    public void testBigButNotHuge() {
        testBigButNotHuge(newLimitedBreaker(ByteSizeValue.ofGb(1)));
    }

    public void testBigButNotHugeCranky() {
        try {
            testBigButNotHuge(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testBigButNotHuge(CircuitBreaker breaker) {
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.SMALL_TAIL));
            byte[] chunk = randomByteArrayOfLength(Math.toIntExact(ByteSizeValue.ofKb(6).getBytes()));
            builder.append(chunk, 0, chunk.length);
            assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.SMALL_TAIL));
            builder.append(chunk, 0, chunk.length);
            assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.PAGED));
            builder.append(chunk, 0, chunk.length);

            assertThat(builder.length(), equalTo(chunk.length * 3));
            try (PagedBytes ref = builder.build()) {
                assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.BUILT));
                assertThat(ref.length(), equalTo(chunk.length * 3));
                assertThat(ref.pages().length, equalTo(2));
            }
        }
    }

    public void testSpansMultiplePages() {
        testSpansMultiplePages(newLimitedBreaker(ByteSizeValue.ofMb(50)));
    }

    public void testSpansMultiplePagesCranky() {
        try {
            testSpansMultiplePages(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testSpansMultiplePages(CircuitBreaker breaker) {
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.SMALL_TAIL));
            byte[] chunk = randomByteArrayOfLength(BYTE_PAGE_SIZE);
            builder.append(chunk, 0, chunk.length);
            assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.PAGED));
            builder.append(chunk, 0, chunk.length);
            builder.append(chunk, 0, chunk.length);

            assertThat(builder.length(), equalTo(BYTE_PAGE_SIZE * 3));
            try (PagedBytes ref = builder.build()) {
                assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.BUILT));
                assertThat(ref.length(), equalTo(BYTE_PAGE_SIZE * 3));
                assertThat(ref.pages().length, equalTo(3));
            }
        }
    }

    public void testInitialCapacityThreeQuartersPageSize() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", BYTE_PAGE_SIZE * 3 / 4)) {
            assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.PAGED));
        }
    }

    public void testBreakOnGrow() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(PagedBytesBuilder.PAGE_RAM_BYTES_USED * 2));
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            byte[] chunk = randomByteArrayOfLength(BYTE_PAGE_SIZE);
            builder.append(chunk, 0, chunk.length);
            assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.PAGED));
            // next append triggers a new page which should break
            expectThrows(CircuitBreakingException.class, () -> builder.append((byte) 0));
        }
    }

    public void testRamBytesUsed() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.SMALL_TAIL));
            assertThat(breaker.getUsed(), equalTo(builder.ramBytesUsed()));
            builder.append(randomByteArrayOfLength(BYTE_PAGE_SIZE), 0, BYTE_PAGE_SIZE);
            assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.PAGED));
            assertThat(breaker.getUsed(), equalTo(builder.ramBytesUsed()));
            builder.append(randomByteArrayOfLength(BYTE_PAGE_SIZE), 0, BYTE_PAGE_SIZE);
            assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.PAGED));
            assertThat(breaker.getUsed(), equalTo(builder.ramBytesUsed()));
        }
    }

    public void testCompareTo() {
        for (int i = 0; i < 100; i++) {
            testCompareTo(newLimitedBreaker(ByteSizeValue.ofMb(50)));
        }
    }

    private void testCompareTo(CircuitBreaker breaker) {
        byte[] lhsFlat = randomByteArrayOfLength(randomIntBetween(0, BYTE_PAGE_SIZE * 3));
        byte[] rhsFlat = randomBoolean() ? lhsFlat.clone() : randomByteArrayOfLength(randomIntBetween(0, BYTE_PAGE_SIZE * 3));
        try (
            PagedBytesBuilder lhs = new PagedBytesBuilder(recycler, breaker, "lhs", 0);
            PagedBytesBuilder rhs = new PagedBytesBuilder(recycler, breaker, "rhs", 0)
        ) {
            lhs.append(lhsFlat, 0, lhsFlat.length);
            rhs.append(rhsFlat, 0, rhsFlat.length);
            int actual = Integer.signum(lhs.compareTo(rhs));
            try (PagedBytes lhsRef = lhs.build(); PagedBytes rhsRef = rhs.build()) {
                assertThat(actual, equalTo(Integer.signum(lhsRef.compareTo(rhsRef))));
            }
        }
    }

    public void testEqualsAndHashCode() {
        for (int i = 0; i < 100; i++) {
            testEqualsAndHashCode(newLimitedBreaker(ByteSizeValue.ofMb(50)));
        }
    }

    private void testEqualsAndHashCode(CircuitBreaker breaker) {
        byte[] flat = randomByteArrayOfLength(randomIntBetween(0, BYTE_PAGE_SIZE * 3));
        try (
            PagedBytesBuilder a = new PagedBytesBuilder(recycler, breaker, "a", 0);
            PagedBytesBuilder b = new PagedBytesBuilder(recycler, breaker, "b", 0)
        ) {
            a.append(flat, 0, flat.length);
            b.append(flat, 0, flat.length);
            assertEquals(a, b);
            assertThat(a.hashCode(), equalTo(b.hashCode()));
            int aHash = a.hashCode();
            try (PagedBytes aRef = a.build()) {
                assertThat(aHash, equalTo(aRef.hashCode()));
            }
        }
    }

    public void testHashCodeMatchesBytesRef() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        for (int i = 0; i < 100; i++) {
            byte[] flat = randomByteArrayOfLength(randomIntBetween(0, BYTE_PAGE_SIZE * 3));
            int expected = new BytesRef(flat).hashCode();
            try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
                builder.append(flat, 0, flat.length);
                assertThat(builder.hashCode(), equalTo(expected));
                try (PagedBytes ref = builder.build()) {
                    assertThat(ref.hashCode(), equalTo(expected));
                }
            }
        }
    }

    public void testNeverBuild() {
        testNeverBuild(newLimitedBreaker(ByteSizeValue.ofMb(50)));
    }

    public void testNeverBuildCranky() {
        try {
            testNeverBuild(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testNeverBuild(CircuitBreaker breaker) {
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            builder.append(randomByteArrayOfLength(BYTE_PAGE_SIZE), 0, BYTE_PAGE_SIZE);
            assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.PAGED));
            assertTrue(breaker.getUsed() > 0);
        }
    }

    public void testBuildTransfersBreakerToRef() {
        testBuildTransfersBreakerToRef(newLimitedBreaker(ByteSizeValue.ofMb(50)));
    }

    public void testBuildTransfersBreakerToRefCranky() {
        try {
            testBuildTransfersBreakerToRef(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testBuildTransfersBreakerToRef(CircuitBreaker breaker) {
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            builder.append(randomByteArrayOfLength(BYTE_PAGE_SIZE), 0, BYTE_PAGE_SIZE);
            assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.PAGED));
            long preCloseCharge = breaker.getUsed();
            try (PagedBytes ref = builder.build()) {
                assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.BUILT));
                // builder is now empty; breaker charge transferred to ref
                assertThat(breaker.getUsed(), equalTo(preCloseCharge));
            }
            // ref is closed; charge released
            assertThat(breaker.getUsed(), equalTo(0L));
        }
        // closing the empty builder releases nothing — charge was already released by ref
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testAppendVInt() {
        testAppendVInt(newLimitedBreaker(ByteSizeValue.ofMb(50)), 1);
    }

    public void testAppendVIntCranky() {
        try {
            testAppendVInt(new CrankyCircuitBreakerService.CrankyCircuitBreaker(), 1);
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    public void testAppendVInts() {
        testAppendVInt(newLimitedBreaker(ByteSizeValue.ofMb(50)), between(2, 100_000));
    }

    public void testAppendVIntsCranky() {
        try {
            testAppendVInt(new CrankyCircuitBreakerService.CrankyCircuitBreaker(), between(2, 100_000));
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testAppendVInt(CircuitBreaker breaker, int count) {
        testAgainstOracle(breaker, builder -> {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                for (int i = 0; i < count; i++) {
                    int v = switch (between(0, 4)) {
                        case 0 -> between(0x00, 0x7F);                      // 1 byte
                        case 1 -> between(0x80, 0x3FFF);                    // 2 bytes
                        case 2 -> between(0x4000, 0x1FFFFF);                // 3 bytes
                        case 3 -> between(0x200000, 0xFFFFFFF);             // 4 bytes
                        default -> randomBoolean()                          // 5 bytes
                            ? between(0x10000000, Integer.MAX_VALUE)
                            : between(Integer.MIN_VALUE, -1);
                    };
                    builder.appendVInt(v);
                    out.writeVInt(v);
                }
                BytesRef ref = out.bytes().toBytesRef();
                return Arrays.copyOfRange(ref.bytes, ref.offset, ref.offset + ref.length);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void testClearSmallTail() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            byte[] first = randomByteArrayOfLength(randomIntBetween(1, PagedBytesBuilder.MAX_SMALL_TAIL_SIZE - 1));
            builder.append(first, 0, first.length);
            assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.SMALL_TAIL));
            long chargeBeforeClear = breaker.getUsed();

            builder.clear();

            assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.SMALL_TAIL));
            assertThat(builder.length(), equalTo(0));
            assertThat(breaker.getUsed(), equalTo(chargeBeforeClear));

            // can still append after clear
            byte[] second = randomByteArrayOfLength(randomIntBetween(1, PagedBytesBuilder.MAX_SMALL_TAIL_SIZE - 1));
            builder.append(second, 0, second.length);
            assertThat(builder.length(), equalTo(second.length));
            try (PagedBytes ref = builder.build()) {
                assertThat(toFlat(ref), equalTo(second));
            }
        }
    }

    public void testClearPagedOnePage() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            byte[] data = randomByteArrayOfLength(BYTE_PAGE_SIZE);
            builder.append(data, 0, data.length);
            assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.PAGED));
            long chargeBeforeClear = breaker.getUsed();

            builder.clear();

            assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.PAGED));
            assertThat(builder.length(), equalTo(0));
            assertThat(breaker.getUsed(), equalTo(chargeBeforeClear));

            // can still append after clear
            byte[] second = randomByteArrayOfLength(BYTE_PAGE_SIZE / 2);
            builder.append(second, 0, second.length);
            assertThat(builder.length(), equalTo(second.length));
            try (PagedBytes ref = builder.build()) {
                assertThat(toFlat(ref), equalTo(second));
            }
        }
    }

    public void testClearPagedMultiplePages() {
        testClearPagedMultiplePages(newLimitedBreaker(ByteSizeValue.ofMb(50)));
    }

    public void testClearPagedMultiplePagesCranky() {
        try {
            testClearPagedMultiplePages(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testClearPagedMultiplePages(CircuitBreaker breaker) {
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            int pageCount = randomIntBetween(2, 5);
            byte[] data = randomByteArrayOfLength(pageCount * BYTE_PAGE_SIZE);
            builder.append(data, 0, data.length);
            assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.PAGED));

            builder.clear();

            assertThat(builder.mode(), equalTo(PagedBytesBuilder.Mode.PAGED));
            assertThat(builder.length(), equalTo(0));
            // breaker should reflect all allocated pages kept for reuse (plus shallow + pages array)
            assertThat(breaker.getUsed(), equalTo(builder.ramBytesUsed()));

            // can still append after clear
            byte[] second = randomByteArrayOfLength(BYTE_PAGE_SIZE / 2);
            builder.append(second, 0, second.length);
            assertThat(builder.length(), equalTo(second.length));
            try (PagedBytes ref = builder.build()) {
                assertThat(toFlat(ref), equalTo(second));
            }
        }
    }

    /**
     * Appends chunks via the supplied function, then checks that {@link PagedBytesBuilder#build()}
     * returns a {@link PagedBytes} whose bytes match the concatenation of all appended chunks.
     */
    private void testAgainstOracle(CircuitBreaker breaker, Function<PagedBytesBuilder, byte[]> appender) {
        int capacity = BYTE_PAGE_SIZE * 10;
        PagedBytesBuilder builder = randomBoolean()
            ? new PagedBytesBuilder(recycler, breaker, "test", 0)
            : new PagedBytesBuilder(recycler, breaker, "test", capacity);
        try (builder) {
            byte[] expected = new byte[0];
            int iterations = randomIntBetween(1, 5);
            for (int i = 0; i < iterations; i++) {
                byte[] chunk = appender.apply(builder);
                expected = concat(expected, chunk);
            }
            assertThat(builder.length(), equalTo(expected.length));
            try (PagedBytes built = builder.build()) {
                assertThat(toFlat(built), equalTo(expected));
                assertThat(built, equalTo(PagedBytesTests.newPagedBytes(expected)));
            }
        }
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] result = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }

    private static byte[] toFlat(PagedBytes ref) {
        byte[] flat = new byte[ref.length()];
        int pos = 0;
        for (int p = 0; p < ref.pages().length; p++) {
            int toCopy = Math.min(ref.pages()[p].length, ref.length() - pos);
            System.arraycopy(ref.pages()[p], 0, flat, pos, toCopy);
            pos += toCopy;
        }
        return flat;
    }

    public void testToStringSmall() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        try (PagedBytesBuilder b = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            b.append((byte) 0x0a);
            b.append((byte) 0xff);
            b.append((byte) 0x00);
            assertThat(b.toString(), equalTo("0a ff 00"));
        }
    }

    public void testToStringTruncates() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        try (PagedBytesBuilder b = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            for (int i = 0; i < 101; i++) {
                b.append((byte) 0);
            }
            assertTrue("should truncate beyond 100 bytes", b.toString().endsWith("..."));
        }
    }

    private static long bytesArrayRamBytesUsed(int capacity) {
        return RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) capacity * RamUsageEstimator.NUM_BYTES_OBJECT_REF
        );
    }
}
