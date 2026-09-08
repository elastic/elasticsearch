/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class BytesRefArrayTests extends ESTestCase {

    @Before
    public void setIntOffsetLimit() {
        if (randomBoolean()) {
            BytesRefArray.MAX_INT_OFFSET = randomIntBetween(1, 1024);
        } else if (randomBoolean()) {
            BytesRefArray.MAX_INT_OFFSET = randomIntBetween(
                (int) ByteSizeValue.ofKb(1).getBytes(),
                (int) ByteSizeValue.ofMb(10).getBytes()
            );
        } else {
            BytesRefArray.MAX_INT_OFFSET = randomIntBetween(
                (int) ByteSizeValue.ofMb(10).getBytes(),
                (int) ByteSizeValue.ofGb(1).getBytes()
            );
        }
    }

    public static BytesRefArray randomArray() {
        return randomArray(randomIntBetween(0, 100), randomIntBetween(10, 50), mockBigArrays());
    }

    public static BytesRefArray randomArray(long capacity, int entries, BigArrays bigArrays) {
        BytesRefArray bytesRefs = new BytesRefArray(capacity, bigArrays);
        BytesRefBuilder ref = new BytesRefBuilder();

        for (int i = 0; i < entries; i++) {
            String str = randomUnicodeOfLengthBetween(4, 20);
            ref.copyChars(str);
            bytesRefs.append(ref.get());
        }

        return bytesRefs;
    }

    public void testRandomWithSerialization() throws IOException {
        int runs = randomIntBetween(2, 20);
        BytesRefArray array = randomArray();

        for (int j = 0; j < runs; j++) {
            BytesRefArray copy = copyInstance(
                array,
                writableRegistry(),
                StreamOutput::writeWriteable,
                in -> new BytesRefArray(in, mockBigArrays()),
                TransportVersion.current()
            );

            assertEquality(array, copy);
            copy.close();
            array.close();
            array = randomArray();
        }
        array.close();
    }

    public void testOwnership() {
        BytesRefArray array = randomArray();
        long size = array.size();
        array.incRef();
        assertThat(array.refCount(), equalTo(2));
        array.close();
        // still accessible
        BytesRef sparse = new BytesRef();
        for (long l = 0; l < size; l++) {
            var v = array.get(l, sparse);
            assertThat(v.length, greaterThan(1));
        }
        assertThat(array.refCount(), equalTo(1));
        array.close();
    }

    public void testLookup() throws IOException {
        int size = randomIntBetween(0, 16 * 1024);
        BytesRefArray array = new BytesRefArray(randomIntBetween(0, size), mockBigArrays());
        try {
            BytesRef[] values = new BytesRef[size];
            for (int i = 0; i < size; i++) {
                BytesRef bytesRef = new BytesRef(randomByteArrayOfLength(between(1, 20)));
                if (bytesRef.length > 0 && randomBoolean()) {
                    bytesRef.offset = randomIntBetween(0, bytesRef.length - 1);
                    bytesRef.length = randomIntBetween(0, bytesRef.length - bytesRef.offset);
                }
                values[i] = bytesRef;
                if (randomBoolean()) {
                    bytesRef = BytesRef.deepCopyOf(bytesRef);
                }
                array.append(bytesRef);
            }
            int copies = randomIntBetween(0, 3);
            for (int i = 0; i < copies; i++) {
                BytesRefArray inArray = array;
                array = copyInstance(
                    inArray,
                    writableRegistry(),
                    StreamOutput::writeWriteable,
                    in -> new BytesRefArray(in, mockBigArrays()),
                    TransportVersion.current()
                );
                assertEquality(inArray, array);
                inArray.close();
            }
            assertThat(array.size(), equalTo((long) size));
            BytesRef bytes = new BytesRef();
            for (int i = 0; i < size; i++) {
                int pos = randomIntBetween(0, size - 1);
                bytes = array.get(pos, bytes);
                assertThat(bytes, equalTo(values[pos]));
            }
        } finally {
            array.close();
        }
    }

    public void testReadWritten() {
        testReadWritten(false);
    }

    public void testReadWrittenHalfEmpty() {
        testReadWritten(true);
    }

    private void testReadWritten(boolean halfEmpty) {
        List<BytesRef> values = new ArrayList<>();
        int bytes = PageCacheRecycler.PAGE_SIZE_IN_BYTES * between(2, 20);
        int used = 0;
        while (used < bytes) {
            String str = halfEmpty && randomBoolean() ? "" : randomAlphaOfLengthBetween(0, 200);
            BytesRef v = new BytesRef(str);
            used += v.length;
            values.add(v);
        }
        testReadWritten(values, randomBoolean() ? bytes : between(0, bytes));
    }

    public void testReadWrittenRepeated() {
        testReadWrittenRepeated(false, between(2, 3000));
    }

    public void testReadWrittenRepeatedPowerOfTwo() {
        testReadWrittenRepeated(false, 1024);
    }

    public void testReadWrittenRepeatedHalfEmpty() {
        testReadWrittenRepeated(true, between(1, 3000));
    }

    public void testReadWrittenRepeatedHalfEmptyPowerOfTwo() {
        testReadWrittenRepeated(true, 1024);
    }

    public void testReadWrittenRepeated(boolean halfEmpty, int listSize) {
        List<BytesRef> values = randomList(2, 10, () -> {
            String str = halfEmpty && randomBoolean() ? "" : randomAlphaOfLengthBetween(0, 10);
            return new BytesRef(str);
        });
        testReadWritten(IntStream.range(0, listSize).mapToObj(i -> values).flatMap(List::stream).toList(), 10);
    }

    private void testReadWritten(List<BytesRef> values, int initialCapacity) {
        try (BytesRefArray array = new BytesRefArray(initialCapacity, mockBigArrays())) {
            for (BytesRef v : values) {
                array.append(v);
            }
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < values.size(); i++) {
                array.get(i, scratch);
                assertThat(scratch, equalTo(values.get(i)));
            }
        }
    }

    public void testValueMaxByteSize() {
        int size = randomIntBetween(0, 100);
        try (BytesRefArray array = new BytesRefArray(size, mockBigArrays())) {
            int expectedMax = 0;
            for (int i = 0; i < size; i++) {
                BytesRef value = new BytesRef(randomByteArrayOfLength(between(0, 20)));
                array.append(value);
                expectedMax = Math.max(expectedMax, value.length);
            }
            assertThat(array.valueMaxByteSize(), equalTo(expectedMax));
        }
    }

    public void testAppendPagedBytesCursorSinglePage() {
        try (BytesRefArray array = new BytesRefArray(0, mockBigArrays())) {
            byte[] data = randomByteArrayOfLength(between(1, 100));
            PagedBytesCursor cursor = new PagedBytesCursor();
            cursor.init(data, 0, data.length);
            array.append(cursor);
            assertThat(array.size(), equalTo(1L));
            assertThat(array.get(0, new BytesRef()), equalTo(new BytesRef(data)));
        }
    }

    public void testAppendPagedBytesCursorMultiPage() {
        try (BytesRefArray array = new BytesRefArray(0, mockBigArrays())) {
            int pageSize = between(2, 16);
            int pages = between(2, 4);
            byte[][] pageData = new byte[pages][pageSize];
            byte[] flat = randomByteArrayOfLength(pages * pageSize);
            for (int p = 0; p < pages; p++) {
                System.arraycopy(flat, p * pageSize, pageData[p], 0, pageSize);
            }
            PagedBytesCursor cursor = new PagedBytesCursor();
            cursor.init(pageData, 0, 0, flat.length, false);
            array.append(cursor);
            assertThat(array.size(), equalTo(1L));
            assertThat(array.get(0, new BytesRef()), equalTo(new BytesRef(flat)));
        }
    }

    public void testAppendPagedBytesCursorEmpty() {
        try (BytesRefArray array = new BytesRefArray(0, mockBigArrays())) {
            PagedBytesCursor cursor = new PagedBytesCursor();
            cursor.init(new byte[0], 0, 0);
            array.append(cursor);
            assertThat(array.size(), equalTo(1L));
            assertThat(array.get(0, new BytesRef()), equalTo(new BytesRef()));
        }
    }

    public void testAppendPagedBytesCursorMixed() {
        try (BytesRefArray array = new BytesRefArray(0, mockBigArrays())) {
            int count = between(4, 20);
            BytesRef[] expected = new BytesRef[count];
            PagedBytesCursor cursor = new PagedBytesCursor();
            for (int i = 0; i < count; i++) {
                byte[] data = randomByteArrayOfLength(between(0, 50));
                expected[i] = new BytesRef(data);
                if (randomBoolean()) {
                    array.append(new BytesRef(data));
                } else {
                    cursor.init(data, 0, data.length);
                    array.append(cursor);
                }
            }
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < count; i++) {
                assertThat(array.get(i, scratch), equalTo(expected[i]));
            }
        }
    }

    public void testEmptyEquals() {
        final int pageSize = PageCacheRecycler.PAGE_SIZE_IN_BYTES;
        try (BytesRefArray array = new BytesRefArray(pageSize, mockBigArrays())) {
            var v0 = new BytesRef(randomByteArrayOfLength(pageSize / 2));
            var v1 = new BytesRef(randomByteArrayOfLength(pageSize / 2));
            var empty = new BytesRef();
            array.append(v0);
            array.append(v1);
            array.append(empty);
            assertTrue(array.bytesEqual(0, v0));
            assertTrue(array.bytesEqual(1, v1));
            assertTrue(array.bytesEqual(2, empty));
            assertFalse(array.bytesEqual(0, empty));
            assertFalse(array.bytesEqual(1, empty));
        }
    }

    public void testRandomEquals() {
        final int pageSize = PageCacheRecycler.PAGE_SIZE_IN_BYTES;
        try (BytesRefArray array = new BytesRefArray(randomIntBetween(1, pageSize), mockBigArrays())) {
            int numValues = between(10, 100);
            BytesRef[] values = new BytesRef[numValues];
            for (int i = 0; i < numValues; i++) {
                int length = randomFrom(0, between(1, 1024), pageSize / 2, pageSize, pageSize * 2);
                var value = new BytesRef(length);
                array.append(value);
                values[i] = value;
            }
            for (int i = 0; i < numValues; i++) {
                assertTrue(array.bytesEqual(i, values[i]));
            }
            for (int i = 0; i < numValues; i++) {
                BytesRef other = randomFrom(values);
                if (other.equals(values[i])) {
                    assertTrue(array.bytesEqual(i, other));
                } else {
                    assertFalse(array.bytesEqual(i, other));
                }
            }
        }
    }

    public void testTruncateThenRead() {
        int total = randomIntBetween(2, 50);
        int kept = randomIntBetween(1, total - 1);
        try (BytesRefArray array = new BytesRefArray(total, mockBigArrays())) {
            List<BytesRef> values = new ArrayList<>();
            for (int i = 0; i < total; i++) {
                BytesRef v = new BytesRef(randomAlphaOfLengthBetween(1, 20));
                array.append(v);
                values.add(BytesRef.deepCopyOf(v));
            }
            array.truncateTo(kept);
            assertThat(array.size(), equalTo((long) kept));
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < kept; i++) {
                assertThat(array.get(i, scratch), equalTo(values.get(i)));
            }
        }
    }

    public void testTruncateThenAppend() {
        int total = randomIntBetween(2, 50);
        int kept = randomIntBetween(0, total - 1);
        int extra = randomIntBetween(1, 20);
        try (BytesRefArray array = new BytesRefArray(total, mockBigArrays())) {
            List<BytesRef> expected = new ArrayList<>();
            for (int i = 0; i < total; i++) {
                BytesRef v = new BytesRef(randomAlphaOfLengthBetween(1, 20));
                array.append(v);
                if (i < kept) {
                    expected.add(BytesRef.deepCopyOf(v));
                }
            }
            array.truncateTo(kept);
            for (int i = 0; i < extra; i++) {
                BytesRef v = new BytesRef(randomAlphaOfLengthBetween(1, 20));
                array.append(v);
                expected.add(BytesRef.deepCopyOf(v));
            }
            assertThat(array.size(), equalTo((long) (kept + extra)));
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < expected.size(); i++) {
                assertThat(array.get(i, scratch), equalTo(expected.get(i)));
            }
        }
    }

    public void testTruncateNoOp() {
        int size = randomIntBetween(1, 20);
        try (BytesRefArray array = new BytesRefArray(size, mockBigArrays())) {
            List<BytesRef> values = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                BytesRef v = new BytesRef(randomAlphaOfLengthBetween(1, 20));
                array.append(v);
                values.add(BytesRef.deepCopyOf(v));
            }
            array.truncateTo(size);
            assertThat(array.size(), equalTo((long) size));
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < size; i++) {
                assertThat(array.get(i, scratch), equalTo(values.get(i)));
            }
        }
    }

    public void testTruncateNoOpAtPageBoundary() {
        // Append one entry of exactly PAGE_SIZE_IN_BYTES bytes. After the append the byte stream's
        // currentPagePos == PAGE_SIZE, so size() == PAGE_SIZE. Calling truncateTo(1) passes
        // lastOffset == PAGE_SIZE to Bytes.truncateTo — the boundary that previously computed
        // targetPageIndex == pageCount and threw ArrayIndexOutOfBoundsException.
        int pageSize = PageCacheRecycler.PAGE_SIZE_IN_BYTES;
        try (BytesRefArray array = new BytesRefArray(1, mockBigArrays())) {
            BytesRef v = new BytesRef(new byte[pageSize]);
            array.append(v);
            array.truncateTo(1); // no-op: keep the single entry
            assertThat(array.size(), equalTo(1L));
            BytesRef scratch = new BytesRef();
            assertThat(array.get(0, scratch), equalTo(v));
        }
    }

    public void testTruncateMultiPageRelease() {
        // Fill enough entries to span multiple internal pages, truncate back, verify that
        // all released-page bytes are gone and the kept entries still read correctly.
        int pageSize = PageCacheRecycler.PAGE_SIZE_IN_BYTES;
        int entrySize = pageSize / 2 + 1; // each entry straddles a page boundary
        int total = 6;
        int kept = 2;
        try (BytesRefArray array = new BytesRefArray(total, mockBigArrays())) {
            List<BytesRef> values = new ArrayList<>();
            for (int i = 0; i < total; i++) {
                byte[] data = new byte[entrySize];
                java.util.Arrays.fill(data, (byte) i);
                BytesRef v = new BytesRef(data);
                array.append(v);
                values.add(BytesRef.deepCopyOf(v));
            }
            array.truncateTo(kept);
            assertThat(array.size(), equalTo((long) kept));
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < kept; i++) {
                assertThat(array.get(i, scratch), equalTo(values.get(i)));
            }
        }
    }

    /**
     * Enough entries that the byte storage keeps grabbing fresh pages for the whole run. Only the sub-page first
     * page grows geometrically; past that each page costs one fixed-size allocation. A short run therefore never
     * leaves the geometric phase and allocates only a handful of times, giving the breaker little to trip on.
     */
    private static final int ENTRIES_SPANNING_MANY_PAGES = 2000;

    /**
     * How many fresh arrays a refusal test may go through looking for one pass that both survived construction
     * and saw an append refused. The constructor makes two breaker calls, so roughly one pass in ten is lost
     * before the loop even starts; without the retry those runs would verify nothing and still pass.
     */
    private static final int MAX_PASSES = 100;

    /**
     * Builds an array, or returns {@code null} when the breaker refuses one of the allocations the constructor
     * makes, leaving nothing for the caller to exercise on that pass.
     */
    private BytesRefArray newArrayOrNullIfRefused(BigArrays bigArrays) {
        try {
            return new BytesRefArray(1, bigArrays);
        } catch (CircuitBreakingException e) {
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
            return null;
        }
    }

    public void testAppendDiscardsEntryRefusedByCircuitBreaker() {
        // Tripping at random spreads the interruptions over growing the byte storage, growing the offset tables
        // and the switch away from the fixed-length encoding. Whichever one is interrupted, the array has to
        // look exactly as it did before the refused append and keep accepting entries afterwards.
        CrankyCircuitBreakerService breakerService = new CrankyCircuitBreakerService();
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), breakerService, true);
        boolean verified = false;
        for (int attempt = 0; attempt < MAX_PASSES && verified == false; attempt++) {
            BytesRefArray array = newArrayOrNullIfRefused(bigArrays);
            if (array == null) {
                continue;
            }
            List<BytesRef> expected = new ArrayList<>();
            int refusals = 0;
            try (array) {
                for (int i = 0; i < ENTRIES_SPANNING_MANY_PAGES; i++) {
                    BytesRef value = new BytesRef(randomAlphaOfLengthBetween(1, 500));
                    try {
                        array.append(value);
                        expected.add(BytesRef.deepCopyOf(value));
                    } catch (CircuitBreakingException e) {
                        // the entry must be discarded whole, so it is deliberately not added to `expected`
                        assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
                        refusals++;
                    }
                }
                assertThat(array.size(), equalTo((long) expected.size()));
                BytesRef scratch = new BytesRef();
                for (int i = 0; i < expected.size(); i++) {
                    assertThat(array.get(i, scratch), equalTo(expected.get(i)));
                }
                verified = refusals > 0;
            }
        }
        assertThat("no pass ever saw an append refused", verified, equalTo(true));
        assertThat(breakerService.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
    }

    public void testAppendCursorDiscardsEntryRefusedByCircuitBreaker() {
        // As above but through the cursor overload, which reserves its offset the same way but copies the bytes
        // chunk by chunk out of the cursor rather than from a single array.
        CrankyCircuitBreakerService breakerService = new CrankyCircuitBreakerService();
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), breakerService, true);
        PagedBytesCursor cursor = new PagedBytesCursor();
        boolean verified = false;
        for (int attempt = 0; attempt < MAX_PASSES && verified == false; attempt++) {
            BytesRefArray array = newArrayOrNullIfRefused(bigArrays);
            if (array == null) {
                continue;
            }
            List<BytesRef> expected = new ArrayList<>();
            int refusals = 0;
            try (array) {
                for (int i = 0; i < ENTRIES_SPANNING_MANY_PAGES; i++) {
                    byte[] data = randomByteArrayOfLength(between(1, 500));
                    cursor.init(data, 0, data.length);
                    try {
                        array.append(cursor);
                        expected.add(new BytesRef(data));
                    } catch (CircuitBreakingException e) {
                        // the entry must be discarded whole; the cursor itself is left partially consumed, so the
                        // refused entry cannot simply be re-offered
                        assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
                        refusals++;
                    }
                }
                assertThat(array.size(), equalTo((long) expected.size()));
                BytesRef scratch = new BytesRef();
                for (int i = 0; i < expected.size(); i++) {
                    assertThat(array.get(i, scratch), equalTo(expected.get(i)));
                }
                verified = refusals > 0;
            }
        }
        assertThat("no pass ever saw an append refused", verified, equalTo(true));
        assertThat(breakerService.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
    }

    public void testRefusedFirstAppendLeavesArrayEmpty() {
        // The very first append is the only one that can install the fixed-length encoding, and rolling it back
        // has to uninstall it: an array left claiming a length it never stored reports that length as its
        // largest entry even though it holds none. The initial byte storage is only 3 bytes wide, so any longer
        // entry has to grow it and can therefore be refused.
        CrankyCircuitBreakerService breakerService = new CrankyCircuitBreakerService();
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), breakerService, true);
        // The breaker only trips now and then, so keep making fresh arrays until a first append is refused.
        boolean refused = false;
        for (int attempt = 0; attempt < 500 && refused == false; attempt++) {
            try (BytesRefArray array = new BytesRefArray(1, bigArrays)) {
                try {
                    array.append(new BytesRef(randomAlphaOfLengthBetween(4, 100)));
                } catch (CircuitBreakingException e) {
                    refused = true;
                    assertThat(array.size(), equalTo(0L));
                    assertThat(array.valueMaxByteSize(), equalTo(0));
                }
            } catch (CircuitBreakingException e) {
                // constructing the array can trip too, which leaves no array to inspect
                assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
            }
        }
        assertThat("no first append was ever refused", refused, equalTo(true));
        assertThat(breakerService.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
    }

    public void testAppendDiscardsFixedLengthEntryRefusedByCircuitBreaker() {
        // Entries of a constant length keep the array on its implicit fixed-length encoding, where a refused
        // append records no offset of its own but still has to give back the byte range it reserved. A closing
        // entry of a different length then forces the switch to explicit offset tables, which is what turns any
        // byte range left over from a refusal into entries that read back wrong.
        CrankyCircuitBreakerService breakerService = new CrankyCircuitBreakerService();
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), breakerService, true);
        int fixedLength = between(200, 500);
        boolean verified = false;
        for (int attempt = 0; attempt < MAX_PASSES && verified == false; attempt++) {
            BytesRefArray array = newArrayOrNullIfRefused(bigArrays);
            if (array == null) {
                continue;
            }
            List<BytesRef> expected = new ArrayList<>();
            int refusals = 0;
            try (array) {
                for (int i = 0; i < ENTRIES_SPANNING_MANY_PAGES; i++) {
                    BytesRef value = new BytesRef(randomAlphaOfLength(fixedLength));
                    try {
                        array.append(value);
                        expected.add(BytesRef.deepCopyOf(value));
                    } catch (CircuitBreakingException e) {
                        // the entry must be discarded whole, so it is deliberately not added to `expected`
                        assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
                        refusals++;
                    }
                }
                // Force the switch to explicit offset tables so that it is guaranteed to happen and to see
                // whatever state the rolled-back appends left behind. Retry until the breaker lets it through.
                BytesRef odd = new BytesRef(randomAlphaOfLength(fixedLength + 1));
                boolean appended = false;
                while (appended == false) {
                    try {
                        array.append(odd);
                        appended = true;
                    } catch (CircuitBreakingException e) {
                        assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
                    }
                }
                expected.add(BytesRef.deepCopyOf(odd));

                assertThat(array.size(), equalTo((long) expected.size()));
                BytesRef scratch = new BytesRef();
                for (int i = 0; i < expected.size(); i++) {
                    assertThat(array.get(i, scratch), equalTo(expected.get(i)));
                }
                verified = refusals > 0;
            }
        }
        assertThat("no pass ever saw an append refused", verified, equalTo(true));
        assertThat(breakerService.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
    }

    private static BigArrays mockBigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    private void assertEquality(BytesRefArray original, BytesRefArray copy) {
        BytesRef scratch = new BytesRef();
        BytesRef scratch2 = new BytesRef();

        assertEquals(original.size(), copy.size());

        // check that all keys of original can be found in the copy
        for (int i = 0; i < original.size(); ++i) {
            original.get(i, scratch);
            copy.get(i, scratch2);
            assertEquals(Integer.toString(i), scratch, scratch2);
        }
    }
}
