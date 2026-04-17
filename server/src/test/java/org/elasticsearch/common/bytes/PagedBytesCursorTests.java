/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import static org.elasticsearch.common.util.PageCacheRecycler.BYTE_PAGE_SIZE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class PagedBytesCursorTests extends ESTestCase {
    private final PageCacheRecycler recycler = new MockPageCacheRecycler(Settings.EMPTY);

    public void testReadByte() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        byte[] bytes = randomByteArrayOfLength(randomTestArraySize());
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            builder.append(bytes, 0, bytes.length);
            try (PagedBytes ref = builder.build()) {
                PagedBytesCursor cursor = ref.cursor(new PagedBytesCursor());
                assertThat(cursor.remaining(), equalTo(bytes.length));
                for (int i = 0; i < bytes.length; i++) {
                    assertThat(cursor.readByte(), equalTo(bytes[i]));
                    assertThat(cursor.remaining(), equalTo(bytes.length - i - 1));
                }
            }
        }
    }

    public void testReadInt() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        int[] values = new int[between(1, 100)];
        for (int i = 0; i < values.length; i++) {
            values[i] = randomInt();
        }
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            for (int v : values) {
                builder.append(v);
            }
            try (PagedBytes ref = builder.build()) {
                PagedBytesCursor cursor = ref.cursor(new PagedBytesCursor());
                assertThat(cursor.remaining(), equalTo(values.length * Integer.BYTES));
                for (int i = 0; i < values.length; i++) {
                    assertThat(cursor.readInt(), equalTo(values[i]));
                    assertThat(cursor.remaining(), equalTo((values.length - i - 1) * Integer.BYTES));
                }
            }
        }
    }

    public void testReadIntCrossPage() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        int value = randomInt();
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            int padding = addGarbageUpToPageBoundary(builder, Integer.BYTES);
            builder.append(value);
            try (PagedBytes ref = builder.build()) {
                PagedBytesCursor cursor = ref.cursor(new PagedBytesCursor());
                skipBytes(cursor, padding);
                assertThat(cursor.readInt(), equalTo(value));
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    public void testReadLong() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        long[] values = new long[between(1, 100)];
        for (int i = 0; i < values.length; i++) {
            values[i] = randomLong();
        }
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            for (long v : values) {
                builder.append(v);
            }
            try (PagedBytes ref = builder.build()) {
                PagedBytesCursor cursor = ref.cursor(new PagedBytesCursor());
                assertThat(cursor.remaining(), equalTo(values.length * Long.BYTES));
                for (int i = 0; i < values.length; i++) {
                    assertThat(cursor.readLong(), equalTo(values[i]));
                    assertThat(cursor.remaining(), equalTo((values.length - i - 1) * Long.BYTES));
                }
            }
        }
    }

    public void testReadVInt() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        int[] values = new int[between(1, 100)];
        for (int i = 0; i < values.length; i++) {
            values[i] = randomInt();
        }
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            for (int v : values) {
                builder.appendVInt(v);
            }
            int totalBytes = builder.length();
            try (PagedBytes ref = builder.build()) {
                PagedBytesCursor cursor = ref.cursor(new PagedBytesCursor());
                assertThat(cursor.remaining(), equalTo(totalBytes));
                for (int v : values) {
                    assertThat(cursor.readVInt(), equalTo(v));
                }
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    public void testReadVIntCrossPage() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        int value = Integer.MAX_VALUE; // always encodes as 5 bytes
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            int padding = addGarbageUpToPageBoundary(builder, 5);
            builder.appendVInt(value);
            try (PagedBytes ref = builder.build()) {
                PagedBytesCursor cursor = ref.cursor(new PagedBytesCursor());
                skipBytes(cursor, padding);
                assertThat(cursor.readVInt(), equalTo(value));
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    public void testReadVIntRespectsRemaining() {
        // 0x80 0x01 encodes vint(128): a 2-byte VInt.
        byte[] page = new byte[BYTE_PAGE_SIZE];
        page[0] = (byte) 0x80;
        page[1] = 0x01;
        PagedBytesCursor cursor = new PagedBytesCursor();
        // remaining=1, but the page has >= 5 bytes from pageOffset=0.
        // The fast path in readVInt() checks only page space, not remaining,
        // so it reads 2 bytes and drives remaining to -1 instead of throwing.
        cursor.init(page, 0, 1);
        expectThrows(IllegalArgumentException.class, cursor::readVInt);
    }

    public void testSliceTerminated() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        byte terminator = randomByte();
        byte[] expected = randomByteArrayOfLength(between(0, 200));
        for (int i = 0; i < expected.length; i++) {
            if (expected[i] == terminator) expected[i] = (byte) (terminator + 1);
        }
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            builder.append(expected, 0, expected.length);
            builder.append(terminator);
            try (PagedBytes ref = builder.build()) {
                PagedBytesCursor cursor = ref.cursor(new PagedBytesCursor());
                PagedBytesCursor scratch = new PagedBytesCursor();
                PagedBytesCursor result = cursor.sliceTerminated(terminator, scratch);
                assertThat(result.remaining(), equalTo(expected.length));
                byte[] actual = new byte[expected.length];
                for (int i = 0; i < actual.length; i++) {
                    actual[i] = result.readByte();
                }
                assertThat(actual, equalTo(expected));
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    public void testSliceTerminatedCrossPage() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        byte terminator = randomByte();
        byte[] payload = new byte[] { (byte) (terminator + 1), (byte) (terminator + 2), (byte) (terminator + 3), (byte) (terminator + 4) };
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            int padding = addGarbageUpToPageBoundary(builder, payload.length + 1);
            builder.append(payload, 0, payload.length);
            builder.append(terminator);
            try (PagedBytes ref = builder.build()) {
                PagedBytesCursor cursor = ref.cursor(new PagedBytesCursor());
                skipBytes(cursor, padding);
                PagedBytesCursor scratch = new PagedBytesCursor();
                PagedBytesCursor result = cursor.sliceTerminated(terminator, scratch);
                assertThat(result.remaining(), equalTo(payload.length));
                byte[] actual = new byte[payload.length];
                for (int i = 0; i < actual.length; i++) {
                    actual[i] = result.readByte();
                }
                assertThat(actual, equalTo(payload));
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    public void testBitwiseNot() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        byte[] bytes = randomByteArrayOfLength(randomTestArraySize());
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            builder.append(bytes, 0, bytes.length);
            try (PagedBytes ref = builder.build()) {
                PagedBytesCursor cursor = ref.cursor(new PagedBytesCursor());
                cursor.bitwiseNot();
                assertThat(cursor.remaining(), equalTo(bytes.length));
                for (byte b : bytes) {
                    assertThat(cursor.readByte(), equalTo((byte) ~b));
                }
            }
        }
    }

    public void testBitwiseNotAfterPartialRead() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        byte[] bytes = randomByteArrayOfLength(between(2, BYTE_PAGE_SIZE * 3));
        int skip = between(1, bytes.length - 1);
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            builder.append(bytes, 0, bytes.length);
            try (PagedBytes ref = builder.build()) {
                PagedBytesCursor cursor = ref.cursor(new PagedBytesCursor());
                skipBytes(cursor, skip);
                cursor.bitwiseNot();
                assertThat(cursor.remaining(), equalTo(bytes.length - skip));
                for (int i = skip; i < bytes.length; i++) {
                    assertThat(cursor.readByte(), equalTo((byte) ~bytes[i]));
                }
            }
        }
    }

    public void testSlice() {
        byte[] bytes = randomByteArrayOfLength(between(2, BYTE_PAGE_SIZE * 3));
        int sliceStart = between(0, bytes.length - 1);
        int sliceLen = between(1, bytes.length - sliceStart);
        assertSlice(bytes, sliceStart, sliceLen);
    }

    public void testSliceExactlyToPageBoundary() {
        // Fill exactly two pages; slice the first page, then read from the second.
        byte[] firstPage = randomByteArrayOfLength(BYTE_PAGE_SIZE);
        byte[] secondPage = randomByteArrayOfLength(between(1, BYTE_PAGE_SIZE));
        byte[] bytes = new byte[firstPage.length + secondPage.length];
        System.arraycopy(firstPage, 0, bytes, 0, firstPage.length);
        System.arraycopy(secondPage, 0, bytes, firstPage.length, secondPage.length);
        assertSlice(bytes, 0, BYTE_PAGE_SIZE);
    }

    public void testSliceUnknownPages() {
        // Build pages of varying sizes (not BYTE_PAGE_SIZE), then slice across boundaries
        int pageCount = between(2, 5);
        byte[][] pages = new byte[pageCount][];
        int totalLen = 0;
        for (int i = 0; i < pageCount; i++) {
            pages[i] = randomByteArrayOfLength(between(1, 20));
            totalLen += pages[i].length;
        }
        byte[] flat = new byte[totalLen];
        int pos = 0;
        for (byte[] page : pages) {
            System.arraycopy(page, 0, flat, pos, page.length);
            pos += page.length;
        }
        int sliceStart = between(0, totalLen - 1);
        int sliceLen = between(1, totalLen - sliceStart);

        PagedBytesCursor cursor = new PagedBytesCursor();
        if (randomBoolean()) {
            cursor.init(flat, 0, totalLen);
        } else {
            cursor.init(pages, 0, 0, totalLen, false);
        }
        skipBytes(cursor, sliceStart);
        PagedBytesCursor sliceCursor = cursor.slice(sliceLen, new PagedBytesCursor());
        assertThat(cursor.remaining(), equalTo(totalLen - sliceStart - sliceLen));
        assertThat(sliceCursor.remaining(), equalTo(sliceLen));
        for (int i = sliceStart; i < sliceStart + sliceLen; i++) {
            assertThat(sliceCursor.readByte(), equalTo(flat[i]));
        }
        assertThat(sliceCursor.remaining(), equalTo(0));
        for (int i = sliceStart + sliceLen; i < totalLen; i++) {
            assertThat(cursor.readByte(), equalTo(flat[i]));
        }
    }

    private void assertSlice(byte[] bytes, int sliceStart, int sliceLen) {
        if (randomBoolean()) {
            PagedBytesCursor cursor = new PagedBytesCursor();
            cursor.init(bytes, 0, bytes.length);
            doAssertSlice(cursor, bytes, sliceStart, sliceLen);
        } else {
            CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(10));
            try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
                builder.append(bytes, 0, bytes.length);
                try (PagedBytes ref = builder.build()) {
                    PagedBytesCursor cursor = ref.cursor(new PagedBytesCursor());
                    doAssertSlice(cursor, bytes, sliceStart, sliceLen);
                }
            }
        }
    }

    private void doAssertSlice(PagedBytesCursor cursor, byte[] bytes, int sliceStart, int sliceLen) {
        skipBytes(cursor, sliceStart);
        PagedBytesCursor sliceCursor = cursor.slice(sliceLen, new PagedBytesCursor());
        assertThat(cursor.remaining(), equalTo(bytes.length - sliceStart - sliceLen));
        assertThat(sliceCursor.remaining(), equalTo(sliceLen));
        for (int i = sliceStart; i < sliceStart + sliceLen; i++) {
            assertThat(sliceCursor.readByte(), equalTo(bytes[i]));
        }
        assertThat(sliceCursor.remaining(), equalTo(0));
        for (int i = sliceStart + sliceLen; i < bytes.length; i++) {
            assertThat(cursor.readByte(), equalTo(bytes[i]));
        }
    }

    public void testReadLongCrossPage() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        long value = randomLong();
        // Padding puts the long starting 4 bytes before the page boundary
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            int padding = addGarbageUpToPageBoundary(builder, Long.BYTES);
            builder.append(value);
            try (PagedBytes ref = builder.build()) {
                PagedBytesCursor cursor = ref.cursor(new PagedBytesCursor());
                skipBytes(cursor, padding);
                assertThat(cursor.readLong(), equalTo(value));
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    public void testReadPageChunk() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        byte[] expected = randomByteArrayOfLength(between(1, 200));
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            builder.append(expected, 0, expected.length);
            try (PagedBytes ref = builder.build()) {
                PagedBytesCursor cursor = ref.cursor(new PagedBytesCursor());
                BytesRef chunk = cursor.readPageChunk(new BytesRef());
                // Data fits in one page — zero-copy into the backing array
                assertThat(chunk.bytes, sameInstance(ref.pages()[0]));
                assertThat(chunk.length, equalTo(expected.length));
                byte[] actual = new byte[chunk.length];
                System.arraycopy(chunk.bytes, chunk.offset, actual, 0, chunk.length);
                assertThat(actual, equalTo(expected));
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    public void testReadPageChunkCrossPage() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        byte[] expected = randomByteArrayOfLength(between(3, 100));
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            int padding = addGarbageUpToPageBoundary(builder, expected.length);
            builder.append(expected, 0, expected.length);
            try (PagedBytes ref = builder.build()) {
                PagedBytesCursor cursor = ref.cursor(new PagedBytesCursor());
                skipBytes(cursor, padding);
                int firstLen = BYTE_PAGE_SIZE - padding;
                byte[] actual = new byte[expected.length];
                // First chunk: bytes remaining on the first page — zero-copy
                BytesRef firstChunk = cursor.readPageChunk(new BytesRef());
                assertThat(firstChunk.bytes, sameInstance(ref.pages()[0]));
                assertThat(firstChunk.length, equalTo(firstLen));
                assertThat(cursor.remaining(), equalTo(expected.length - firstLen));
                System.arraycopy(firstChunk.bytes, firstChunk.offset, actual, 0, firstLen);
                // Second chunk: the rest on the second page — zero-copy
                BytesRef secondChunk = cursor.readPageChunk(new BytesRef());
                assertThat(secondChunk.bytes, sameInstance(ref.pages()[1]));
                assertThat(secondChunk.length, equalTo(expected.length - firstLen));
                System.arraycopy(secondChunk.bytes, secondChunk.offset, actual, firstLen, secondChunk.length);
                assertThat(actual, equalTo(expected));
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    public void testEquals() {
        byte[] bytes = randomByteArrayOfLength(randomTestArraySize());
        byte[] mutated = bytes.clone();
        mutated[mutated.length - 1] ^= 1;
        PagedBytesCursor original = new PagedBytesCursor();
        original.init(bytes, 0, bytes.length);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(original, cursor -> {
            PagedBytesCursor copy = new PagedBytesCursor();
            copy.init(bytes, 0, bytes.length);
            return copy;
        }, cursor -> {
            PagedBytesCursor mutation = new PagedBytesCursor();
            mutation.init(mutated, 0, mutated.length);
            return mutation;
        });
    }

    public void testEqualsAfterPartial() {
        // Enough bytes for any read: int(4), vint(1 with masked first byte), slice(4), slice(all)
        byte[] bytes = randomByteArrayOfLength(between(9, BYTE_PAGE_SIZE * 3));
        // Guarantee bytes[0] is a valid single-byte vint (bit 7 = 0)
        bytes[0] &= 0x7F;

        PagedBytesCursor cursor = new PagedBytesCursor();
        cursor.init(bytes, 0, bytes.length);

        PagedBytesCursor scratch = new PagedBytesCursor();
        switch (between(0, 3)) {
            case 0 -> cursor.readInt();
            case 1 -> cursor.readVInt();
            case 2 -> cursor.slice(between(1, 4), scratch);
            case 3 -> cursor.slice(cursor.remaining(), scratch);
        }

        int consumed = bytes.length - cursor.remaining();

        if (cursor.remaining() == 0) {
            // Both cursors are empty — just verify equality and matching hash
            PagedBytesCursor copy = new PagedBytesCursor();
            copy.init(bytes, consumed, 0);
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(cursor, c -> {
                PagedBytesCursor cp = new PagedBytesCursor();
                cp.init(bytes, consumed, 0);
                return cp;
            });
        } else {
            byte[] mutated = bytes.clone();
            mutated[bytes.length - 1] ^= 1;
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(cursor, c -> {
                PagedBytesCursor copy = new PagedBytesCursor();
                copy.init(bytes, consumed, bytes.length - consumed);
                return copy;
            }, c -> {
                PagedBytesCursor mutation = new PagedBytesCursor();
                mutation.init(mutated, consumed, mutated.length - consumed);
                return mutation;
            });
        }
    }

    /**
     * After slicing exactly to a page boundary the cursor's {@code pageIndex} is left at
     * {@code pages.length} with {@code remaining == 0} and {@code pageOffset == 0}.
     * The fast path in {@link PagedBytesCursor#hashCode()} then calls {@code pages[pageIndex]}
     * unconditionally, which throws {@link ArrayIndexOutOfBoundsException}.
     */
    public void testHashCodeAfterSliceToPageBoundary() {
        PagedBytesCursor cursor = new PagedBytesCursor();
        cursor.init(new byte[BYTE_PAGE_SIZE], 0, BYTE_PAGE_SIZE);
        PagedBytesCursor scratch = new PagedBytesCursor();
        cursor.slice(BYTE_PAGE_SIZE, scratch);
        // cursor is now drained: remaining == 0, pageIndex == pages.length == 1, pageOffset == 0
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(cursor, c -> {
            PagedBytesCursor copy = new PagedBytesCursor();
            copy.init(new byte[BYTE_PAGE_SIZE], 0, BYTE_PAGE_SIZE);
            copy.slice(BYTE_PAGE_SIZE, new PagedBytesCursor());
            return copy;
        });
    }

    public void testEqualsHashCodeEmptyBytes() {
        PagedBytesCursor original = new PagedBytesCursor();
        original.init(new byte[0], 0, 0);
        assertThat(original.hashCode(), equalTo(new BytesRef(new byte[0]).hashCode()));
        // Copy can be another inited-empty cursor or a completely uninitialized cursor (pages == null)
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(original, c -> {
            if (randomBoolean()) {
                return new PagedBytesCursor();
            }
            PagedBytesCursor copy = new PagedBytesCursor();
            copy.init(new byte[0], 0, 0);
            return copy;
        });
    }

    public void testHashCodeMatchesBytesRef() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        byte[] bytes = randomByteArrayOfLength(between(0, BYTE_PAGE_SIZE * 3));
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            builder.append(bytes, 0, bytes.length);
            try (PagedBytes ref = builder.build()) {
                PagedBytesCursor cursor = ref.cursor(new PagedBytesCursor());
                BytesRef bytesRef = new BytesRef(bytes);
                assertThat(cursor.hashCode(), equalTo(bytesRef.hashCode()));
            }
        }
    }

    public void testToString() {
        byte[] bytes = new byte[] { 0x48, 0x65, 0x6C, 0x6C, 0x6F };
        PagedBytesCursor cursor = new PagedBytesCursor();
        cursor.init(bytes, 0, bytes.length);
        assertThat(cursor.toString(), equalTo("[48 65 6C 6C 6F]"));
    }

    public void testToStringTruncates() {
        byte[] bytes = new byte[101];
        PagedBytesCursor cursor = new PagedBytesCursor();
        cursor.init(bytes, 0, bytes.length);
        assertThat(cursor.toString(), equalTo("[00" + " 00".repeat(99) + " ...1b more...]"));
    }

    public void testToStringTruncatesKilobytes() {
        byte[] bytes = new byte[2251]; // 100 shown + 2151 unrendered = 2.1kb more
        PagedBytesCursor cursor = new PagedBytesCursor();
        cursor.init(bytes, 0, bytes.length);
        assertThat(cursor.toString(), equalTo("[00" + " 00".repeat(99) + " ...2.1kb more...]"));
    }

    private static void skipBytes(PagedBytesCursor cursor, int count) {
        for (int i = 0; i < count; i++) {
            cursor.readByte();
        }
    }

    /**
     * Appends random garbage to {@code builder} so that the next write of {@code width} bytes
     * will cross a page boundary. The crossing point is random: anywhere from the first byte
     * to the last byte of the {@code width}-byte write.
     *
     * @return the number of garbage bytes appended
     */
    private int addGarbageUpToPageBoundary(PagedBytesBuilder builder, int width) {
        int count = BYTE_PAGE_SIZE - width + between(1, width - 1);
        byte[] garbage = randomByteArrayOfLength(count);
        builder.append(garbage, 0, count);
        return count;
    }

    // inner PagedBytes and PagedBytesBuilder classes removed — now production code

    /**
     * Returns a random byte-array length that sometimes lands on an exact page-multiple
     * so tests exercise page-boundary edge cases.
     */
    private int randomTestArraySize() {
        return randomBoolean() ? between(1, BYTE_PAGE_SIZE * 3) : between(1, 3) * BYTE_PAGE_SIZE;
    }
}
