/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

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

    // TODO: move to production code
    private static final class PagedBytes implements Comparable<PagedBytes>, Releasable {
        private final byte[][] pages;
        private final int length;
        private final Releasable onClose;

        static final PagedBytes EMPTY = new PagedBytes(new byte[0][], 0, () -> {});

        PagedBytes(byte[][] pages, int length, Releasable onClose) {
            if (Assertions.ENABLED) {
                for (int i = 0; i < pages.length - 1; i++) {
                    if (pages[i].length != BYTE_PAGE_SIZE) {
                        throw new IllegalStateException("page " + i + " has length " + pages[i].length + " but expected " + BYTE_PAGE_SIZE);
                    }
                }
                if (pages.length > 0 && pages[pages.length - 1].length > BYTE_PAGE_SIZE) {
                    throw new IllegalStateException(
                        "last page has length " + pages[pages.length - 1].length + " but expected at most " + BYTE_PAGE_SIZE
                    );
                }
            }
            this.pages = pages;
            this.length = length;
            this.onClose = onClose;
        }

        public byte[][] pages() {
            return pages;
        }

        public int length() {
            return length;
        }

        public boolean bytesEquals(BytesRef other) {
            if (length != other.length) {
                return false;
            }
            int otherOffset = other.offset;
            int remaining = length;
            for (byte[] page : pages) {
                int len = Math.min(page.length, remaining);
                for (int i = 0; i < len; i++) {
                    if (page[i] != other.bytes[otherOffset++]) {
                        return false;
                    }
                }
                remaining -= len;
                if (remaining == 0) {
                    break;
                }
            }
            return true;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj.getClass() != PagedBytes.class) {
                return false;
            }
            PagedBytes rhs = (PagedBytes) obj;
            if (this.length != rhs.length) {
                return false;
            }
            return compareTo(rhs) == 0;
        }

        @Override
        public int hashCode() {
            MurmurHash3x86_32 hasher = new MurmurHash3x86_32();
            for (int i = 0; i < pages.length - 1; i++) {
                hasher.fullPage(pages[i]);
            }
            return pages.length == 0
                ? hasher.lastPage(new byte[0], 0)
                : hasher.lastPage(pages[pages.length - 1], length - (pages.length - 1) * BYTE_PAGE_SIZE);
        }

        @Override
        public int compareTo(PagedBytes rhs) {
            int remaining = Math.min(this.length, rhs.length);
            int fullPages = remaining / BYTE_PAGE_SIZE;
            int tail = remaining % BYTE_PAGE_SIZE;

            for (int page = 0; page < fullPages; page++) {
                int diff = Arrays.compareUnsigned(this.pages[page], rhs.pages[page]);
                if (diff != 0) {
                    return diff;
                }
            }

            if (tail > 0) {
                int diff = Arrays.compareUnsigned(this.pages[fullPages], 0, tail, rhs.pages[fullPages], 0, tail);
                if (diff != 0) {
                    return diff;
                }
            }

            return this.length - rhs.length;
        }

        public int compareTo(BytesRef rhs) {
            int remaining = Math.min(this.length, rhs.length);
            int rhsOffset = rhs.offset;
            for (int i = 0; remaining > 0; i++) {
                int pageLen = Math.min(remaining, BYTE_PAGE_SIZE);
                int diff = Arrays.compareUnsigned(this.pages[i], 0, pageLen, rhs.bytes, rhsOffset, rhsOffset + pageLen);
                if (diff != 0) {
                    return diff;
                }
                remaining -= pageLen;
                rhsOffset += pageLen;
            }
            return this.length - rhs.length;
        }

        /**
         * Reset {@code scratch} to point at the start of this ref and return it.
         */
        public PagedBytesCursor cursor(PagedBytesCursor scratch) {
            scratch.init(pages(), 0, 0, length(), true);
            return scratch;
        }

        @Override
        public void close() {
            onClose.close();
        }
    }

    // TODO: move to production code
    private static class PagedBytesBuilder implements Accountable, Releasable, Comparable<PagedBytesBuilder> {
        private static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
        private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
        static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(PagedBytesBuilder.class);
        static final int MIN_SIZE = 64;
        static final int MAX_SMALL_TAIL_SIZE = BYTE_PAGE_SIZE / 2;

        private final PageCacheRecycler recycler;
        private final CircuitBreaker breaker;
        private final String label;

        private Recycler.V<byte[]>[] pages;
        private int usedPages;
        private int allocatedPages;
        private byte[] tail;
        private int tailOffset;

        PagedBytesBuilder(PageCacheRecycler recycler, CircuitBreaker breaker, String label, int initialCapacity) {
            this.recycler = recycler;
            this.breaker = breaker;
            this.label = label;
            int expandedCapacity = initialCapacity <= MIN_SIZE ? MIN_SIZE : nextPowerOfTwo(initialCapacity);
            if (expandedCapacity < MAX_SMALL_TAIL_SIZE) {
                initSmallTailMode(expandedCapacity);
            } else {
                allocatePages(initialCapacity, SHALLOW_SIZE);
            }
        }

        public void append(byte b) {
            if (growTail(1)) {
                appendToTail(b);
            } else {
                appendPaged(b);
            }
        }

        public void append(byte[] b, int off, int len) {
            if (growTail(len)) {
                appendToTail(b, off, len);
            } else {
                appendPaged(b, off, len);
            }
        }

        private void appendToTail(byte b) {
            tail[tailOffset++] = b;
        }

        private void appendToTail(byte[] b, int off, int len) {
            System.arraycopy(b, off, tail, tailOffset, len);
            tailOffset += len;
        }

        private void appendToTail(int v) {
            INT.set(tail, tailOffset, v);
            tailOffset += Integer.BYTES;
        }

        private void appendToTail(long v) {
            LONG.set(tail, tailOffset, v);
            tailOffset += Long.BYTES;
        }

        private void appendNotToTail(byte[] b, int off, int len) {
            for (int i = 0; i < len; i++) {
                tail[tailOffset + i] = (byte) ~b[off + i];
            }
            tailOffset += len;
        }

        private void appendPaged(byte b) {
            if (tailOffset == tail.length) {
                nextPage();
            }
            appendToTail(b);
        }

        private void appendPaged(byte[] b, int off, int len) {
            while (len > 0) {
                if (tailOffset == tail.length) {
                    nextPage();
                }
                int toCopy = Math.min(tail.length - tailOffset, len);
                System.arraycopy(b, off, tail, tailOffset, toCopy);
                tailOffset += toCopy;
                off += toCopy;
                len -= toCopy;
            }
        }

        private void appendPaged(int v) {
            appendPaged((byte) (v >> 24));
            appendPaged((byte) (v >> 16));
            appendPaged((byte) (v >> 8));
            appendPaged((byte) v);
        }

        private void appendPaged(long v) {
            appendPaged((byte) (v >> 56));
            appendPaged((byte) (v >> 48));
            appendPaged((byte) (v >> 40));
            appendPaged((byte) (v >> 32));
            appendPaged((byte) (v >> 24));
            appendPaged((byte) (v >> 16));
            appendPaged((byte) (v >> 8));
            appendPaged((byte) v);
        }

        private void appendNotPaged(byte[] b, int off, int len) {
            while (len > 0) {
                if (tailOffset == tail.length) {
                    nextPage();
                }
                int toCopy = Math.min(tail.length - tailOffset, len);
                appendNotToTail(b, off, toCopy);
                off += toCopy;
                len -= toCopy;
            }
        }

        public void appendNot(byte[] b, int off, int len) {
            if (growTail(len)) {
                appendNotToTail(b, off, len);
            } else {
                appendNotPaged(b, off, len);
            }
        }

        public void append(BytesRef b) {
            append(b.bytes, b.offset, b.length);
        }

        public void append(PagedBytesBuilder b) {
            for (int i = 0; i < b.usedPages - 1; i++) {
                append(b.pages[i].v(), 0, BYTE_PAGE_SIZE);
            }
            if (b.tail != null) {
                append(b.tail, 0, b.tailOffset);
            }
        }

        public void append(PagedBytes b) {
            int remaining = b.length();
            for (byte[] page : b.pages()) {
                int toCopy = Math.min(page.length, remaining);
                append(page, 0, toCopy);
                remaining -= toCopy;
            }
        }

        public void append(int v) {
            if (growTail(Integer.BYTES)) {
                appendToTail(v);
            } else {
                appendPaged(v);
            }
        }

        public void append(long v) {
            if (growTail(Long.BYTES)) {
                appendToTail(v);
            } else {
                appendPaged(v);
            }
        }

        public void appendVInt(int value) {
            if (growTail(Integer.BYTES + 1)) {
                appendVIntToTail(value);
            } else {
                appendVIntPaged(value);
            }
        }

        private void appendVIntToTail(int value) {
            while ((value & ~0x7F) != 0) {
                appendToTail((byte) ((value & 0x7f) | 0x80));
                value >>>= 7;
            }
            appendToTail((byte) value);
        }

        private void appendVIntPaged(int value) {
            while ((value & ~0x7F) != 0) {
                appendPaged((byte) ((value & 0x7f) | 0x80));
                value >>>= 7;
            }
            appendPaged((byte) value);
        }

        public int length() {
            int length = tailOffset;
            if (usedPages > 1) {
                length += (usedPages - 1) * BYTE_PAGE_SIZE;
            }
            return length;
        }

        public void clear() {
            assert mode() != Mode.BUILT : "clear() called on a built PagedBytesBuilder";
            if (pages != null && usedPages > 1) {
                usedPages = 1;
                tail = pages[0].v();
            }
            tailOffset = 0;
        }

        public PagedBytes view() {
            int len = length();
            if (len == 0) {
                return PagedBytes.EMPTY;
            }
            if (usedPages == 0) {
                return new PagedBytes(new byte[][] { tail }, tailOffset, () -> {});
            }
            byte[][] bytePages = new byte[usedPages][];
            for (int i = 0; i < usedPages; i++) {
                bytePages[i] = pages[i].v();
            }
            return new PagedBytes(bytePages, len, () -> {});
        }

        public PagedBytes build() {
            if (length() == 0) {
                return PagedBytes.EMPTY;
            }
            if (usedPages == 0) {
                PagedBytes result = new PagedBytes(new byte[][] { tail }, tailOffset, new Releasable() {
                    private final long charge = ramBytesUsed();

                    @Override
                    public void close() {
                        breaker.addWithoutBreaking(-charge);
                    }
                });
                moveToBuilt();
                return result;
            }
            byte[][] bytePages = new byte[usedPages][];
            for (int i = 0; i < usedPages; i++) {
                bytePages[i] = pages[i].v();
            }
            PagedBytes result = new PagedBytes(bytePages, length(), new Releasable() {
                private final Recycler.V<byte[]>[] recycledPages = pages;
                private final long charge = ramBytesUsed();

                @Override
                public void close() {
                    Releasables.close(Releasables.wrap(recycledPages), () -> breaker.addWithoutBreaking(-charge));
                }
            });
            moveToBuilt();
            return result;
        }

        private void moveToBuilt() {
            pages = null;
            usedPages = 0;
            allocatedPages = 0;
            tail = null;
            tailOffset = 0;
            assert mode() == Mode.BUILT;
        }

        private boolean growTail(int needed) {
            int end = tailOffset + needed;
            if (end <= tail.length) {
                return true;
            }
            if (pages != null) {
                return false;
            }
            int length = nextPowerOfTwo(end);
            if (length > MAX_SMALL_TAIL_SIZE) {
                promoteToPaged(length);
                return length < BYTE_PAGE_SIZE;
            }
            growSmallTail(length);
            return true;
        }

        private void nextPage() {
            maybeGrowPagesArray();
            if (usedPages < allocatedPages) {
                tail = pages[usedPages++].v();
            } else {
                grabNextPageFromRecycler();
            }
            tailOffset = 0;
        }

        private void maybeGrowPagesArray() {
            if (usedPages < pages.length) {
                return;
            }
            int newLength = ArrayUtil.oversize(pages.length + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
            int oldLength = pages.length;
            breaker.addEstimateBytesAndMaybeBreak(pagesRamBytesUsed(newLength), label);
            pages = Arrays.copyOf(pages, newLength);
            breaker.addWithoutBreaking(-pagesRamBytesUsed(oldLength));
        }

        private void grabNextPageFromRecycler() {
            breaker.addEstimateBytesAndMaybeBreak(PAGE_RAM_BYTES_USED, label);
            Recycler.V<byte[]> v = recycler.bytePage(false);
            pages[usedPages++] = v;
            allocatedPages = usedPages;
            tail = v.v();
        }

        private void promoteToPaged(int length) {
            assert mode() == Mode.SMALL_TAIL;
            byte[] oldTail = tail;
            allocatePages(length, 0);
            System.arraycopy(oldTail, 0, tail, 0, tailOffset);
            breaker.addWithoutBreaking(-smallTailRamBytesUsed(oldTail.length));
        }

        @SuppressWarnings("unchecked")
        private void allocatePages(int needed, long extraBytesToReserve) {
            int size = (needed + BYTE_PAGE_SIZE - 1) / BYTE_PAGE_SIZE;
            assert size > 0;
            breaker.addEstimateBytesAndMaybeBreak(extraBytesToReserve + pagesRamBytesUsed(size), label);
            boolean success = false;
            try {
                pages = new Recycler.V[size];
                grabNextPageFromRecycler();
                success = true;
            } finally {
                if (success == false) {
                    pages = null;
                    breaker.addWithoutBreaking(-extraBytesToReserve - pagesRamBytesUsed(size));
                }
            }
        }

        private void growSmallTail(int length) {
            assert length <= MAX_SMALL_TAIL_SIZE;
            breaker.addEstimateBytesAndMaybeBreak(smallTailRamBytesUsed(length), label);
            int oldLength = tail.length;
            tail = Arrays.copyOf(tail, length);
            breaker.addWithoutBreaking(-smallTailRamBytesUsed(oldLength));
        }

        private void initSmallTailMode(int length) {
            assert length <= MAX_SMALL_TAIL_SIZE;
            breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE + smallTailRamBytesUsed(length), label);
            tail = new byte[length];
        }

        private static int nextPowerOfTwo(int n) {
            return 1 << (32 - Integer.numberOfLeadingZeros(n - 1));
        }

        static final long PAGE_RAM_BYTES_USED = RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + BYTE_PAGE_SIZE
        );

        private static long smallTailRamBytesUsed(int size) {
            return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + size);
        }

        private static long pagesRamBytesUsed(int capacity) {
            return RamUsageEstimator.alignObjectSize(
                RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) capacity * RamUsageEstimator.NUM_BYTES_OBJECT_REF
            );
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj.getClass() != PagedBytesBuilder.class) {
                return false;
            }
            PagedBytesBuilder rhs = (PagedBytesBuilder) obj;
            if (this.length() != rhs.length()) {
                return false;
            }
            return compareTo(rhs) == 0;
        }

        @Override
        public int hashCode() {
            if (pages == null) {
                return StringHelper.murmurhash3_x86_32(tail, 0, tailOffset, StringHelper.GOOD_FAST_HASH_SEED);
            }
            MurmurHash3x86_32 hasher = new MurmurHash3x86_32();
            for (int i = 0; i < usedPages - 1; i++) {
                hasher.fullPage(pages[i].v());
            }
            return hasher.lastPage(tail, tailOffset);
        }

        @Override
        public int compareTo(PagedBytesBuilder rhs) {
            int remaining = Math.min(this.length(), rhs.length());
            int fullPages = remaining / BYTE_PAGE_SIZE;
            int tailLen = remaining % BYTE_PAGE_SIZE;

            for (int page = 0; page < fullPages; page++) {
                int diff = Arrays.compareUnsigned(this.pages[page].v(), rhs.pages[page].v());
                if (diff != 0) {
                    return diff;
                }
            }

            if (tailLen > 0) {
                byte[] lhsTail = this.usedPages > 0 ? this.pages[fullPages].v() : this.tail;
                byte[] rhsTail = rhs.usedPages > 0 ? rhs.pages[fullPages].v() : rhs.tail;
                int diff = Arrays.compareUnsigned(lhsTail, 0, tailLen, rhsTail, 0, tailLen);
                if (diff != 0) {
                    return diff;
                }
            }

            return this.length() - rhs.length();
        }

        enum Mode {
            SMALL_TAIL,
            PAGED,
            BUILT,
        }

        Mode mode() {
            if (pages != null) {
                return Mode.PAGED;
            }
            if (tail != null) {
                return Mode.SMALL_TAIL;
            }
            return Mode.BUILT;
        }

        @Override
        public long ramBytesUsed() {
            return switch (mode()) {
                case BUILT -> 0;
                case SMALL_TAIL -> SHALLOW_SIZE + smallTailRamBytesUsed(tail.length);
                case PAGED -> SHALLOW_SIZE + pagesRamBytesUsed(pages.length) + (long) allocatedPages * PAGE_RAM_BYTES_USED;
            };
        }

        @Override
        public String toString() {
            return view().toString();
        }

        @Override
        public void close() {
            if (mode() == Mode.BUILT) {
                return;
            }
            long charge = ramBytesUsed();
            if (pages != null) {
                Releasables.close(pages);
                pages = null;
            }
            tail = null;
            if (charge > 0) {
                breaker.addWithoutBreaking(-charge);
            }
        }
    }

    /**
     * Returns a random byte-array length that sometimes lands on an exact page-multiple
     * so tests exercise page-boundary edge cases.
     */
    private int randomTestArraySize() {
        return randomBoolean() ? between(1, BYTE_PAGE_SIZE * 3) : between(1, 3) * BYTE_PAGE_SIZE;
    }
}
