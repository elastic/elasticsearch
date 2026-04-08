/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.common.util.PageCacheRecycler.BYTE_PAGE_SIZE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class PagedBytesCursorTests extends ESTestCase {
    private final PageCacheRecycler recycler = new MockPageCacheRecycler(Settings.EMPTY);

    public void testReadByte() {
        var breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        byte[] bytes = randomByteArrayOfLength(randomIntBetween(1, BYTE_PAGE_SIZE * 3));
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            builder.append(bytes, 0, bytes.length);
            try (PagedBytes ref = builder.build()) {
                var cursor = ref.cursor(new PagedBytesCursor());
                assertThat(cursor.remaining(), equalTo(bytes.length));
                for (byte b : bytes) {
                    assertThat(cursor.readByte(), equalTo(b));
                }
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    public void testReadInt() {
        var breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        int[] values = new int[randomIntBetween(1, 100)];
        for (int i = 0; i < values.length; i++) {
            values[i] = randomInt();
        }
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            for (int v : values) {
                builder.append(v);
            }
            try (PagedBytes ref = builder.build()) {
                var cursor = ref.cursor(new PagedBytesCursor());
                assertThat(cursor.remaining(), equalTo(values.length * Integer.BYTES));
                for (int v : values) {
                    assertThat(cursor.readInt(), equalTo(v));
                }
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    public void testReadIntCrossPage() {
        var breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        int value = randomInt();
        // Padding puts the int starting 2 bytes before the page boundary
        byte[] padding = new byte[BYTE_PAGE_SIZE - 2];
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            builder.append(padding, 0, padding.length);
            builder.append(value);
            try (PagedBytes ref = builder.build()) {
                var cursor = ref.cursor(new PagedBytesCursor());
                for (int i = 0; i < padding.length; i++) {
                    cursor.readByte();
                }
                assertThat(cursor.readInt(), equalTo(value));
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    public void testReadLong() {
        var breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        long[] values = new long[randomIntBetween(1, 100)];
        for (int i = 0; i < values.length; i++) {
            values[i] = randomLong();
        }
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            for (long v : values) {
                builder.append(v);
            }
            try (PagedBytes ref = builder.build()) {
                var cursor = ref.cursor(new PagedBytesCursor());
                assertThat(cursor.remaining(), equalTo(values.length * Long.BYTES));
                for (long v : values) {
                    assertThat(cursor.readLong(), equalTo(v));
                }
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    public void testReadVInt() {
        var breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        int[] values = new int[randomIntBetween(1, 100)];
        for (int i = 0; i < values.length; i++) {
            values[i] = randomInt();
        }
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            for (int v : values) {
                builder.appendVInt(v);
            }
            int totalBytes = builder.length();
            try (PagedBytes ref = builder.build()) {
                var cursor = ref.cursor(new PagedBytesCursor());
                assertThat(cursor.remaining(), equalTo(totalBytes));
                for (int v : values) {
                    assertThat(cursor.readVInt(), equalTo(v));
                }
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    public void testReadVIntCrossPage() {
        var breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        // Padding puts the 5-byte VInt starting 2 bytes before the page boundary
        byte[] padding = new byte[BYTE_PAGE_SIZE - 2];
        int value = Integer.MAX_VALUE; // always encodes as 5 bytes
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            builder.append(padding, 0, padding.length);
            builder.appendVInt(value);
            try (PagedBytes ref = builder.build()) {
                var cursor = ref.cursor(new PagedBytesCursor());
                for (int i = 0; i < padding.length; i++)
                    cursor.readByte();
                assertThat(cursor.readVInt(), equalTo(value));
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    public void testReadBytesRefWithLen() {
        var breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        byte[] expected = randomByteArrayOfLength(randomIntBetween(1, 200));
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            builder.append(expected, 0, expected.length);
            try (PagedBytes ref = builder.build()) {
                var cursor = ref.cursor(new PagedBytesCursor());
                var scratch = new BytesRef();
                BytesRef result = cursor.readBytesRef(expected.length, scratch);
                assertThat(result.length, equalTo(expected.length));
                assertThat(result.offset, equalTo(0));
                // Small data fits in one page — zero-copy
                assertThat(result.bytes, sameInstance(ref.pages()[0]));
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    public void testReadBytesRefWithLenCrossPage() {
        var breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        // Position 2 bytes before a page boundary so the read spans it
        byte[] padding = new byte[BYTE_PAGE_SIZE - 2];
        byte[] expected = randomByteArrayOfLength(randomIntBetween(3, 100));
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            builder.append(padding, 0, padding.length);
            builder.append(expected, 0, expected.length);
            try (PagedBytes ref = builder.build()) {
                var cursor = ref.cursor(new PagedBytesCursor());
                for (int i = 0; i < padding.length; i++)
                    cursor.readByte();
                var scratch = new BytesRef();
                BytesRef result = cursor.readBytesRef(expected.length, scratch);
                assertThat(result.length, equalTo(expected.length));
                // Cross-page: copied into scratch
                assertThat(result.bytes, sameInstance(scratch.bytes));
                byte[] actual = new byte[expected.length];
                System.arraycopy(result.bytes, result.offset, actual, 0, result.length);
                assertThat(actual, equalTo(expected));
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    public void testReadTerminatedBytesRef() {
        var breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        // Payload contains no 0x00 bytes; terminator is 0x00
        byte[] expected = randomByteArrayOfLength(randomIntBetween(0, 200));
        for (int i = 0; i < expected.length; i++) {
            if (expected[i] == 0) expected[i] = 1;
        }
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            builder.append(expected, 0, expected.length);
            builder.append((byte) 0x00);
            try (PagedBytes ref = builder.build()) {
                var cursor = ref.cursor(new PagedBytesCursor());
                var scratch = new BytesRef(new byte[expected.length]);
                BytesRef result = cursor.readTerminatedBytesRef((byte) 0x00, scratch);
                assertThat(result.length, equalTo(expected.length));
                byte[] actual = new byte[expected.length];
                System.arraycopy(result.bytes, result.offset, actual, 0, result.length);
                assertThat(actual, equalTo(expected));
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    public void testReadTerminatedBytesRefCrossPage() {
        var breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        // Position the terminator just past the page boundary
        byte[] padding = new byte[BYTE_PAGE_SIZE - 2];
        byte[] payload = new byte[] { 1, 2, 3, 4 };  // no 0x00 bytes
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            builder.append(padding, 0, padding.length);
            builder.append(payload, 0, payload.length);
            builder.append((byte) 0x00);
            try (PagedBytes ref = builder.build()) {
                var cursor = ref.cursor(new PagedBytesCursor());
                for (int i = 0; i < padding.length; i++)
                    cursor.readByte();
                var scratch = new BytesRef(new byte[payload.length]);
                BytesRef result = cursor.readTerminatedBytesRef((byte) 0x00, scratch);
                assertThat(result.length, equalTo(payload.length));
                byte[] actual = new byte[payload.length];
                System.arraycopy(result.bytes, result.offset, actual, 0, result.length);
                assertThat(actual, equalTo(payload));
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    public void testBitwiseNot() {
        var breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        byte[] bytes = randomByteArrayOfLength(randomIntBetween(1, BYTE_PAGE_SIZE * 3));
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            builder.append(bytes, 0, bytes.length);
            try (PagedBytes ref = builder.build()) {
                var cursor = ref.cursor(new PagedBytesCursor());
                cursor.bitwiseNot();
                assertThat(cursor.remaining(), equalTo(bytes.length));
                for (byte b : bytes) {
                    assertThat(cursor.readByte(), equalTo((byte) ~b));
                }
            }
        }
    }

    public void testBitwiseNotAfterPartialRead() {
        var breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        byte[] bytes = randomByteArrayOfLength(randomIntBetween(2, BYTE_PAGE_SIZE * 3));
        int skip = randomIntBetween(1, bytes.length - 1);
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            builder.append(bytes, 0, bytes.length);
            try (PagedBytes ref = builder.build()) {
                var cursor = ref.cursor(new PagedBytesCursor());
                for (int i = 0; i < skip; i++) {
                    cursor.readByte();
                }
                cursor.bitwiseNot();
                assertThat(cursor.remaining(), equalTo(bytes.length - skip));
                for (int i = skip; i < bytes.length; i++) {
                    assertThat(cursor.readByte(), equalTo((byte) ~bytes[i]));
                }
            }
        }
    }

    public void testSlice() {
        byte[] bytes = randomByteArrayOfLength(randomIntBetween(2, BYTE_PAGE_SIZE * 3));
        int sliceStart = randomIntBetween(0, bytes.length - 1);
        int sliceLen = randomIntBetween(1, bytes.length - sliceStart);
        assertSlice(bytes, sliceStart, sliceLen);
    }

    public void testSliceExactlyToPageBoundary() {
        // Fill exactly two pages; slice the first page, then read from the second.
        byte[] firstPage = randomByteArrayOfLength(BYTE_PAGE_SIZE);
        byte[] secondPage = randomByteArrayOfLength(randomIntBetween(1, BYTE_PAGE_SIZE));
        byte[] bytes = new byte[firstPage.length + secondPage.length];
        System.arraycopy(firstPage, 0, bytes, 0, firstPage.length);
        System.arraycopy(secondPage, 0, bytes, firstPage.length, secondPage.length);
        assertSlice(bytes, 0, BYTE_PAGE_SIZE);
    }

    private void assertSlice(byte[] bytes, int sliceStart, int sliceLen) {
        var breaker = newLimitedBreaker(ByteSizeValue.ofMb(10));
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            builder.append(bytes, 0, bytes.length);
            try (PagedBytes ref = builder.build()) {
                PagedBytesCursor cursor = ref.cursor(new PagedBytesCursor());
                for (int i = 0; i < sliceStart; i++) {
                    cursor.readByte();
                }
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
        }
    }

    public void testReadLongCrossPage() {
        var breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        long value = randomLong();
        // Padding puts the long starting 4 bytes before the page boundary
        byte[] padding = new byte[BYTE_PAGE_SIZE - 4];
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "test", 0)) {
            builder.append(padding, 0, padding.length);
            builder.append(value);
            try (PagedBytes ref = builder.build()) {
                var cursor = ref.cursor(new PagedBytesCursor());
                for (int i = 0; i < padding.length; i++) {
                    cursor.readByte();
                }
                assertThat(cursor.readLong(), equalTo(value));
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }
}
