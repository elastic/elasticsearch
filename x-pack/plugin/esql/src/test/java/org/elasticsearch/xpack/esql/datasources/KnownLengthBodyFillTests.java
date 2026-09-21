/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

/**
 * Unit tests for {@link KnownLengthBodyFill}. Message text is pinned so HTTP 206 and S3
 * assertions stay byte-identical after the subscribers switch to this helper.
 */
public class KnownLengthBodyFillTests extends ESTestCase {

    private static final StoragePath HTTP_PATH = StoragePath.of("https://example.com/file.parquet");
    private static final StoragePath S3_PATH = StoragePath.of("s3://test-bucket/data/file.parquet");

    public void testExactFill() {
        byte[] payload = randomByteArrayOfLength(between(0, 64));
        try (DirectReadBuffer dest = dest(payload.length)) {
            KnownLengthBodyFill fill = new KnownLengthBodyFill(randomStore(), randomPath(), payload.length);
            assertNull(fill.copyOrOverflow(dest, ByteBuffer.wrap(payload)));
            assertNull(fill.shortReadOrNull());
            assertEquals(payload.length, fill.offset());
            assertArrayEquals(payload, copiedBytes(dest, fill.offset()));
        }
    }

    public void testOverflowUsesStorePrefixAndPath() {
        for (String store : new String[] { "HTTP", "S3" }) {
            StoragePath path = store.equals("HTTP") ? HTTP_PATH : S3_PATH;
            int expected = between(2, 24);
            int prefix = between(1, expected - 1);
            byte[] filled = randomByteArrayOfLength(prefix);
            byte[] overflow = randomByteArrayOfLength(expected - prefix + between(1, 16));
            AtomicInteger closeCalls = new AtomicInteger();
            byte[] sentinel = randomByteArrayOfLength(expected);
            DirectReadBuffer dest = new DirectReadBuffer(ByteBuffer.wrap(sentinel.clone()), closeCalls::incrementAndGet);
            try {
                KnownLengthBodyFill fill = new KnownLengthBodyFill(store, path, expected);
                assertNull(fill.copyOrOverflow(dest, ByteBuffer.wrap(filled)));
                assertEquals(prefix, fill.offset());
                ExternalUnavailableException eue = fill.copyOrOverflow(dest, ByteBuffer.wrap(overflow));
                assertNotNull(eue);
                assertFalse(eue.throttling());
                assertEquals(
                    store
                        + " response body exceeded expected length reading ["
                        + path
                        + "]: cumulative="
                        + ((long) prefix + overflow.length)
                        + ", expected="
                        + expected,
                    eue.getMessage()
                );
                assertThat(eue.getMessage(), startsWith(store + " "));
                assertThat(eue.getMessage(), containsString(path.toString()));
                assertEquals(prefix, fill.offset());
                assertEquals(0, closeCalls.get());
                assertArrayEquals(filled, copiedBytes(dest, prefix));
                byte[] tail = new byte[expected - prefix];
                dest.buffer().duplicate().clear().position(prefix).get(tail);
                assertArrayEquals(Arrays.copyOfRange(sentinel, prefix, expected), tail);
            } finally {
                dest.close();
            }
            assertEquals(1, closeCalls.get());
        }
    }

    public void testShortReadUsesStorePrefixAndPath() {
        for (String store : new String[] { "HTTP", "S3" }) {
            StoragePath path = store.equals("HTTP") ? HTTP_PATH : S3_PATH;
            byte[] payload = randomByteArrayOfLength(between(0, 16));
            int expected = payload.length + between(1, 16);
            try (DirectReadBuffer dest = dest(expected)) {
                KnownLengthBodyFill fill = new KnownLengthBodyFill(store, path, expected);
                if (payload.length > 0) {
                    assertNull(fill.copyOrOverflow(dest, ByteBuffer.wrap(payload)));
                }
                ExternalUnavailableException eue = fill.shortReadOrNull();
                assertNotNull(eue);
                assertFalse(eue.throttling());
                assertEquals(
                    store
                        + " response body shorter than expected reading ["
                        + path
                        + "]: received="
                        + payload.length
                        + ", expected="
                        + expected,
                    eue.getMessage()
                );
                assertThat(eue.getMessage(), startsWith(store + " "));
                assertThat(eue.getMessage(), containsString(path.toString()));
            }
        }
    }

    public void testCopyBoundedLeavesExtraChunkBytes() {
        byte[] window = randomByteArrayOfLength(between(1, 32));
        byte[] extra = randomByteArrayOfLength(between(1, 16));
        byte[] combined = new byte[window.length + extra.length];
        System.arraycopy(window, 0, combined, 0, window.length);
        System.arraycopy(extra, 0, combined, window.length, extra.length);
        ByteBuffer chunk = ByteBuffer.wrap(combined);
        try (DirectReadBuffer dest = dest(window.length)) {
            KnownLengthBodyFill fill = new KnownLengthBodyFill(randomStore(), randomPath(), window.length);
            assertEquals(window.length, fill.copyBounded(dest, chunk));
            assertEquals(extra.length, chunk.remaining());
            byte[] leftover = new byte[chunk.remaining()];
            chunk.get(leftover);
            assertArrayEquals(extra, leftover);
            assertNull(fill.shortReadOrNull());
            assertArrayEquals(window, copiedBytes(dest, fill.offset()));
        }
    }

    public void testOffsetAfterMixedChunks() {
        byte[] payload = randomByteArrayOfLength(between(8, 48));
        int first = between(1, payload.length - 1);
        byte[] extra = randomByteArrayOfLength(between(1, 8));
        byte[] secondAndExtra = new byte[payload.length - first + extra.length];
        System.arraycopy(payload, first, secondAndExtra, 0, payload.length - first);
        System.arraycopy(extra, 0, secondAndExtra, payload.length - first, extra.length);
        ByteBuffer second = ByteBuffer.wrap(secondAndExtra);
        try (DirectReadBuffer dest = dest(payload.length)) {
            KnownLengthBodyFill fill = new KnownLengthBodyFill(randomStore(), randomPath(), payload.length);
            assertNull(fill.copyOrOverflow(dest, ByteBuffer.wrap(payload, 0, first)));
            assertEquals(first, fill.offset());
            assertEquals(payload.length - first, fill.copyBounded(dest, second));
            assertEquals(extra.length, second.remaining());
            assertEquals(payload.length, fill.offset());
            assertNull(fill.shortReadOrNull());
            assertArrayEquals(payload, copiedBytes(dest, fill.offset()));
        }
    }

    private static String randomStore() {
        return randomBoolean() ? "HTTP" : "S3";
    }

    private static StoragePath randomPath() {
        return randomBoolean() ? HTTP_PATH : S3_PATH;
    }

    private static DirectReadBuffer dest(int capacity) {
        return new DirectReadBuffer(ByteBuffer.allocate(capacity), () -> {});
    }

    private static byte[] copiedBytes(DirectReadBuffer dest, int length) {
        ByteBuffer view = dest.buffer().duplicate().clear().limit(length);
        byte[] bytes = new byte[length];
        view.get(bytes);
        return bytes;
    }
}
