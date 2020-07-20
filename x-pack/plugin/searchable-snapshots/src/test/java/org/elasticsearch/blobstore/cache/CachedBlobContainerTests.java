/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.blobstore.cache;

import org.elasticsearch.blobstore.cache.CachedBlobContainer.CopyOnReadInputStream;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static org.elasticsearch.blobstore.cache.CachedBlobContainer.DEFAULT_BYTE_ARRAY_SIZE;
import static org.hamcrest.Matchers.equalTo;

public class CachedBlobContainerTests extends ESTestCase {

    private final MockBigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

    public void testCopyOnReadInputStreamDoesNotCopyMoreThanByteArraySize() throws Exception {
        final byte[] expected = randomByteArray();

        final ByteArray byteArray = bigArrays.newByteArray(randomIntBetween(0, DEFAULT_BYTE_ARRAY_SIZE));
        final InputStream stream = new CopyOnReadInputStream(new ByteArrayInputStream(expected), byteArray) {
            @Override
            protected void closeInternal(ReleasableBytesReference releasable) {
                assertThat(getCount(), equalTo((long) expected.length));
                assertArrayEquals(
                    Arrays.copyOfRange(expected, 0, Math.toIntExact(Math.min(expected.length, byteArray.size()))),
                    BytesReference.toBytes(releasable)
                );
                super.closeInternal(releasable);
            }
        };
        randomReads(stream, expected.length);
        stream.close();
    }

    public void testCopyOnReadInputStream() throws Exception {
        final byte[] expected = randomByteArray();
        final ByteArray byteArray = bigArrays.newByteArray(DEFAULT_BYTE_ARRAY_SIZE);

        final int maxBytesToRead = randomIntBetween(0, Math.toIntExact(Math.min(expected.length, byteArray.size())));
        final InputStream stream = new CopyOnReadInputStream(new ByteArrayInputStream(expected), byteArray) {
            @Override
            protected void closeInternal(ReleasableBytesReference releasable) {
                assertThat(getCount(), equalTo((long) maxBytesToRead));
                assertArrayEquals(Arrays.copyOfRange(expected, 0, maxBytesToRead), BytesReference.toBytes(releasable));
                super.closeInternal(releasable);
            }
        };
        randomReads(stream, maxBytesToRead);
        stream.close();
    }

    private static byte[] randomByteArray() {
        return randomByteArrayOfLength(randomIntBetween(0, frequently() ? 512 : 1 << 20)); // rarely up to 1mb;
    }

    private void randomReads(final InputStream stream, final int maxBytesToRead) throws IOException {
        int remaining = maxBytesToRead;
        while (remaining > 0) {
            int read;
            switch (randomInt(3)) {
                case 0: // single byte read
                    read = stream.read();
                    if (read != -1) {
                        remaining--;
                    }
                    break;
                case 1: // buffered read with fixed buffer offset/length
                    read = stream.read(new byte[randomIntBetween(1, remaining)]);
                    if (read != -1) {
                        remaining -= read;
                    }
                    break;
                case 2: // buffered read with random buffer offset/length
                    final byte[] tmp = new byte[randomIntBetween(1, remaining)];
                    final int off = randomIntBetween(0, tmp.length - 1);
                    read = stream.read(tmp, off, randomIntBetween(1, Math.min(1, tmp.length - off)));
                    if (read != -1) {
                        remaining -= read;
                    }
                    break;

                case 3: // mark & reset with intermediate skip()
                    final int toSkip = randomIntBetween(1, remaining);
                    stream.mark(toSkip);
                    assertThat(stream.skip(toSkip), equalTo((long) toSkip));
                    stream.reset();
                    break;
                default:
                    fail("Unsupported test condition in " + getTestName());
            }
        }
    }
}
