/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.blobstore.cache;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
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
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static org.elasticsearch.blobstore.cache.CachedBlobContainer.DEFAULT_BYTE_ARRAY_SIZE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CachedBlobContainerTests extends ESTestCase {

    private final MockBigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

    public void testCopyOnReadInputStreamDoesNotCopyMoreThanByteArraySize() throws Exception {
        final SetOnce<ReleasableBytesReference> onSuccess = new SetOnce<>();
        final SetOnce<Exception> onFailure = new SetOnce<>();
        final ActionListener<ReleasableBytesReference> listener = ActionListener.wrap(onSuccess::set, onFailure::set);

        final byte[] blobContent = randomByteArray();

        final ByteArray byteArray = bigArrays.newByteArray(randomIntBetween(0, DEFAULT_BYTE_ARRAY_SIZE));
        final InputStream stream = new CopyOnReadInputStream(new ByteArrayInputStream(blobContent), byteArray, listener);
        randomReads(stream, blobContent.length);
        stream.close();

        final ReleasableBytesReference releasable = onSuccess.get();
        assertThat(releasable, notNullValue());
        assertThat(releasable.length(), equalTo(Math.toIntExact(Math.min(blobContent.length, byteArray.size()))));
        assertArrayEquals(Arrays.copyOfRange(blobContent, 0, releasable.length()), BytesReference.toBytes(releasable));
        releasable.close();

        final Exception failure = onFailure.get();
        assertThat(failure, nullValue());
    }

    public void testCopyOnReadInputStream() throws Exception {
        final SetOnce<ReleasableBytesReference> onSuccess = new SetOnce<>();
        final SetOnce<Exception> onFailure = new SetOnce<>();
        final ActionListener<ReleasableBytesReference> listener = ActionListener.wrap(onSuccess::set, onFailure::set);

        final byte[] blobContent = randomByteArray();
        final ByteArray byteArray = bigArrays.newByteArray(DEFAULT_BYTE_ARRAY_SIZE);

        final int maxBytesToRead = randomIntBetween(0, blobContent.length);
        final InputStream stream = new CopyOnReadInputStream(new ByteArrayInputStream(blobContent), byteArray, listener);
        randomReads(stream, maxBytesToRead);
        stream.close();

        final ReleasableBytesReference releasable = onSuccess.get();
        assertThat(releasable, notNullValue());
        assertThat(releasable.length(), equalTo((int) Math.min(maxBytesToRead, byteArray.size())));
        assertArrayEquals(Arrays.copyOfRange(blobContent, 0, releasable.length()), BytesReference.toBytes(releasable));
        releasable.close();

        final Exception failure = onFailure.get();
        assertThat(failure, nullValue());
    }

    public void testCopyOnReadWithFailure() throws Exception {
        final SetOnce<ReleasableBytesReference> onSuccess = new SetOnce<>();
        final SetOnce<Exception> onFailure = new SetOnce<>();
        final ActionListener<ReleasableBytesReference> listener = ActionListener.wrap(onSuccess::set, onFailure::set);

        final byte[] blobContent = new byte[0];
        randomByteArray();

        // InputStream that throws an IOException once byte at position N is read/skipped
        final int failAfterNBytesRead = randomIntBetween(0, Math.max(0, blobContent.length - 1));
        final InputStream erroneousStream = new FilterInputStream(new ByteArrayInputStream(blobContent)) {

            long bytesRead;
            long mark;

            void canReadMoreBytes() throws IOException {
                if (failAfterNBytesRead <= bytesRead) {
                    throw new IOException("Cannot read more bytes");
                }
            }

            @Override
            public int read() throws IOException {
                canReadMoreBytes();
                final int read = super.read();
                if (read != -1) {
                    bytesRead++;
                }
                return read;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                canReadMoreBytes();
                final int read = super.read(b, off, Math.min(len, Math.toIntExact(failAfterNBytesRead - bytesRead)));
                if (read != -1) {
                    bytesRead += read;
                }
                return read;
            }

            @Override
            public long skip(long n) throws IOException {
                canReadMoreBytes();
                final long skipped = super.skip(Math.min(n, Math.toIntExact(failAfterNBytesRead - bytesRead)));
                if (skipped > 0L) {
                    bytesRead += skipped;
                }
                return skipped;
            }

            @Override
            public synchronized void reset() throws IOException {
                super.reset();
                bytesRead = mark;
            }

            @Override
            public synchronized void mark(int readlimit) {
                super.mark(readlimit);
                mark = bytesRead;
            }
        };

        final int byteSize = randomIntBetween(0, DEFAULT_BYTE_ARRAY_SIZE);
        try (InputStream stream = new CopyOnReadInputStream(erroneousStream, bigArrays.newByteArray(byteSize), listener)) {
            IOException exception = expectThrows(IOException.class, () -> randomReads(stream, Math.max(1, blobContent.length)));
            assertThat(exception.getMessage(), containsString("Cannot read more bytes"));
        }

        if (failAfterNBytesRead < byteSize) {
            final Exception failure = onFailure.get();
            assertThat(failure, notNullValue());
            assertThat(failure.getMessage(), containsString("Cannot read more bytes"));
            assertThat(onSuccess.get(), nullValue());

        } else {
            final ReleasableBytesReference releasable = onSuccess.get();
            assertThat(releasable, notNullValue());
            assertArrayEquals(Arrays.copyOfRange(blobContent, 0, byteSize), BytesReference.toBytes(releasable));
            assertThat(onFailure.get(), nullValue());
            releasable.close();
        }
    }

    private static byte[] randomByteArray() {
        return randomByteArrayOfLength(randomIntBetween(0, frequently() ? DEFAULT_BYTE_ARRAY_SIZE : 1 << 20)); // rarely up to 1mb;
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
                    stream.skip(toSkip);
                    stream.reset();
                    break;
                default:
                    fail("Unsupported test condition in " + getTestName());
            }
        }
    }
}
