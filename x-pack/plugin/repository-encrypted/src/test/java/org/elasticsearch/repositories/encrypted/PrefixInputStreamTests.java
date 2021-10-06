/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PrefixInputStreamTests extends ESTestCase {

    public void testZeroLength() throws Exception {
        Tuple<AtomicInteger, InputStream> mockTuple = getMockBoundedInputStream(0);
        PrefixInputStream test = new PrefixInputStream(mockTuple.v2(), 1 + Randomness.get().nextInt(32), randomBoolean());
        assertThat(test.available(), Matchers.is(0));
        assertThat(test.read(), Matchers.is(-1));
        assertThat(test.skip(1 + Randomness.get().nextInt(32)), Matchers.is(0L));
    }

    public void testClose() throws Exception {
        int boundedLength = 1 + Randomness.get().nextInt(256);
        Tuple<AtomicInteger, InputStream> mockTuple = getMockBoundedInputStream(boundedLength);
        int prefixLength = Randomness.get().nextInt(boundedLength);
        PrefixInputStream test = new PrefixInputStream(mockTuple.v2(), prefixLength, randomBoolean());
        test.close();
        int byteCountBefore = mockTuple.v1().get();
        IOException e = expectThrows(IOException.class, () -> { test.read(); });
        assertThat(e.getMessage(), Matchers.is("Stream has been closed"));
        e = expectThrows(IOException.class, () -> {
            byte[] b = new byte[1 + Randomness.get().nextInt(32)];
            test.read(b, 0, 1 + Randomness.get().nextInt(b.length));
        });
        assertThat(e.getMessage(), Matchers.is("Stream has been closed"));
        e = expectThrows(IOException.class, () -> { test.skip(1 + Randomness.get().nextInt(32)); });
        assertThat(e.getMessage(), Matchers.is("Stream has been closed"));
        e = expectThrows(IOException.class, () -> { test.available(); });
        assertThat(e.getMessage(), Matchers.is("Stream has been closed"));
        int byteCountAfter = mockTuple.v1().get();
        assertThat(byteCountBefore - byteCountAfter, Matchers.is(0));
        // test closeSource parameter
        AtomicBoolean isClosed = new AtomicBoolean(false);
        InputStream mockIn = mock(InputStream.class);
        doAnswer(new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) {
                isClosed.set(true);
                return null;
            }
        }).when(mockIn).close();
        new PrefixInputStream(mockIn, 1 + Randomness.get().nextInt(32), true).close();
        assertThat(isClosed.get(), Matchers.is(true));
        isClosed.set(false);
        new PrefixInputStream(mockIn, 1 + Randomness.get().nextInt(32), false).close();
        assertThat(isClosed.get(), Matchers.is(false));
    }

    public void testAvailable() throws Exception {
        AtomicInteger available = new AtomicInteger(0);
        int boundedLength = 1 + Randomness.get().nextInt(256);
        InputStream mockIn = mock(InputStream.class);
        when(mockIn.available()).thenAnswer(invocationOnMock -> { return available.get(); });
        PrefixInputStream test = new PrefixInputStream(mockIn, boundedLength, randomBoolean());
        assertThat(test.available(), Matchers.is(0));
        available.set(Randomness.get().nextInt(boundedLength));
        assertThat(test.available(), Matchers.is(available.get()));
        available.set(boundedLength + 1 + Randomness.get().nextInt(boundedLength));
        assertThat(test.available(), Matchers.is(boundedLength));
    }

    public void testReadPrefixLength() throws Exception {
        int boundedLength = 1 + Randomness.get().nextInt(256);
        Tuple<AtomicInteger, InputStream> mockTuple = getMockBoundedInputStream(boundedLength);
        int prefixLength = Randomness.get().nextInt(boundedLength);
        PrefixInputStream test = new PrefixInputStream(mockTuple.v2(), prefixLength, randomBoolean());
        int byteCountBefore = mockTuple.v1().get();
        byte[] b = test.readAllBytes();
        int byteCountAfter = mockTuple.v1().get();
        assertThat(b.length, Matchers.is(prefixLength));
        assertThat(byteCountBefore - byteCountAfter, Matchers.is(prefixLength));
        assertThat(test.read(), Matchers.is(-1));
        assertThat(test.available(), Matchers.is(0));
        assertThat(mockTuple.v2().read(), Matchers.not(-1));
    }

    public void testSkipPrefixLength() throws Exception {
        int boundedLength = 1 + Randomness.get().nextInt(256);
        Tuple<AtomicInteger, InputStream> mockTuple = getMockBoundedInputStream(boundedLength);
        int prefixLength = Randomness.get().nextInt(boundedLength);
        PrefixInputStream test = new PrefixInputStream(mockTuple.v2(), prefixLength, randomBoolean());
        int byteCountBefore = mockTuple.v1().get();
        skipNBytes(test, prefixLength);
        int byteCountAfter = mockTuple.v1().get();
        assertThat(byteCountBefore - byteCountAfter, Matchers.is(prefixLength));
        assertThat(test.read(), Matchers.is(-1));
        assertThat(test.available(), Matchers.is(0));
        assertThat(mockTuple.v2().read(), Matchers.not(-1));
    }

    public void testReadShorterWrapped() throws Exception {
        int boundedLength = 1 + Randomness.get().nextInt(256);
        Tuple<AtomicInteger, InputStream> mockTuple = getMockBoundedInputStream(boundedLength);
        int prefixLength = boundedLength;
        if (randomBoolean()) {
            prefixLength += 1 + Randomness.get().nextInt(boundedLength);
        }
        PrefixInputStream test = new PrefixInputStream(mockTuple.v2(), prefixLength, randomBoolean());
        int byteCountBefore = mockTuple.v1().get();
        byte[] b = test.readAllBytes();
        int byteCountAfter = mockTuple.v1().get();
        assertThat(b.length, Matchers.is(boundedLength));
        assertThat(byteCountBefore - byteCountAfter, Matchers.is(boundedLength));
        assertThat(test.read(), Matchers.is(-1));
        assertThat(test.available(), Matchers.is(0));
        assertThat(mockTuple.v2().read(), Matchers.is(-1));
        assertThat(mockTuple.v2().available(), Matchers.is(0));
    }

    public void testSkipShorterWrapped() throws Exception {
        int boundedLength = 1 + Randomness.get().nextInt(256);
        Tuple<AtomicInteger, InputStream> mockTuple = getMockBoundedInputStream(boundedLength);
        final int prefixLength;
        if (randomBoolean()) {
            prefixLength = boundedLength + 1 + Randomness.get().nextInt(boundedLength);
        } else {
            prefixLength = boundedLength;
        }
        PrefixInputStream test = new PrefixInputStream(mockTuple.v2(), prefixLength, randomBoolean());
        int byteCountBefore = mockTuple.v1().get();
        if (prefixLength == boundedLength) {
            skipNBytes(test, prefixLength);
        } else {
            expectThrows(EOFException.class, () -> { skipNBytes(test, prefixLength); });
        }
        int byteCountAfter = mockTuple.v1().get();
        assertThat(byteCountBefore - byteCountAfter, Matchers.is(boundedLength));
        assertThat(test.read(), Matchers.is(-1));
        assertThat(test.available(), Matchers.is(0));
        assertThat(mockTuple.v2().read(), Matchers.is(-1));
        assertThat(mockTuple.v2().available(), Matchers.is(0));
    }

    private Tuple<AtomicInteger, InputStream> getMockBoundedInputStream(int bound) throws IOException {
        InputStream mockSource = mock(InputStream.class);
        AtomicInteger bytesRemaining = new AtomicInteger(bound);
        when(mockSource.read(org.mockito.Matchers.<byte[]>any(), org.mockito.Matchers.anyInt(), org.mockito.Matchers.anyInt())).thenAnswer(
            invocationOnMock -> {
                final byte[] b = (byte[]) invocationOnMock.getArguments()[0];
                final int off = (int) invocationOnMock.getArguments()[1];
                final int len = (int) invocationOnMock.getArguments()[2];
                if (len == 0) {
                    return 0;
                } else {
                    if (bytesRemaining.get() <= 0) {
                        return -1;
                    }
                    int bytesCount = 1 + Randomness.get().nextInt(Math.min(len, bytesRemaining.get()));
                    bytesRemaining.addAndGet(-bytesCount);
                    return bytesCount;
                }
            }
        );
        when(mockSource.read()).thenAnswer(invocationOnMock -> {
            if (bytesRemaining.get() <= 0) {
                return -1;
            }
            bytesRemaining.decrementAndGet();
            return Randomness.get().nextInt(256);
        });
        when(mockSource.skip(org.mockito.Matchers.anyLong())).thenAnswer(invocationOnMock -> {
            final long n = (long) invocationOnMock.getArguments()[0];
            if (n <= 0 || bytesRemaining.get() <= 0) {
                return 0;
            }
            int bytesSkipped = 1 + Randomness.get().nextInt(Math.min(bytesRemaining.get(), Math.toIntExact(n)));
            bytesRemaining.addAndGet(-bytesSkipped);
            return bytesSkipped;
        });
        when(mockSource.available()).thenAnswer(invocationOnMock -> {
            if (bytesRemaining.get() <= 0) {
                return 0;
            }
            return 1 + Randomness.get().nextInt(bytesRemaining.get());
        });
        when(mockSource.markSupported()).thenReturn(false);
        return new Tuple<>(bytesRemaining, mockSource);
    }

    private static void skipNBytes(InputStream in, long n) throws IOException {
        if (n > 0) {
            long ns = in.skip(n);
            if (ns >= 0 && ns < n) { // skipped too few bytes
                // adjust number to skip
                n -= ns;
                // read until requested number skipped or EOS reached
                while (n > 0 && in.read() != -1) {
                    n--;
                }
                // if not enough skipped, then EOFE
                if (n != 0) {
                    throw new EOFException();
                }
            } else if (ns != n) { // skipped negative or too many bytes
                throw new IOException("Unable to skip exactly");
            }
        }
    }
}
