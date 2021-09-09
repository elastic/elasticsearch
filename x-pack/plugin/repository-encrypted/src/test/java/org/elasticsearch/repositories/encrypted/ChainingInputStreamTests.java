/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ChainingInputStreamTests extends ESTestCase {

    public void testChainComponentsWhenUsingFactoryMethod() throws Exception {
        InputStream input1 = mock(InputStream.class);
        when(input1.markSupported()).thenReturn(true);
        when(input1.read()).thenReturn(randomIntBetween(0, 255));
        InputStream input2 = mock(InputStream.class);
        when(input2.markSupported()).thenReturn(true);
        when(input2.read()).thenReturn(randomIntBetween(0, 255));

        ChainingInputStream chain = ChainingInputStream.chain(input1, input2);

        chain.read();
        verify(input1).read();
        verify(input2, times(0)).read();

        when(input1.read()).thenReturn(-1);
        chain.read();
        verify(input1, times(2)).read();
        verify(input1, times(0)).close();
        verify(input2).read();

        when(input2.read()).thenReturn(-1);
        chain.read();
        verify(input1, times(2)).read();
        verify(input2, times(2)).read();
        verify(input1, times(0)).close();
        verify(input2, times(0)).close();

        chain.close();
        verify(input1).close();
        verify(input2).close();
    }

    public void testMarkAndResetWhenUsingFactoryMethod() throws Exception {
        InputStream input1 = mock(InputStream.class);
        when(input1.markSupported()).thenReturn(true);
        when(input1.read()).thenReturn(randomIntBetween(0, 255));
        InputStream input2 = mock(InputStream.class);
        when(input2.markSupported()).thenReturn(true);
        when(input2.read()).thenReturn(randomIntBetween(0, 255));

        ChainingInputStream chain = ChainingInputStream.chain(input1, input2);
        verify(input1, times(1)).mark(anyInt());
        verify(input2, times(1)).mark(anyInt());

        // mark at the beginning
        chain.mark(randomIntBetween(1, 32));
        verify(input1, times(1)).mark(anyInt());
        verify(input2, times(1)).mark(anyInt());

        verify(input1, times(0)).reset();
        chain.read();
        verify(input1, times(1)).reset();
        chain.reset();
        verify(input1, times(0)).close();
        verify(input1, times(1)).reset();
        chain.read();
        verify(input1, times(2)).reset();

        // mark at the first component
        chain.mark(randomIntBetween(1, 32));
        verify(input1, times(2)).mark(anyInt());
        verify(input2, times(1)).mark(anyInt());

        when(input1.read()).thenReturn(-1);
        chain.read();
        verify(input1, times(0)).close();
        chain.reset();
        verify(input1, times(3)).reset();

        chain.read();
        verify(input2, times(2)).reset();

        // mark at the second component
        chain.mark(randomIntBetween(1, 32));
        verify(input1, times(2)).mark(anyInt());
        verify(input2, times(2)).mark(anyInt());

        when(input2.read()).thenReturn(-1);
        chain.read();
        verify(input1, times(0)).close();
        verify(input2, times(0)).close();
        chain.reset();
        verify(input2, times(3)).reset();

        chain.close();
        verify(input1, times(1)).close();
        verify(input2, times(1)).close();
    }

    public void testSkipWithinComponent() throws Exception {
        byte[] b1 = randomByteArrayOfLength(randomIntBetween(2, 16));
        ChainingInputStream test = new ChainingInputStream() {
            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                if (currentComponentIn == null) {
                    return new ByteArrayInputStream(b1);
                } else {
                    return null;
                }
            }
        };
        int prefix = randomIntBetween(0, b1.length - 2);
        test.readNBytes(prefix);
        // skip less bytes than the component has
        int nSkip1 = randomInt(b1.length - prefix);
        long nSkip = test.skip(nSkip1);
        assertThat((int) nSkip, Matchers.is(nSkip1));
        int nSkip2 = b1.length - prefix - nSkip1 + randomIntBetween(1, 8);
        // skip more bytes than the component has
        nSkip = test.skip(nSkip2);
        assertThat((int) nSkip, Matchers.is(b1.length - prefix - nSkip1));
    }

    public void testSkipAcrossComponents() throws Exception {
        byte[] b1 = randomByteArrayOfLength(randomIntBetween(1, 16));
        byte[] b2 = randomByteArrayOfLength(randomIntBetween(1, 16));
        ChainingInputStream test = new ChainingInputStream() {
            final Iterator<ByteArrayInputStream> iter = List.of(new ByteArrayInputStream(b1), new ByteArrayInputStream(b2)).iterator();

            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                if (iter.hasNext()) {
                    return iter.next();
                } else {
                    return null;
                }
            }
        };
        long skipArg = b1.length + randomIntBetween(1, b2.length);
        long nSkip = test.skip(skipArg);
        assertThat(nSkip, Matchers.is(skipArg));
        byte[] rest = test.readAllBytes();
        assertThat((long) rest.length, Matchers.is(b1.length + b2.length - nSkip));
        for (int i = rest.length - 1; i >= 0; i--) {
            assertThat(rest[i], Matchers.is(b2[i + (int) nSkip - b1.length]));
        }
    }

    public void testEmptyChain() throws Exception {
        // chain is empty because it doesn't have any components
        ChainingInputStream emptyStream = newEmptyStream(false);
        assertThat(emptyStream.read(), Matchers.is(-1));
        emptyStream = newEmptyStream(false);
        byte[] b = randomByteArrayOfLength(randomIntBetween(1, 8));
        int off = randomInt(b.length - 1);
        assertThat(emptyStream.read(b, off, b.length - off), Matchers.is(-1));
        emptyStream = newEmptyStream(false);
        assertThat(emptyStream.available(), Matchers.is(0));
        emptyStream = newEmptyStream(false);
        assertThat(emptyStream.skip(randomIntBetween(1, 32)), Matchers.is(0L));
        // chain is empty because all its components are empty
        emptyStream = newEmptyStream(true);
        assertThat(emptyStream.read(), Matchers.is(-1));
        emptyStream = newEmptyStream(true);
        b = randomByteArrayOfLength(randomIntBetween(1, 8));
        off = randomInt(b.length - 1);
        assertThat(emptyStream.read(b, off, b.length - off), Matchers.is(-1));
        emptyStream = newEmptyStream(true);
        assertThat(emptyStream.available(), Matchers.is(0));
        emptyStream = newEmptyStream(true);
        assertThat(emptyStream.skip(randomIntBetween(1, 32)), Matchers.is(0L));
    }

    public void testClose() throws Exception {
        ChainingInputStream test1 = newEmptyStream(randomBoolean());
        test1.close();
        IOException e = expectThrows(IOException.class, () -> { test1.read(); });
        assertThat(e.getMessage(), Matchers.is("Stream is closed"));
        ChainingInputStream test2 = newEmptyStream(randomBoolean());
        test2.close();
        byte[] b = randomByteArrayOfLength(randomIntBetween(2, 9));
        int off = randomInt(b.length - 2);
        e = expectThrows(IOException.class, () -> { test2.read(b, off, randomInt(b.length - off - 1)); });
        assertThat(e.getMessage(), Matchers.is("Stream is closed"));
        ChainingInputStream test3 = newEmptyStream(randomBoolean());
        test3.close();
        e = expectThrows(IOException.class, () -> { test3.skip(randomInt(31)); });
        assertThat(e.getMessage(), Matchers.is("Stream is closed"));
        ChainingInputStream test4 = newEmptyStream(randomBoolean());
        test4.close();
        e = expectThrows(IOException.class, () -> { test4.available(); });
        assertThat(e.getMessage(), Matchers.is("Stream is closed"));
        ChainingInputStream test5 = newEmptyStream(randomBoolean());
        test5.close();
        e = expectThrows(IOException.class, () -> { test5.reset(); });
        assertThat(e.getMessage(), Matchers.is("Stream is closed"));
        ChainingInputStream test6 = newEmptyStream(randomBoolean());
        test6.close();
        try {
            test6.mark(randomInt());
        } catch (Exception e1) {
            assumeNoException("mark on a closed stream should not throw", e1);
        }
    }

    public void testInitialComponentArgumentIsNull() throws Exception {
        AtomicReference<InputStream> initialInputStream = new AtomicReference<>();
        AtomicBoolean nextCalled = new AtomicBoolean(false);
        ChainingInputStream test = new ChainingInputStream() {
            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                initialInputStream.set(currentComponentIn);
                nextCalled.set(true);
                return null;
            }
        };
        assertThat(test.read(), Matchers.is(-1));
        assertThat(nextCalled.get(), Matchers.is(true));
        assertThat(initialInputStream.get(), Matchers.nullValue());
    }

    public void testChaining() throws Exception {
        int componentCount = randomIntBetween(2, 9);
        ByteBuffer testSource = ByteBuffer.allocate(componentCount);
        TestInputStream[] sourceComponents = new TestInputStream[componentCount];
        for (int i = 0; i < sourceComponents.length; i++) {
            byte[] b = randomByteArrayOfLength(randomInt(1));
            testSource.put(b);
            sourceComponents[i] = new TestInputStream(b);
        }
        ChainingInputStream test = new ChainingInputStream() {
            int i = 0;

            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                if (i == 0) {
                    assertThat(currentComponentIn, Matchers.nullValue());
                    return sourceComponents[i++];
                } else if (i < sourceComponents.length) {
                    assertThat(((TestInputStream) currentComponentIn).closed.get(), Matchers.is(true));
                    assertThat(currentComponentIn, Matchers.is(sourceComponents[i - 1]));
                    return sourceComponents[i++];
                } else if (i == sourceComponents.length) {
                    assertThat(((TestInputStream) currentComponentIn).closed.get(), Matchers.is(true));
                    assertThat(currentComponentIn, Matchers.is(sourceComponents[i - 1]));
                    i++;
                    return null;
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            public boolean markSupported() {
                return false;
            }
        };
        byte[] testArr = test.readAllBytes();
        byte[] ref = testSource.array();
        // testArr and ref should be equal, but ref might have trailing zeroes
        for (int i = 0; i < testArr.length; i++) {
            assertThat(testArr[i], Matchers.is(ref[i]));
        }
    }

    public void testEmptyInputStreamComponents() throws Exception {
        // leading single empty stream
        Tuple<ChainingInputStream, byte[]> test = testEmptyComponentsInChain(3, Arrays.asList(0));
        byte[] result = test.v1().readAllBytes();
        assertThat(result.length, Matchers.is(test.v2().length));
        for (int i = 0; i < result.length; i++) {
            assertThat(result[i], Matchers.is(test.v2()[i]));
        }
        // leading double empty streams
        test = testEmptyComponentsInChain(3, Arrays.asList(0, 1));
        result = test.v1().readAllBytes();
        assertThat(result.length, Matchers.is(test.v2().length));
        for (int i = 0; i < result.length; i++) {
            assertThat(result[i], Matchers.is(test.v2()[i]));
        }
        // trailing single empty stream
        test = testEmptyComponentsInChain(3, Arrays.asList(2));
        result = test.v1().readAllBytes();
        assertThat(result.length, Matchers.is(test.v2().length));
        for (int i = 0; i < result.length; i++) {
            assertThat(result[i], Matchers.is(test.v2()[i]));
        }
        // trailing double empty stream
        test = testEmptyComponentsInChain(3, Arrays.asList(1, 2));
        result = test.v1().readAllBytes();
        assertThat(result.length, Matchers.is(test.v2().length));
        for (int i = 0; i < result.length; i++) {
            assertThat(result[i], Matchers.is(test.v2()[i]));
        }
        // middle single empty stream
        test = testEmptyComponentsInChain(3, Arrays.asList(1));
        result = test.v1().readAllBytes();
        assertThat(result.length, Matchers.is(test.v2().length));
        for (int i = 0; i < result.length; i++) {
            assertThat(result[i], Matchers.is(test.v2()[i]));
        }
        // leading and trailing empty streams
        test = testEmptyComponentsInChain(3, Arrays.asList(0, 2));
        result = test.v1().readAllBytes();
        assertThat(result.length, Matchers.is(test.v2().length));
        for (int i = 0; i < result.length; i++) {
            assertThat(result[i], Matchers.is(test.v2()[i]));
        }
        // all streams are empty
        test = testEmptyComponentsInChain(3, Arrays.asList(0, 1, 2));
        result = test.v1().readAllBytes();
        assertThat(result.length, Matchers.is(0));
    }

    public void testNullComponentTerminatesChain() throws Exception {
        TestInputStream[] sourceComponents = new TestInputStream[3];
        TestInputStream[] chainComponents = new TestInputStream[5];
        byte[] b1 = randomByteArrayOfLength(randomIntBetween(1, 2));
        sourceComponents[0] = new TestInputStream(b1);
        sourceComponents[1] = null;
        byte[] b2 = randomByteArrayOfLength(randomIntBetween(1, 2));
        sourceComponents[2] = new TestInputStream(b2);
        ChainingInputStream test = new ChainingInputStream() {
            int i = 0;

            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                chainComponents[i] = (TestInputStream) currentComponentIn;
                if (i < sourceComponents.length) {
                    return sourceComponents[i++];
                } else {
                    i++;
                    return null;
                }
            }

            @Override
            public boolean markSupported() {
                return false;
            }
        };
        assertThat(test.readAllBytes(), Matchers.equalTo(b1));
        assertThat(chainComponents[0], Matchers.nullValue());
        assertThat(chainComponents[1], Matchers.is(sourceComponents[0]));
        assertThat(chainComponents[1].closed.get(), Matchers.is(true));
        assertThat(chainComponents[2], Matchers.nullValue());
        assertThat(chainComponents[3], Matchers.nullValue());
    }

    public void testCallsForwardToCurrentComponent() throws Exception {
        InputStream mockCurrentIn = mock(InputStream.class);
        when(mockCurrentIn.markSupported()).thenReturn(true);
        ChainingInputStream test = new ChainingInputStream() {
            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                if (currentComponentIn == null) {
                    return mockCurrentIn;
                } else {
                    throw new IllegalStateException();
                }
            }
        };
        // verify "byte-wise read" is proxied to the current component stream
        when(mockCurrentIn.read()).thenReturn(randomInt(255));
        test.read();
        verify(mockCurrentIn).read();
        // verify "array read" is proxied to the current component stream
        when(mockCurrentIn.read(org.mockito.Matchers.<byte[]>any(), org.mockito.Matchers.anyInt(), org.mockito.Matchers.anyInt()))
            .thenAnswer(invocationOnMock -> {
                final int len = (int) invocationOnMock.getArguments()[2];
                if (len == 0) {
                    return 0;
                } else {
                    // partial read return
                    int bytesCount = randomIntBetween(1, len);
                    return bytesCount;
                }
            });
        byte[] b = randomByteArrayOfLength(randomIntBetween(2, 33));
        int len = randomIntBetween(1, b.length - 1);
        int offset = randomInt(b.length - len - 1);
        test.read(b, offset, len);
        verify(mockCurrentIn).read(Mockito.eq(b), Mockito.eq(offset), Mockito.eq(len));
        // verify "skip" is proxied to the current component stream
        long skipCount = randomIntBetween(1, 3);
        test.skip(skipCount);
        verify(mockCurrentIn).skip(Mockito.eq(skipCount));
        // verify "available" is proxied to the current component stream
        test.available();
        verify(mockCurrentIn).available();
    }

    public void testEmptyReadAsksForNext() throws Exception {
        InputStream mockCurrentIn = mock(InputStream.class);
        when(mockCurrentIn.markSupported()).thenReturn(true);
        ChainingInputStream test = new ChainingInputStream() {
            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                return mockCurrentIn;
            }
        };
        test.currentIn = InputStream.nullInputStream();
        when(mockCurrentIn.read()).thenReturn(randomInt(255));
        test.read();
        verify(mockCurrentIn).read();
        // test "array read"
        test.currentIn = InputStream.nullInputStream();
        when(mockCurrentIn.read(org.mockito.Matchers.<byte[]>any(), org.mockito.Matchers.anyInt(), org.mockito.Matchers.anyInt()))
            .thenAnswer(invocationOnMock -> {
                final int len = (int) invocationOnMock.getArguments()[2];
                if (len == 0) {
                    return 0;
                } else {
                    int bytesCount = randomIntBetween(1, len);
                    return bytesCount;
                }
            });
        byte[] b = new byte[randomIntBetween(2, 33)];
        int len = randomIntBetween(1, b.length - 1);
        int offset = randomInt(b.length - len - 1);
        test.read(b, offset, len);
        verify(mockCurrentIn).read(Mockito.eq(b), Mockito.eq(offset), Mockito.eq(len));
    }

    public void testReadAll() throws Exception {
        byte[] b = randomByteArrayOfLength(randomIntBetween(2, 33));
        int splitIdx = randomInt(b.length - 2);
        ByteArrayInputStream first = new ByteArrayInputStream(b, 0, splitIdx + 1);
        ByteArrayInputStream second = new ByteArrayInputStream(b, splitIdx + 1, b.length - splitIdx - 1);
        ChainingInputStream test = new ChainingInputStream() {
            @Override
            InputStream nextComponent(InputStream currentElementIn) throws IOException {
                if (currentElementIn == null) {
                    return first;
                } else if (currentElementIn == first) {
                    return second;
                } else if (currentElementIn == second) {
                    return null;
                } else {
                    throw new IllegalArgumentException();
                }
            }
        };
        byte[] result = test.readAllBytes();
        assertThat(result, Matchers.equalTo(b));
    }

    public void testMarkAtBeginning() throws Exception {
        InputStream mockIn = mock(InputStream.class);
        when(mockIn.markSupported()).thenReturn(true);
        when(mockIn.read()).thenAnswer(invocationOnMock -> randomInt(255));
        ChainingInputStream test = new ChainingInputStream() {
            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                if (currentComponentIn == null) {
                    return mockIn;
                } else {
                    return null;
                }
            }
        };
        assertThat(test.currentIn, Matchers.nullValue());
        // mark at the beginning
        assertThat(test.markIn, Matchers.nullValue());
        test.mark(randomInt(63));
        assertThat(test.markIn, Matchers.nullValue());
        // another mark is a no-op
        test.mark(randomInt(63));
        assertThat(test.markIn, Matchers.nullValue());
        // read does not change the marK
        test.read();
        assertThat(test.currentIn, Matchers.is(mockIn));
        // mark reference is still unchanged
        assertThat(test.markIn, Matchers.nullValue());
        // read reaches end
        when(mockIn.read()).thenReturn(-1);
        test.read();
        assertThat(test.currentIn, Matchers.is(ChainingInputStream.EXHAUSTED_MARKER));
        verify(mockIn).close();
        // mark reference is still unchanged
        assertThat(test.markIn, Matchers.nullValue());
    }

    public void testMarkAtEnding() throws Exception {
        InputStream mockIn = mock(InputStream.class);
        when(mockIn.markSupported()).thenReturn(true);
        when(mockIn.read()).thenAnswer(invocationOnMock -> randomFrom(-1, randomInt(255)));
        ChainingInputStream test = new ChainingInputStream() {
            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                if (currentComponentIn == null) {
                    return mockIn;
                } else {
                    return null;
                }
            }
        };
        // read all bytes
        while (test.read() != -1) {
        }
        assertThat(test.currentIn, Matchers.is(ChainingInputStream.EXHAUSTED_MARKER));
        // mark is null (beginning)
        assertThat(test.markIn, Matchers.nullValue());
        test.mark(randomInt(255));
        assertThat(test.markIn, Matchers.is(ChainingInputStream.EXHAUSTED_MARKER));
        // another mark is a no-op
        test.mark(randomInt(255));
        assertThat(test.markIn, Matchers.is(ChainingInputStream.EXHAUSTED_MARKER));
    }

    public void testSingleMarkAnywhere() throws Exception {
        Supplier<InputStream> mockInputStreamSupplier = () -> {
            InputStream mockIn = mock(InputStream.class);
            when(mockIn.markSupported()).thenReturn(true);
            try {
                when(mockIn.read()).thenAnswer(invocationOnMock -> randomFrom(-1, randomInt(1)));
                when(mockIn.read(org.mockito.Matchers.<byte[]>any(), org.mockito.Matchers.anyInt(), org.mockito.Matchers.anyInt()))
                    .thenAnswer(invocationOnMock -> {
                        final int len = (int) invocationOnMock.getArguments()[2];
                        if (len == 0) {
                            return 0;
                        } else {
                            if (randomBoolean()) {
                                return -1;
                            } else {
                                // partial read return
                                return randomIntBetween(1, len);
                            }
                        }
                    });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return mockIn;
        };
        AtomicBoolean chainingInputStreamEOF = new AtomicBoolean(false);
        ChainingInputStream test = new ChainingInputStream() {
            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                if (chainingInputStreamEOF.get()) {
                    return null;
                } else {
                    return mockInputStreamSupplier.get();
                }
            }
        };
        // possibly skips over several components
        for (int i = 0; i < randomIntBetween(4, 16); i++) {
            test.readNBytes(randomInt(63));
        }
        InputStream currentIn = test.currentIn;
        int readLimit = randomInt(63);
        test.mark(readLimit);
        assertThat(test.currentIn, Matchers.is(currentIn));
        assertThat(test.markIn, Matchers.is(currentIn));
        verify(currentIn).mark(Mockito.eq(readLimit));
        // mark again, same position
        int readLimit2 = randomInt(63);
        test.mark(readLimit2);
        assertThat(test.currentIn, Matchers.is(currentIn));
        assertThat(test.markIn, Matchers.is(currentIn));
        if (readLimit != readLimit2) {
            verify(currentIn).mark(Mockito.eq(readLimit2));
        } else {
            verify(currentIn, times(2)).mark(Mockito.eq(readLimit));
        }
        // read more (possibly moving on to a new component)
        test.readNBytes(randomInt(63));
        // mark does not budge
        assertThat(test.markIn, Matchers.is(currentIn));
        // read until the end
        chainingInputStreamEOF.set(true);
        test.readAllBytes();
        // current component is at the end
        assertThat(test.currentIn, Matchers.is(ChainingInputStream.EXHAUSTED_MARKER));
        // mark is still put
        assertThat(test.markIn, Matchers.is(currentIn));
        verify(test.markIn, never()).close();
        // but close also closes the mark
        test.close();
        verify(test.markIn).close();
    }

    public void testMarkOverwritesPreviousMark() throws Exception {
        AtomicBoolean chainingInputStreamEOF = new AtomicBoolean(false);
        Supplier<InputStream> mockInputStreamSupplier = () -> {
            InputStream mockIn = mock(InputStream.class);
            when(mockIn.markSupported()).thenReturn(true);
            try {
                // single byte read never returns "-1" so it never advances component
                when(mockIn.read()).thenAnswer(invocationOnMock -> randomInt(255));
                when(mockIn.read(org.mockito.Matchers.<byte[]>any(), org.mockito.Matchers.anyInt(), org.mockito.Matchers.anyInt()))
                    .thenAnswer(invocationOnMock -> {
                        final int len = (int) invocationOnMock.getArguments()[2];
                        if (len == 0) {
                            return 0;
                        } else {
                            if (randomBoolean()) {
                                return -1;
                            } else {
                                // partial read return
                                return randomIntBetween(1, len);
                            }
                        }
                    });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return mockIn;
        };
        ChainingInputStream test = new ChainingInputStream() {
            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                if (chainingInputStreamEOF.get()) {
                    return null;
                } else {
                    return mockInputStreamSupplier.get();
                }
            }
        };
        // possibly skips over several components
        for (int i = 0; i < randomIntBetween(4, 16); i++) {
            test.readNBytes(randomInt(63));
        }
        InputStream currentIn = test.currentIn;
        int readLimit = randomInt(63);
        test.mark(readLimit);
        assertThat(test.currentIn, Matchers.is(currentIn));
        assertThat(test.markIn, Matchers.is(currentIn));
        verify(test.markIn).mark(Mockito.eq(readLimit));
        // read more within the same component
        for (int i = 0; i < randomIntBetween(4, 16); i++) {
            test.read();
        }
        // mark does not budge
        assertThat(test.markIn, Matchers.is(currentIn));
        // mark again
        int readLimit2 = randomInt(63);
        test.mark(readLimit2);
        assertThat(test.markIn, Matchers.is(currentIn));
        verify(currentIn, never()).close();
        if (readLimit != readLimit2) {
            verify(currentIn).mark(Mockito.eq(readLimit2));
        } else {
            verify(currentIn, times(2)).mark(Mockito.eq(readLimit));
        }
        // read more while switching the component
        for (int i = 0; i < randomIntBetween(4, 16) || test.currentIn == currentIn; i++) {
            test.readNBytes(randomInt(63));
        }
        // mark does not budge
        assertThat(test.markIn, Matchers.is(currentIn));
        // mark again
        readLimit = randomInt(63);
        test.mark(readLimit);
        assertThat(test.markIn, Matchers.is(test.currentIn));
        // previous mark closed
        verify(currentIn).close();
        verify(test.markIn).mark(Mockito.eq(readLimit));
        InputStream markIn = test.markIn;
        // read until the end
        chainingInputStreamEOF.set(true);
        test.readAllBytes();
        // current component is at the end
        assertThat(test.currentIn, Matchers.is(ChainingInputStream.EXHAUSTED_MARKER));
        // mark is still put
        assertThat(test.markIn, Matchers.is(markIn));
        verify(test.markIn, never()).close();
        // mark at the end
        readLimit = randomInt(63);
        test.mark(readLimit);
        assertThat(test.markIn, Matchers.is(ChainingInputStream.EXHAUSTED_MARKER));
        verify(markIn).close();
    }

    public void testResetAtBeginning() throws Exception {
        InputStream mockIn = mock(InputStream.class);
        when(mockIn.markSupported()).thenReturn(true);
        when(mockIn.read()).thenAnswer(invocationOnMock -> randomInt(255));
        ChainingInputStream test = new ChainingInputStream() {
            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                if (currentComponentIn == null) {
                    return mockIn;
                } else {
                    return null;
                }
            }
        };
        assertThat(test.currentIn, Matchers.nullValue());
        assertThat(test.markIn, Matchers.nullValue());
        if (randomBoolean()) {
            // mark at the beginning
            test.mark(randomInt(63));
            assertThat(test.markIn, Matchers.nullValue());
        }
        // reset immediately
        test.reset();
        assertThat(test.currentIn, Matchers.nullValue());
        // read does not change the marK
        test.read();
        assertThat(test.currentIn, Matchers.is(mockIn));
        // mark reference is still unchanged
        assertThat(test.markIn, Matchers.nullValue());
        // reset back to beginning
        test.reset();
        verify(mockIn).close();
        assertThat(test.currentIn, Matchers.nullValue());
        // read reaches end
        when(mockIn.read()).thenReturn(-1);
        test.read();
        assertThat(test.currentIn, Matchers.is(ChainingInputStream.EXHAUSTED_MARKER));
        // mark reference is still unchanged
        assertThat(test.markIn, Matchers.nullValue());
        // reset back to beginning
        test.reset();
        assertThat(test.currentIn, Matchers.nullValue());
    }

    public void testResetAtEnding() throws Exception {
        InputStream mockIn = mock(InputStream.class);
        when(mockIn.markSupported()).thenReturn(true);
        when(mockIn.read()).thenAnswer(invocationOnMock -> randomFrom(-1, randomInt(255)));
        ChainingInputStream test = new ChainingInputStream() {
            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                if (currentComponentIn == null) {
                    return mockIn;
                } else {
                    return null;
                }
            }
        };
        // read all bytes
        while (test.read() != -1) {
        }
        assertThat(test.currentIn, Matchers.is(ChainingInputStream.EXHAUSTED_MARKER));
        // mark is null (beginning)
        assertThat(test.markIn, Matchers.nullValue());
        test.mark(randomInt(255));
        assertThat(test.markIn, Matchers.is(ChainingInputStream.EXHAUSTED_MARKER));
        // reset
        test.reset();
        assertThat(test.currentIn, Matchers.is(ChainingInputStream.EXHAUSTED_MARKER));
        assertThat(test.read(), Matchers.is(-1));
        // another mark is a no-op
        test.mark(randomInt(255));
        assertThat(test.markIn, Matchers.is(ChainingInputStream.EXHAUSTED_MARKER));
        assertThat(test.read(), Matchers.is(-1));
    }

    public void testResetForSingleMarkAnywhere() throws Exception {
        Supplier<InputStream> mockInputStreamSupplier = () -> {
            InputStream mockIn = mock(InputStream.class);
            when(mockIn.markSupported()).thenReturn(true);
            try {
                // single byte read never returns "-1" so it never advances component
                when(mockIn.read()).thenAnswer(invocationOnMock -> randomInt(255));
                when(mockIn.read(org.mockito.Matchers.<byte[]>any(), org.mockito.Matchers.anyInt(), org.mockito.Matchers.anyInt()))
                    .thenAnswer(invocationOnMock -> {
                        final int len = (int) invocationOnMock.getArguments()[2];
                        if (len == 0) {
                            return 0;
                        } else {
                            if (randomBoolean()) {
                                return -1;
                            } else {
                                // partial read return
                                return randomIntBetween(1, len);
                            }
                        }
                    });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return mockIn;
        };
        AtomicBoolean chainingInputStreamEOF = new AtomicBoolean(false);
        AtomicReference<InputStream> nextComponentArg = new AtomicReference<>();
        ChainingInputStream test = new ChainingInputStream() {
            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                if (nextComponentArg.get() != null) {
                    Assert.assertThat(currentComponentIn, Matchers.is(nextComponentArg.get()));
                    nextComponentArg.set(null);
                }
                if (chainingInputStreamEOF.get()) {
                    return null;
                } else {
                    return mockInputStreamSupplier.get();
                }
            }
        };
        // possibly skips over several components
        for (int i = 0; i < randomIntBetween(4, 16); i++) {
            test.readNBytes(randomInt(63));
        }
        InputStream currentIn = test.currentIn;
        int readLimit = randomInt(63);
        test.mark(readLimit);
        assertThat(test.currentIn, Matchers.is(currentIn));
        assertThat(test.markIn, Matchers.is(currentIn));
        verify(currentIn).mark(Mockito.eq(readLimit));
        // read more without moving to a new component
        for (int i = 0; i < randomIntBetween(4, 16); i++) {
            test.read();
        }
        // first reset
        test.reset();
        assertThat(test.currentIn, Matchers.is(currentIn));
        assertThat(test.markIn, Matchers.is(currentIn));
        verify(test.currentIn, never()).close();
        verify(test.currentIn).reset();
        // read more, moving on to a new component
        for (int i = 0; i < randomIntBetween(4, 16) || test.currentIn == currentIn; i++) {
            test.readNBytes(randomInt(63));
        }
        // mark does not budge
        assertThat(test.markIn, Matchers.is(currentIn));
        assertThat(test.currentIn, Matchers.not(currentIn));
        InputStream lastCurrentIn = test.currentIn;
        // second reset
        test.reset();
        verify(lastCurrentIn).close();
        assertThat(test.currentIn, Matchers.is(currentIn));
        assertThat(test.markIn, Matchers.is(currentIn));
        verify(test.currentIn, times(2)).reset();
        // assert the "nextComponent" argument
        nextComponentArg.set(currentIn);
        // read more, moving on to a new component
        for (int i = 0; i < randomIntBetween(4, 16) || test.currentIn == currentIn; i++) {
            test.readNBytes(randomInt(63));
        }
        // read until the end
        chainingInputStreamEOF.set(true);
        test.readAllBytes();
        // current component is at the end
        assertThat(test.currentIn, Matchers.is(ChainingInputStream.EXHAUSTED_MARKER));
        // mark is still put
        assertThat(test.markIn, Matchers.is(currentIn));
        verify(test.markIn, never()).close();
        // reset when stream is at the end
        test.reset();
        assertThat(test.currentIn, Matchers.is(currentIn));
        assertThat(test.markIn, Matchers.is(currentIn));
        verify(test.currentIn, times(3)).reset();
        // assert the "nextComponent" argument
        nextComponentArg.set(currentIn);
        // read more to verify that current component is passed as nextComponent argument
        test.readAllBytes();
    }

    public void testResetForDoubleMarkAnywhere() throws Exception {
        Supplier<InputStream> mockInputStreamSupplier = () -> {
            InputStream mockIn = mock(InputStream.class);
            when(mockIn.markSupported()).thenReturn(true);
            try {
                // single byte read never returns "-1" so it never advances component
                when(mockIn.read()).thenAnswer(invocationOnMock -> randomInt(255));
                when(mockIn.read(org.mockito.Matchers.<byte[]>any(), org.mockito.Matchers.anyInt(), org.mockito.Matchers.anyInt()))
                    .thenAnswer(invocationOnMock -> {
                        final int len = (int) invocationOnMock.getArguments()[2];
                        if (len == 0) {
                            return 0;
                        } else {
                            if (randomBoolean()) {
                                return -1;
                            } else {
                                // partial read return
                                return randomIntBetween(1, len);
                            }
                        }
                    });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return mockIn;
        };
        AtomicBoolean chainingInputStreamEOF = new AtomicBoolean(false);
        AtomicReference<InputStream> nextComponentArg = new AtomicReference<>();
        ChainingInputStream test = new ChainingInputStream() {
            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                if (nextComponentArg.get() != null) {
                    Assert.assertThat(currentComponentIn, Matchers.is(nextComponentArg.get()));
                    nextComponentArg.set(null);
                }
                if (chainingInputStreamEOF.get()) {
                    return null;
                } else {
                    return mockInputStreamSupplier.get();
                }
            }
        };
        // possibly skips over several components
        for (int i = 0; i < randomIntBetween(4, 16); i++) {
            test.readNBytes(randomInt(63));
        }
        InputStream currentIn = test.currentIn;
        int readLimit = randomInt(63);
        // first mark
        test.mark(readLimit);
        assertThat(test.currentIn, Matchers.is(currentIn));
        assertThat(test.markIn, Matchers.is(currentIn));
        verify(currentIn).mark(Mockito.eq(readLimit));
        // possibly skips over several components
        for (int i = 0; i < randomIntBetween(1, 2); i++) {
            test.readNBytes(randomInt(63));
        }
        InputStream lastCurrentIn = test.currentIn;
        // second mark
        int readLimit2 = randomInt(63);
        test.mark(readLimit2);
        if (lastCurrentIn != currentIn) {
            verify(currentIn).close();
        }
        assertThat(test.currentIn, Matchers.is(lastCurrentIn));
        assertThat(test.markIn, Matchers.is(lastCurrentIn));
        if (currentIn == lastCurrentIn && readLimit == readLimit2) {
            verify(lastCurrentIn, times(2)).mark(Mockito.eq(readLimit));
        } else {
            verify(lastCurrentIn).mark(Mockito.eq(readLimit2));
        }
        currentIn = lastCurrentIn;
        // possibly skips over several components
        for (int i = 0; i < randomIntBetween(1, 2); i++) {
            test.readNBytes(randomInt(63));
        }
        lastCurrentIn = test.currentIn;
        // reset
        test.reset();
        assertThat(test.currentIn, Matchers.is(currentIn));
        assertThat(test.markIn, Matchers.is(currentIn));
        if (lastCurrentIn != currentIn) {
            verify(lastCurrentIn).close();
        }
        verify(currentIn).reset();
        final InputStream firstResetStream = currentIn;
        // assert the "nextComponet" arg is the current component
        nextComponentArg.set(currentIn);
        // possibly skips over several components
        for (int i = 0; i < randomIntBetween(4, 16); i++) {
            test.readNBytes(randomInt(63));
        }
        lastCurrentIn = test.currentIn;
        // third mark after reset
        int readLimit3 = 128 + randomInt(63); // it is harder to assert the cases with the same readLimit
        test.mark(readLimit3);
        if (lastCurrentIn != currentIn) {
            verify(currentIn).close();
        }
        assertThat(test.currentIn, Matchers.is(lastCurrentIn));
        assertThat(test.markIn, Matchers.is(lastCurrentIn));
        verify(lastCurrentIn).mark(Mockito.eq(readLimit3));
        nextComponentArg.set(lastCurrentIn);
        currentIn = lastCurrentIn;
        // possibly skips over several components
        for (int i = 0; i < randomIntBetween(1, 2); i++) {
            test.readNBytes(randomInt(63));
        }
        lastCurrentIn = test.currentIn;
        // reset after mark after reset
        test.reset();
        assertThat(test.currentIn, Matchers.is(currentIn));
        assertThat(test.markIn, Matchers.is(currentIn));
        if (lastCurrentIn != currentIn) {
            verify(lastCurrentIn).close();
        }
        if (currentIn != firstResetStream) {
            verify(currentIn).reset();
        } else {
            verify(currentIn, times(2)).reset();
        }
    }

    public void testMarkAfterResetNoMock() throws Exception {
        int len = randomIntBetween(8, 15);
        byte[] b = randomByteArrayOfLength(len);
        for (int p = 0; p <= len; p++) {
            for (int mark1 = 0; mark1 < len; mark1++) {
                for (int offset1 = 0; offset1 < len - mark1; offset1++) {
                    for (int mark2 = 0; mark2 < len - mark1; mark2++) {
                        for (int offset2 = 0; offset2 < len - mark1 - mark2; offset2++) {
                            final int pivot = p;
                            ChainingInputStream test = new ChainingInputStream() {
                                @Override
                                InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                                    if (currentComponentIn == null) {
                                        return new TestInputStream(b, 0, pivot, 1);
                                    } else if (((TestInputStream) currentComponentIn).label == 1) {
                                        return new TestInputStream(b, pivot, len - pivot, 2);
                                    } else if (((TestInputStream) currentComponentIn).label == 2) {
                                        return null;
                                    } else {
                                        throw new IllegalStateException();
                                    }
                                }
                            };
                            // read "mark1" bytes
                            byte[] pre = test.readNBytes(mark1);
                            for (int i = 0; i < pre.length; i++) {
                                assertThat(pre[i], Matchers.is(b[i]));
                            }
                            // first mark
                            test.mark(len);
                            // read "offset" bytes
                            byte[] span1 = test.readNBytes(offset1);
                            for (int i = 0; i < span1.length; i++) {
                                assertThat(span1[i], Matchers.is(b[mark1 + i]));
                            }
                            // reset back to "mark1" offset
                            test.reset();
                            // read/replay "mark2" bytes
                            byte[] span2 = test.readNBytes(mark2);
                            for (int i = 0; i < span2.length; i++) {
                                assertThat(span2[i], Matchers.is(b[mark1 + i]));
                            }
                            // second mark
                            test.mark(len);
                            byte[] span3 = test.readNBytes(offset2);
                            for (int i = 0; i < span3.length; i++) {
                                assertThat(span3[i], Matchers.is(b[mark1 + mark2 + i]));
                            }
                            // reset to second mark
                            test.reset();
                            // read rest of bytes
                            byte[] span4 = test.readAllBytes();
                            for (int i = 0; i < span4.length; i++) {
                                assertThat(span4[i], Matchers.is(b[mark1 + mark2 + i]));
                            }
                        }
                    }
                }
            }
        }
    }

    private byte[] concatenateArrays(byte[] b1, byte[] b2) {
        byte[] result = new byte[b1.length + b2.length];
        System.arraycopy(b1, 0, result, 0, b1.length);
        System.arraycopy(b2, 0, result, b1.length, b2.length);
        return result;
    }

    private Tuple<ChainingInputStream, byte[]> testEmptyComponentsInChain(int componentCount, List<Integer> emptyComponentIndices)
        throws Exception {
        byte[] result = new byte[0];
        InputStream[] sourceComponents = new InputStream[componentCount];
        for (int i = 0; i < componentCount; i++) {
            if (emptyComponentIndices.contains(i)) {
                sourceComponents[i] = InputStream.nullInputStream();
            } else {
                byte[] b = randomByteArrayOfLength(randomIntBetween(1, 8));
                sourceComponents[i] = new ByteArrayInputStream(b);
                result = concatenateArrays(result, b);
            }
        }
        return new Tuple<>(new ChainingInputStream() {
            int i = 0;

            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                if (i < sourceComponents.length) {
                    return sourceComponents[i++];
                } else {
                    return null;
                }
            }

            @Override
            public boolean markSupported() {
                return false;
            }
        }, result);
    }

    private ChainingInputStream newEmptyStream(boolean hasEmptyComponents) {
        if (hasEmptyComponents) {
            final Iterator<ByteArrayInputStream> iterator = Arrays.asList(
                randomArray(1, 5, ByteArrayInputStream[]::new, () -> new ByteArrayInputStream(new byte[0]))
            ).iterator();
            return new ChainingInputStream() {
                InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                    if (iterator.hasNext()) {
                        return iterator.next();
                    } else {
                        return null;
                    }
                }
            };
        } else {
            return new ChainingInputStream() {
                @Override
                InputStream nextComponent(InputStream currentElementIn) throws IOException {
                    return null;
                }
            };
        }
    }

    static class TestInputStream extends InputStream {

        final byte[] b;
        final int label;
        final int len;
        int i = 0;
        int mark = -1;
        final AtomicBoolean closed = new AtomicBoolean(false);

        TestInputStream(byte[] b) {
            this(b, 0, b.length, 0);
        }

        TestInputStream(byte[] b, int label) {
            this(b, 0, b.length, label);
        }

        TestInputStream(byte[] b, int offset, int len, int label) {
            this.b = b;
            this.i = offset;
            this.len = len;
            this.label = label;
        }

        @Override
        public int read() throws IOException {
            if (b == null || i >= len) {
                return -1;
            }
            return b[i++] & 0xFF;
        }

        @Override
        public void close() throws IOException {
            closed.set(true);
        }

        @Override
        public void mark(int readlimit) {
            this.mark = i;
        }

        @Override
        public void reset() {
            this.i = this.mark;
        }

        @Override
        public boolean markSupported() {
            return true;
        }

    }
}
