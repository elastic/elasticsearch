/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ChainingInputStreamTests extends ESTestCase {

    public void testSkipAcrossComponents() throws Exception {
        ChainingInputStream test = new ChainingInputStream() {
            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                return null;
            }
        };
        byte[] b1 = new byte[1 + Randomness.get().nextInt(16)];
        Randomness.get().nextBytes(b1);
        test.currentIn = new ByteArrayInputStream(b1);
        long nSkip = test.skip(b1.length + 1 + Randomness.get().nextInt(16));
        assertThat(nSkip, Matchers.is((long)b1.length));
        byte[] b2 = new byte[1 + Randomness.get().nextInt(16)];
        Randomness.get().nextBytes(b2);
        test = new ChainingInputStream() {
            boolean second = false;
            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                if (false == second) {
                    second = true;
                    return new ByteArrayInputStream(b2);
                } else {
                    return null;
                }
            }
        };
        test.currentIn = new ByteArrayInputStream(b1);
        long skipArg = b1.length + 1 + Randomness.get().nextInt(b2.length);
        nSkip = test.skip(skipArg);
        assertThat(nSkip, Matchers.is(skipArg));
        byte[] rest = test.readAllBytes();
        assertThat((long)rest.length, Matchers.is(b1.length + b2.length - nSkip));
        for (int i = rest.length - 1; i >= 0; i--) {
            assertThat(rest[i], Matchers.is(b2[i + (int)nSkip - b1.length]));
        }
    }

    public void testEmptyChain() throws Exception {
        ChainingInputStream emptyStream = newEmptyStream();
        assertThat(emptyStream.read(), Matchers.is(-1));
        emptyStream = newEmptyStream();
        byte[] b = new byte[1 + Randomness.get().nextInt(8)];
        int off = Randomness.get().nextInt(b.length);
        assertThat(emptyStream.read(b, off, b.length - off), Matchers.is(-1));
        emptyStream = newEmptyStream();
        assertThat(emptyStream.available(), Matchers.is(0));
        emptyStream = newEmptyStream();
        assertThat(emptyStream.skip(1 + Randomness.get().nextInt(32)), Matchers.is(0L));
    }

    public void testClose() throws Exception {
        ChainingInputStream test1 = newEmptyStream();
        test1.close();
        IOException e = expectThrows(IOException.class, () -> {
            test1.read();
        });
        assertThat(e.getMessage(), Matchers.is("Stream is closed"));
        ChainingInputStream test2 = newEmptyStream();
        test2.close();
        byte[] b = new byte[2 + Randomness.get().nextInt(8)];
        int off = Randomness.get().nextInt(b.length - 1);
        e = expectThrows(IOException.class, () -> {
            test2.read(b, off, Randomness.get().nextInt(b.length - off));
        });
        assertThat(e.getMessage(), Matchers.is("Stream is closed"));
        ChainingInputStream test3 = newEmptyStream();
        test3.close();
        e = expectThrows(IOException.class, () -> {
            test3.skip(Randomness.get().nextInt(32));
        });
        ChainingInputStream test4 = newEmptyStream();
        test4.close();
        e = expectThrows(IOException.class, () -> {
            test4.available();
        });
        ChainingInputStream test5 = newEmptyStream();
        test5.close();
        e = expectThrows(IOException.class, () -> {
            test5.reset();
        });
        ChainingInputStream test6 = newEmptyStream();
        test6.close();
        test6.mark(Randomness.get().nextInt());
    }

    public void testHeadComponentArgumentIsNull() throws Exception {
        AtomicReference<InputStream> headInputStream = new AtomicReference<>();
        AtomicBoolean nextCalled = new AtomicBoolean(false);
        ChainingInputStream test = new ChainingInputStream() {
            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                headInputStream.set(currentComponentIn);
                nextCalled.set(true);
                return null;
            }
        };
        assertThat(test.read(), Matchers.is(-1));
        assertThat(nextCalled.get(), Matchers.is(true));
        assertThat(headInputStream.get(), Matchers.nullValue());
    }

    public void testChaining() throws Exception {
        int componentCount = 2 + Randomness.get().nextInt(8);
        TestInputStream[] sourceComponents = new TestInputStream[componentCount];
        for (int i = 0; i < sourceComponents.length; i++) {
            byte[] b = new byte[Randomness.get().nextInt(2)];
            Randomness.get().nextBytes(b);
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
                    assertThat(currentComponentIn, Matchers.is(sourceComponents[i-1]));
                    return sourceComponents[i++];
                } else if (i == sourceComponents.length) {
                    assertThat(((TestInputStream) currentComponentIn).closed.get(), Matchers.is(true));
                    assertThat(currentComponentIn, Matchers.is(sourceComponents[i-1]));
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
        test.readAllBytes();
    }

    public void testEmptyInputStreamComponents() throws Exception {
        // leading single empty stream
        Tuple<ChainingInputStream, byte[]> test = testEmptyComponentsInChain(3, Arrays.asList(0));
        byte[] result = test.v1().readAllBytes();
        assertThat(result.length, Matchers.is(test.v2().length));
        for (int i = 0; i < result.length; i++) {
            Assert.assertThat(result[i], Matchers.is(test.v2()[i]));
        }
        // leading double empty streams
        test = testEmptyComponentsInChain(3, Arrays.asList(0, 1));
        result = test.v1().readAllBytes();
        assertThat(result.length, Matchers.is(test.v2().length));
        for (int i = 0; i < result.length; i++) {
            Assert.assertThat(result[i], Matchers.is(test.v2()[i]));
        }
        // trailing single empty stream
        test = testEmptyComponentsInChain(3, Arrays.asList(2));
        result = test.v1().readAllBytes();
        assertThat(result.length, Matchers.is(test.v2().length));
        for (int i = 0; i < result.length; i++) {
            Assert.assertThat(result[i], Matchers.is(test.v2()[i]));
        }
        // trailing double empty stream
        test = testEmptyComponentsInChain(3, Arrays.asList(1, 2));
        result = test.v1().readAllBytes();
        assertThat(result.length, Matchers.is(test.v2().length));
        for (int i = 0; i < result.length; i++) {
            Assert.assertThat(result[i], Matchers.is(test.v2()[i]));
        }
        // middle single empty stream
        test = testEmptyComponentsInChain(3, Arrays.asList(1));
        result = test.v1().readAllBytes();
        assertThat(result.length, Matchers.is(test.v2().length));
        for (int i = 0; i < result.length; i++) {
            Assert.assertThat(result[i], Matchers.is(test.v2()[i]));
        }
        // leading and trailing empty streams
        test = testEmptyComponentsInChain(3, Arrays.asList(0, 2));
        result = test.v1().readAllBytes();
        assertThat(result.length, Matchers.is(test.v2().length));
        for (int i = 0; i < result.length; i++) {
            Assert.assertThat(result[i], Matchers.is(test.v2()[i]));
        }
    }

    public void testNullComponentTerminatesChain() throws Exception {
        TestInputStream[] sourceComponents = new TestInputStream[3];
        TestInputStream[] chainComponents = new TestInputStream[5];
        byte[] b1 = new byte[1 + Randomness.get().nextInt(2)];
        Randomness.get().nextBytes(b1);
        sourceComponents[0] = new TestInputStream(b1);
        sourceComponents[1] = null;
        byte[] b2 = new byte[1 + Randomness.get().nextInt(2)];
        Randomness.get().nextBytes(b2);
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
        byte[] b = test.readAllBytes();
        assertThat(b.length, Matchers.is(b1.length));
        for (int i = 0; i < b.length; i++) {
            Assert.assertThat(b[i], Matchers.is(b1[i]));
        }
        assertThat(chainComponents[0], Matchers.nullValue());
        assertThat(chainComponents[1], Matchers.is(sourceComponents[0]));
        assertThat(chainComponents[1].closed.get(), Matchers.is(true));
        assertThat(chainComponents[2], Matchers.nullValue());
        assertThat(chainComponents[3], Matchers.nullValue());
    }

    public void testCallsForwardToCurrentComponent() throws Exception {
        InputStream mockCurrentIn = mock(InputStream.class);
        ChainingInputStream test = new ChainingInputStream() {
            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                throw new IllegalStateException();
            }
        };
        test.currentIn = mockCurrentIn;
        // verify "byte-wise read" is proxied to the current component stream
        when(mockCurrentIn.read()).thenAnswer(invocationOnMock -> Randomness.get().nextInt(256));
        test.read();
        verify(mockCurrentIn).read();
        // verify "array read" is proxied to the current component stream
        when(mockCurrentIn.read(org.mockito.Matchers.<byte[]>any(), org.mockito.Matchers.anyInt(), org.mockito.Matchers.anyInt())).
                thenAnswer(invocationOnMock -> {
                    final int len = (int) invocationOnMock.getArguments()[2];
                    if (len == 0) {
                        return 0;
                    } else {
                        // partial read return
                        int bytesCount = 1 + Randomness.get().nextInt(len);
                        return bytesCount;
                    }
                });
        byte[] b = new byte[2 + Randomness.get().nextInt(32)];
        int len = 1 + Randomness.get().nextInt(b.length - 1);
        int offset = Randomness.get().nextInt(b.length - len);
        test.read(b, offset, len);
        verify(mockCurrentIn).read(Mockito.eq(b), Mockito.eq(offset), Mockito.eq(len));
        // verify "skip" is proxied to the current component stream
        long skipCount = 1 + Randomness.get().nextInt(3);
        test.skip(skipCount);
        verify(mockCurrentIn).skip(Mockito.eq(skipCount));
        // verify "available" is proxied to the current component stream
        test.available();
        verify(mockCurrentIn).available();
    }

    public void testEmptyReadAsksForNext() throws Exception {
        InputStream mockCurrentIn = mock(InputStream.class);
        when(mockCurrentIn.markSupported()).thenAnswer(invocationOnMock -> true);
        ChainingInputStream test = new ChainingInputStream() {
            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                return mockCurrentIn;
            }
        };
        test.currentIn = InputStream.nullInputStream();
        when(mockCurrentIn.read()).thenAnswer(invocationOnMock -> Randomness.get().nextInt(256));
        test.read();
        verify(mockCurrentIn).read();
        // test "array read"
        test.currentIn = InputStream.nullInputStream();
        when(mockCurrentIn.read(org.mockito.Matchers.<byte[]>any(), org.mockito.Matchers.anyInt(), org.mockito.Matchers.anyInt())).
                thenAnswer(invocationOnMock -> {
                    final int len = (int) invocationOnMock.getArguments()[2];
                    if (len == 0) {
                        return 0;
                    } else {
                        int bytesCount = 1 + Randomness.get().nextInt(len);
                        return bytesCount;
                    }
                });
        byte[] b = new byte[2 + Randomness.get().nextInt(32)];
        int len = 1 + Randomness.get().nextInt(b.length - 1);
        int offset = Randomness.get().nextInt(b.length - len);
        test.read(b, offset, len);
        verify(mockCurrentIn).read(Mockito.eq(b), Mockito.eq(offset), Mockito.eq(len));
    }

    public void testReadAll() throws Exception {
        byte[] b = new byte[2 + Randomness.get().nextInt(32)];
        Randomness.get().nextBytes(b);
        int splitIdx = Randomness.get().nextInt(b.length - 1);
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
        assertThat(result.length, Matchers.is(b.length));
        for (int i = 0; i < result.length; i++) {
            Assert.assertThat(result[i], Matchers.is(b[i]));
        }
    }

    public void testMark() throws Exception {
        InputStream mockIn = mock(InputStream.class);
        when(mockIn.markSupported()).thenAnswer(invocationOnMock -> true);
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
        int readLimit = Randomness.get().nextInt(64);
        // mark at the beginning
        test.mark(readLimit);
        assertThat(test.markIn, Matchers.nullValue());
        // mark intermediary position
        when(mockIn.read()).thenAnswer(invocationOnMock -> Randomness.get().nextInt(256));
        test.read();
        assertThat(test.currentIn, Matchers.is(mockIn));
        test.mark(readLimit);
        assertThat(test.markIn, Matchers.is(mockIn));
        verify(mockIn).mark(Mockito.eq(readLimit));
        // mark end position
        when(mockIn.read()).thenAnswer(invocationOnMock -> -1);
        test.read();
        assertThat(test.currentIn, Matchers.is(ChainingInputStream.EXHAUSTED_MARKER));
        verify(mockIn, never()).close();
        readLimit = Randomness.get().nextInt(64);
        test.mark(readLimit);
        verify(mockIn).close();
        assertThat(test.markIn, Matchers.is(ChainingInputStream.EXHAUSTED_MARKER));
    }

    public void testReset() throws Exception {
        InputStream mockMarkIn = mock(InputStream.class);
        when(mockMarkIn.markSupported()).thenAnswer(invocationOnMock -> true);
        InputStream mockCurrentIn = mock(InputStream.class);
        when(mockCurrentIn.markSupported()).thenAnswer(invocationOnMock -> true);
        ChainingInputStream test = new ChainingInputStream() {
            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                return null;
            }
        };
        test.currentIn = mockCurrentIn;
        test.markIn = mockMarkIn;
        test.reset();
        assertThat(test.currentIn, Matchers.is(mockMarkIn));
        assertThat(test.markIn, Matchers.is(mockMarkIn));
        verify(mockMarkIn).reset();
        when(mockCurrentIn.read()).thenAnswer(invocationOnMock -> -1);
        verify(mockMarkIn, never()).close();
        verify(mockCurrentIn).close();
    }

    public void testMarkAfterReset() throws Exception {
        int len = 8 + Randomness.get().nextInt(8);
        byte[] b = new byte[len];
        Randomness.get().nextBytes(b);
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

    private Tuple<ChainingInputStream, byte[]> testEmptyComponentsInChain(int componentCount,
                                                                          List<Integer> emptyComponentIndices) throws Exception {
        byte[] result = new byte[0];
        InputStream[] sourceComponents = new InputStream[componentCount];
        for (int i = 0; i < componentCount; i++) {
            if (emptyComponentIndices.contains(i)) {
                sourceComponents[i] = InputStream.nullInputStream();
            } else {
                byte[] b = new byte[1 + Randomness.get().nextInt(8)];
                Randomness.get().nextBytes(b);
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

    private ChainingInputStream newEmptyStream() {
        return new ChainingInputStream() {
            @Override
            InputStream nextComponent(InputStream currentElementIn) throws IOException {
                return null;
            }
        };
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
            return b[i++];
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
