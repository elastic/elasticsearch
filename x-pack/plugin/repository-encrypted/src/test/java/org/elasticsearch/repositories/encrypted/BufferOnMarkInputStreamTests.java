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
import org.junit.BeforeClass;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BufferOnMarkInputStreamTests extends ESTestCase {

    private static byte[] testArray;

    @BeforeClass
    static void createTestArray() throws Exception {
        testArray = new byte[128];
        for (int i = 0; i < testArray.length; i++) {
            testArray[i] = (byte) i;
        }
    }

    public void testResetWithoutMarkFails() throws Exception {
        Tuple<AtomicInteger, InputStream> mockSourceTuple = getMockInfiniteInputStream();
        BufferOnMarkInputStream test = new BufferOnMarkInputStream(mockSourceTuple.v2(), 1 + Randomness.get().nextInt(1024));
        // maybe read some bytes
        test.readNBytes(randomFrom(0, Randomness.get().nextInt(32)));
        IOException e = expectThrows(IOException.class, () -> {
            test.reset();
        });
        assertThat(e.getMessage(), Matchers.is("Mark not called or has been invalidated"));
    }

    public void testMarkAndBufferReadLimitsCheck() throws Exception {
        Tuple<AtomicInteger, InputStream> mockSourceTuple = getMockInfiniteInputStream();
        int bufferSize = 1 + Randomness.get().nextInt(1024);
        BufferOnMarkInputStream test = new BufferOnMarkInputStream(mockSourceTuple.v2(), bufferSize);
        assertThat(test.getMaxMarkReadlimit(), Matchers.is(bufferSize));
        // maybe read some bytes
        test.readNBytes(randomFrom(0, Randomness.get().nextInt(32)));
        int wrongReadLimit = bufferSize + 1 + Randomness.get().nextInt(8);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            test.mark(wrongReadLimit);
        });
        assertThat(e.getMessage(), Matchers.is("Readlimit value [" + wrongReadLimit + "] exceeds the maximum value of ["
                + bufferSize + "]"));
        e = expectThrows(IllegalArgumentException.class, () -> {
            test.mark(-1 - Randomness.get().nextInt(2));
        });
        assertThat(e.getMessage(), Matchers.containsString("cannot be negative"));
        e = expectThrows(IllegalArgumentException.class, () -> {
            new BufferOnMarkInputStream(mock(InputStream.class), 0 - Randomness.get().nextInt(2));
        });
        assertThat(e.getMessage(), Matchers.is("The buffersize constructor argument must be a strictly positive value"));
    }

    public void testCloseRejectsSuccessiveCalls() throws Exception {
        int bufferSize = 3 + Randomness.get().nextInt(32);
        Tuple<AtomicInteger, InputStream> mockSourceTuple = getMockInfiniteInputStream();
        AtomicInteger bytesRead = mockSourceTuple.v1();
        BufferOnMarkInputStream test = new BufferOnMarkInputStream(mockSourceTuple.v2(), bufferSize);
        // maybe read some bytes
        test.readNBytes(randomFrom(0, Randomness.get().nextInt(32)));
        test.close();
        int bytesReadBefore = bytesRead.get();
        IOException e = expectThrows(IOException.class, () -> {
            test.read();
        });
        assertThat(e.getMessage(), Matchers.is("Stream has been closed"));
        e = expectThrows(IOException.class, () -> {
            byte[] b = new byte[1 + Randomness.get().nextInt(32)];
            test.read(b, 0, 1 + Randomness.get().nextInt(b.length));
        });
        assertThat(e.getMessage(), Matchers.is("Stream has been closed"));
        e = expectThrows(IOException.class, () -> {
            test.skip(1 + Randomness.get().nextInt(32));
        });
        assertThat(e.getMessage(), Matchers.is("Stream has been closed"));
        e = expectThrows(IOException.class, () -> {
            test.available();
        });
        assertThat(e.getMessage(), Matchers.is("Stream has been closed"));
        e = expectThrows(IOException.class, () -> {
            test.reset();
        });
        assertThat(e.getMessage(), Matchers.is("Stream has been closed"));
        int bytesReadAfter = bytesRead.get();
        assertThat(bytesReadAfter - bytesReadBefore, Matchers.is(0));
        assertThat(test.getCurrentBufferCount(), Matchers.is(0));
        assertThat(test.getRemainingBufferCapacity(), Matchers.is(bufferSize));
        assertThat(test.getRemainingBufferToRead(), Matchers.is(0));
    }

    public void testBufferingUponMark() throws Exception {
        int bufferSize = 3 + Randomness.get().nextInt(32);
        Tuple<AtomicInteger, InputStream> mockSourceTuple = getMockInfiniteInputStream();
        AtomicInteger bytesRead = mockSourceTuple.v1();
        BufferOnMarkInputStream test = new BufferOnMarkInputStream(mockSourceTuple.v2(), bufferSize);
        assertThat(test.getCurrentBufferCount(), Matchers.is(0));
        assertThat(test.getRemainingBufferCapacity(), Matchers.is(bufferSize));
        assertThat(test.getRemainingBufferToRead(), Matchers.is(0));
        // read without mark
        assertThat(test.read(), Matchers.not(-1));
        int readLen = 1 + Randomness.get().nextInt(8);
        if (randomBoolean()) {
            test.readNBytes(readLen);
        } else {
            skipNBytes(test, readLen);
        }
        assertThat(readLen, Matchers.not(0));
        // assert no buffering
        assertThat(test.getCurrentBufferCount(), Matchers.is(0));
        assertThat(test.getRemainingBufferCapacity(), Matchers.is(bufferSize));
        assertThat(test.getRemainingBufferToRead(), Matchers.is(0));
        // mark
        test.mark(1 + Randomness.get().nextInt(bufferSize));
        // read one byte
        int bytesReadBefore = bytesRead.get();
        assertThat(test.read(), Matchers.not(-1));
        int bytesReadAfter = bytesRead.get();
        // assert byte is "read" and not returned from the buffer
        assertThat(bytesReadAfter - bytesReadBefore, Matchers.is(1));
        // assert byte is buffered
        assertThat(test.getCurrentBufferCount(), Matchers.is(1));
        assertThat(test.getRemainingBufferCapacity(), Matchers.is(bufferSize - 1));
        assertThat(test.getRemainingBufferToRead(), Matchers.is(1));
        assertThat(test.resetCalled, Matchers.is(false));
        // read more bytes, up to buffer size bytes
        bytesReadBefore = bytesRead.get();
        readLen = 1 + Randomness.get().nextInt(bufferSize - 1);
        if (randomBoolean()) {
            test.readNBytes(readLen);
        } else {
            skipNBytes(test, readLen);
        }
        bytesReadAfter = bytesRead.get();
        // assert byte is "read" and not returned from the buffer
        assertThat(bytesReadAfter - bytesReadBefore, Matchers.is(readLen));
        // assert byte is buffered
        assertThat(test.getCurrentBufferCount(), Matchers.is(1 + readLen));
        assertThat(test.getRemainingBufferCapacity(), Matchers.is(bufferSize - 1 - readLen));
        assertThat(test.getRemainingBufferToRead(), Matchers.is(1 + readLen));
        assertThat(test.resetCalled, Matchers.is(false));
    }

    public void testInvalidateMark() throws Exception {
        int bufferSize = 3 + Randomness.get().nextInt(32);
        Tuple<AtomicInteger, InputStream> mockSourceTuple = getMockInfiniteInputStream();
        AtomicInteger bytesRead = mockSourceTuple.v1();
        BufferOnMarkInputStream test = new BufferOnMarkInputStream(mockSourceTuple.v2(), bufferSize);
        assertThat(test.getCurrentBufferCount(), Matchers.is(0));
        assertThat(test.getRemainingBufferCapacity(), Matchers.is(bufferSize));
        assertThat(test.getRemainingBufferToRead(), Matchers.is(0));
        // mark
        test.mark(1 + Randomness.get().nextInt(bufferSize));
        // read all bytes to fill the mark buffer
        int bytesReadBefore = bytesRead.get();
        int readLen = bufferSize;
        if (randomBoolean()) {
            test.readNBytes(readLen);
        } else {
            skipNBytes(test, readLen);
        }
        int bytesReadAfter = bytesRead.get();
        // assert byte is "read" and not returned from the buffer
        assertThat(bytesReadAfter - bytesReadBefore, Matchers.is(readLen));
        // assert byte is buffered
        assertThat(test.getCurrentBufferCount(), Matchers.is(bufferSize));
        assertThat(test.getRemainingBufferCapacity(), Matchers.is(0));
        assertThat(test.getRemainingBufferToRead(), Matchers.is(bufferSize));
        assertThat(test.resetCalled, Matchers.is(false));
        // read another one byte
        bytesReadBefore = bytesRead.get();
        assertThat(test.read(), Matchers.not(-1));
        bytesReadAfter = bytesRead.get();
        // assert byte is "read" and not returned from the buffer
        assertThat(bytesReadAfter - bytesReadBefore, Matchers.is(1));
        // assert mark is invalidated
        assertThat(test.getCurrentBufferCount(), Matchers.is(0));
        assertThat(test.getRemainingBufferCapacity(), Matchers.is(bufferSize));
        assertThat(test.getRemainingBufferToRead(), Matchers.is(0));
        assertThat(test.markCalled, Matchers.is(false));
        // read more bytes
        bytesReadBefore = bytesRead.get();
        readLen = 1 + Randomness.get().nextInt(2 * bufferSize);
        if (randomBoolean()) {
            test.readNBytes(readLen);
        } else {
            skipNBytes(test, readLen);
        }
        bytesReadAfter = bytesRead.get();
        // assert byte is "read" and not returned from the buffer
        assertThat(bytesReadAfter - bytesReadBefore, Matchers.is(readLen));
        // assert byte is NOT buffered
        assertThat(test.getCurrentBufferCount(), Matchers.is(0));
        assertThat(test.getRemainingBufferCapacity(), Matchers.is(bufferSize));
        assertThat(test.getRemainingBufferToRead(), Matchers.is(0));
        assertThat(test.markCalled, Matchers.is(false));
        // assert reset does not work any more
        IOException e = expectThrows(IOException.class, () -> {
            test.reset();
        });
        assertThat(e.getMessage(), Matchers.is("Mark not called or has been invalidated"));
    }

    public void testConsumeBufferUponReset() throws Exception {
        int bufferSize = 3 + Randomness.get().nextInt(128);
        Tuple<AtomicInteger, InputStream> mockSourceTuple = getMockInfiniteInputStream();
        AtomicInteger bytesRead = mockSourceTuple.v1();
        BufferOnMarkInputStream test = new BufferOnMarkInputStream(mockSourceTuple.v2(), bufferSize);
        // maybe read some bytes
        test.readNBytes(randomFrom(0, Randomness.get().nextInt(32)));
        // mark
        test.mark(1 + Randomness.get().nextInt(bufferSize));
        // read less than bufferSize bytes
        int bytesReadBefore = bytesRead.get();
        int readLen = 1 + Randomness.get().nextInt(bufferSize);
        if (randomBoolean()) {
            test.readNBytes(readLen);
        } else {
            skipNBytes(test, readLen);
        }
        int bytesReadAfter = bytesRead.get();
        // assert bytes are "read" and not returned from the buffer
        assertThat(bytesReadAfter - bytesReadBefore, Matchers.is(readLen));
        // assert buffer is populated
        assertThat(test.getCurrentBufferCount(), Matchers.is(readLen));
        assertThat(test.getRemainingBufferCapacity(), Matchers.is(bufferSize - readLen));
        assertThat(test.getRemainingBufferToRead(), Matchers.is(readLen));
        assertThat(test.markCalled, Matchers.is(true));
        // reset
        test.reset();
        assertThat(test.resetCalled, Matchers.is(true));
        // read again, from buffer this time
        bytesReadBefore = bytesRead.get();
        int readLen2 = 1 + Randomness.get().nextInt(readLen);
        if (randomBoolean()) {
            test.readNBytes(readLen2);
        } else {
            skipNBytes(test, readLen2);
        }
        bytesReadAfter = bytesRead.get();
        // assert bytes are replayed from the buffer
        assertThat(bytesReadAfter - bytesReadBefore, Matchers.is(0));
        // assert buffer is consumed
        assertThat(test.getCurrentBufferCount(), Matchers.is(readLen));
        assertThat(test.getRemainingBufferCapacity(), Matchers.is(bufferSize - readLen));
        assertThat(test.getRemainingBufferToRead(), Matchers.is(readLen - readLen2));
        assertThat(test.markCalled, Matchers.is(true));
        assertThat(test.resetCalled, Matchers.is(true));
    }

    public void testInvalidateMarkAfterReset() throws Exception {
        int bufferSize = 3 + Randomness.get().nextInt(128);
        Tuple<AtomicInteger, InputStream> mockSourceTuple = getMockInfiniteInputStream();
        AtomicInteger bytesRead = mockSourceTuple.v1();
        BufferOnMarkInputStream test = new BufferOnMarkInputStream(mockSourceTuple.v2(), bufferSize);
        // maybe read some bytes
        test.readNBytes(randomFrom(0, Randomness.get().nextInt(32)));
        // mark
        test.mark(1 + Randomness.get().nextInt(bufferSize));
        // read less than bufferSize bytes
        int bytesReadBefore = bytesRead.get();
        int readLen = 1 + Randomness.get().nextInt(bufferSize);
        if (randomBoolean()) {
            test.readNBytes(readLen);
        } else {
            skipNBytes(test, readLen);
        }
        int bytesReadAfter = bytesRead.get();
        // assert bytes are "read" and not returned from the buffer
        assertThat(bytesReadAfter - bytesReadBefore, Matchers.is(readLen));
        // assert buffer is populated
        assertThat(test.getCurrentBufferCount(), Matchers.is(readLen));
        assertThat(test.getRemainingBufferCapacity(), Matchers.is(bufferSize - readLen));
        assertThat(test.getRemainingBufferToRead(), Matchers.is(readLen));
        assertThat(test.markCalled, Matchers.is(true));
        // reset
        test.reset();
        assertThat(test.resetCalled, Matchers.is(true));
        assertThat(test.getCurrentBufferCount(), Matchers.is(readLen));
        assertThat(test.getRemainingBufferCapacity(), Matchers.is(bufferSize - readLen));
        assertThat(test.getRemainingBufferToRead(), Matchers.is(readLen));
        // read again, from buffer this time
        bytesReadBefore = bytesRead.get();
        int readLen2 = readLen;
        if (randomBoolean()) {
            test.readNBytes(readLen2);
        } else {
            skipNBytes(test, readLen2);
        }
        bytesReadAfter = bytesRead.get();
        // assert bytes are replayed from the buffer
        assertThat(bytesReadAfter - bytesReadBefore, Matchers.is(0));
        // assert buffer is consumed
        assertThat(test.getCurrentBufferCount(), Matchers.is(readLen));
        assertThat(test.getRemainingBufferCapacity(), Matchers.is(bufferSize - readLen));
        assertThat(test.getRemainingBufferToRead(), Matchers.is(0));
        assertThat(test.markCalled, Matchers.is(true));
        assertThat(test.resetCalled, Matchers.is(true));
        // read on, from the stream, until the mark buffer is full
        bytesReadBefore = bytesRead.get();
        int readLen3 = bufferSize - readLen;
        if (randomBoolean()) {
            test.readNBytes(readLen3);
        } else {
            skipNBytes(test, readLen3);
        }
        bytesReadAfter = bytesRead.get();
        // assert bytes are "read" and not returned from the buffer
        assertThat(bytesReadAfter - bytesReadBefore, Matchers.is(readLen3));
        assertThat(test.getCurrentBufferCount(), Matchers.is(bufferSize));
        assertThat(test.getRemainingBufferCapacity(), Matchers.is(0));
        assertThat(test.markCalled, Matchers.is(true));
        assertThat(test.resetCalled, Matchers.is(false));
        // read more bytes
        bytesReadBefore = bytesRead.get();
        int readLen4 = 1 + Randomness.get().nextInt(2 * bufferSize);
        if (randomBoolean()) {
            test.readNBytes(readLen4);
        } else {
            skipNBytes(test, readLen4);
        }
        bytesReadAfter = bytesRead.get();
        // assert byte is "read" and not returned from the buffer
        assertThat(bytesReadAfter - bytesReadBefore, Matchers.is(readLen4));
        // assert mark reset
        assertThat(test.getCurrentBufferCount(), Matchers.is(0));
        assertThat(test.getRemainingBufferCapacity(), Matchers.is(bufferSize));
        assertThat(test.getRemainingBufferToRead(), Matchers.is(0));
        assertThat(test.markCalled, Matchers.is(false));
        // assert reset does not work any more
        IOException e = expectThrows(IOException.class, () -> {
            test.reset();
        });
        assertThat(e.getMessage(), Matchers.is("Mark not called or has been invalidated"));
    }

    public void testMarkAfterResetWhileReplayingBuffer() throws Exception {
        int bufferSize = 8 + Randomness.get().nextInt(8);
        Tuple<AtomicInteger, InputStream> mockSourceTuple = getMockInfiniteInputStream();
        AtomicInteger bytesRead = mockSourceTuple.v1();
        BufferOnMarkInputStream test = new BufferOnMarkInputStream(mockSourceTuple.v2(), bufferSize);
        // maybe read some bytes
        test.readNBytes(randomFrom(0, Randomness.get().nextInt(32)));
        // mark
        test.mark(1 + Randomness.get().nextInt(bufferSize));
        // read less than bufferSize bytes
        int bytesReadBefore = bytesRead.get();
        int readLen = 1 + Randomness.get().nextInt(bufferSize);
        if (randomBoolean()) {
            test.readNBytes(readLen);
        } else {
            skipNBytes(test, readLen);
        }
        int bytesReadAfter = bytesRead.get();
        // assert bytes are "read" and not returned from the buffer
        assertThat(bytesReadAfter - bytesReadBefore, Matchers.is(readLen));
        // assert buffer is populated
        assertThat(test.getCurrentBufferCount(), Matchers.is(readLen));
        assertThat(test.getRemainingBufferCapacity(), Matchers.is(bufferSize - readLen));
        assertThat(test.getRemainingBufferToRead(), Matchers.is(readLen));
        assertThat(test.markCalled, Matchers.is(true));
        assertThat(test.resetCalled, Matchers.is(false));
        // reset
        test.reset();
        assertThat(test.resetCalled, Matchers.is(true));
        assertThat(test.getCurrentBufferCount(), Matchers.is(readLen));
        assertThat(test.getRemainingBufferCapacity(), Matchers.is(bufferSize - readLen));
        assertThat(test.getRemainingBufferToRead(), Matchers.is(readLen));
        for (int readLen2 = 1; readLen2 <= readLen; readLen2++) {
            Tuple<AtomicInteger, InputStream> mockSourceTuple2 = getMockInfiniteInputStream();
            BufferOnMarkInputStream cloneTest = new BufferOnMarkInputStream(mockSourceTuple.v2(), bufferSize);
            cloneBufferOnMarkStream(cloneTest, test);
            AtomicInteger bytesRead2 = mockSourceTuple2.v1();
            // read again, from buffer this time, less than before
            bytesReadBefore = bytesRead2.get();
            if (randomBoolean()) {
                cloneTest.readNBytes(readLen2);
            } else {
                skipNBytes(cloneTest, readLen2);
            }
            bytesReadAfter = bytesRead2.get();
            // assert bytes are replayed from the buffer, and not read from the stream
            assertThat(bytesReadAfter - bytesReadBefore, Matchers.is(0));
            // assert buffer is consumed
            assertThat(cloneTest.getCurrentBufferCount(), Matchers.is(readLen));
            assertThat(cloneTest.getRemainingBufferCapacity(), Matchers.is(bufferSize - readLen));
            assertThat(cloneTest.getRemainingBufferToRead(), Matchers.is(readLen - readLen2));
            assertThat(cloneTest.markCalled, Matchers.is(true));
            assertThat(cloneTest.resetCalled, Matchers.is(true));
            // mark
            cloneTest.mark(1 + Randomness.get().nextInt(bufferSize));
            assertThat(cloneTest.getCurrentBufferCount(), Matchers.is(readLen - readLen2));
            assertThat(cloneTest.getRemainingBufferCapacity(), Matchers.is(bufferSize - readLen + readLen2));
            assertThat(cloneTest.getRemainingBufferToRead(), Matchers.is(readLen - readLen2));
            assertThat(cloneTest.markCalled, Matchers.is(true));
            assertThat(cloneTest.resetCalled, Matchers.is(true));
            // read until the buffer is filled
            for (int readLen3 = 1; readLen3 <= readLen - readLen2; readLen3++) {
                Tuple<AtomicInteger, InputStream> mockSourceTuple3 = getMockInfiniteInputStream();
                BufferOnMarkInputStream cloneTest3 = new BufferOnMarkInputStream(mockSourceTuple3.v2(), bufferSize);
                cloneBufferOnMarkStream(cloneTest3, cloneTest);
                AtomicInteger bytesRead3 = mockSourceTuple3.v1();
                // read again from buffer, after the mark inside the buffer
                bytesReadBefore = bytesRead3.get();
                if (randomBoolean()) {
                    cloneTest3.readNBytes(readLen3);
                } else {
                    skipNBytes(cloneTest3, readLen3);
                }
                bytesReadAfter = bytesRead3.get();
                // assert bytes are replayed from the buffer, and not read from the stream
                assertThat(bytesReadAfter - bytesReadBefore, Matchers.is(0));
                // assert buffer is consumed completely
                assertThat(cloneTest3.getCurrentBufferCount(), Matchers.is(readLen - readLen2));
                assertThat(cloneTest3.getRemainingBufferCapacity(), Matchers.is(bufferSize - readLen + readLen2));
                assertThat(cloneTest3.getRemainingBufferToRead(), Matchers.is(readLen - readLen2 - readLen3));
                assertThat(cloneTest3.markCalled, Matchers.is(true));
                assertThat(cloneTest3.resetCalled, Matchers.is(true));
            }
            // read beyond the buffer can supply, but not more than it can accommodate
            for (int readLen3 = readLen - readLen2 + 1; readLen3 <= bufferSize - readLen2; readLen3++) {
                Tuple<AtomicInteger, InputStream> mockSourceTuple3 = getMockInfiniteInputStream();
                BufferOnMarkInputStream cloneTest3 = new BufferOnMarkInputStream(mockSourceTuple3.v2(), bufferSize);
                cloneBufferOnMarkStream(cloneTest3, cloneTest);
                AtomicInteger bytesRead3 = mockSourceTuple3.v1();
                // read again from buffer, after the mark inside the buffer
                bytesReadBefore = bytesRead3.get();
                if (randomBoolean()) {
                    cloneTest3.readNBytes(readLen3);
                } else {
                    skipNBytes(cloneTest3, readLen3);
                }
                bytesReadAfter = bytesRead3.get();
                // assert bytes are PARTLY replayed, PARTLY read from the stream
                assertThat(bytesReadAfter - bytesReadBefore, Matchers.is(readLen3 + readLen2 - readLen));
                // assert buffer is appended and fully replayed
                assertThat(cloneTest3.getCurrentBufferCount(), Matchers.is(readLen3));
                assertThat(cloneTest3.getRemainingBufferCapacity(), Matchers.is(bufferSize - readLen3));
                assertThat(cloneTest3.markCalled, Matchers.is(true));
                assertThat(cloneTest3.resetCalled, Matchers.is(false));
            }
        }
    }

    public void testMarkAfterResetAfterReplayingBuffer() throws Exception {
        int bufferSize = 8 + Randomness.get().nextInt(8);
        Tuple<AtomicInteger, InputStream> mockSourceTuple = getMockInfiniteInputStream();
        AtomicInteger bytesRead = mockSourceTuple.v1();
        BufferOnMarkInputStream test = new BufferOnMarkInputStream(mockSourceTuple.v2(), bufferSize);
        // maybe read some bytes
        test.readNBytes(randomFrom(0, Randomness.get().nextInt(32)));
        // mark
        test.mark(1 + Randomness.get().nextInt(bufferSize));
        // read less than bufferSize bytes
        int bytesReadBefore = bytesRead.get();
        int readLen = 1 + Randomness.get().nextInt(bufferSize);
        if (randomBoolean()) {
            test.readNBytes(readLen);
        } else {
            skipNBytes(test, readLen);
        }
        int bytesReadAfter = bytesRead.get();
        // assert bytes are "read" and not returned from the buffer
        assertThat(bytesReadAfter - bytesReadBefore, Matchers.is(readLen));
        // assert buffer is populated
        assertThat(test.getCurrentBufferCount(), Matchers.is(readLen));
        assertThat(test.getRemainingBufferCapacity(), Matchers.is(bufferSize - readLen));
        assertThat(test.getRemainingBufferToRead(), Matchers.is(readLen));
        assertThat(test.markCalled, Matchers.is(true));
        // reset
        test.reset();
        assertThat(test.getCurrentBufferCount(), Matchers.is(readLen));
        assertThat(test.getRemainingBufferCapacity(), Matchers.is(bufferSize - readLen));
        assertThat(test.getRemainingBufferToRead(), Matchers.is(readLen));
        assertThat(test.markCalled, Matchers.is(true));
        assertThat(test.resetCalled, Matchers.is(true));
        for (int readLen2 = readLen + 1; readLen2 <= bufferSize; readLen2++) {
            Tuple<AtomicInteger, InputStream> mockSourceTuple2 = getMockInfiniteInputStream();
            BufferOnMarkInputStream test2 = new BufferOnMarkInputStream(mockSourceTuple2.v2(), bufferSize);
            cloneBufferOnMarkStream(test2, test);
            AtomicInteger bytesRead2 = mockSourceTuple2.v1();
            // read again, more than before
            bytesReadBefore = bytesRead2.get();
            byte[] read2 = test2.readNBytes(readLen2);
            bytesReadAfter = bytesRead2.get();
            // assert bytes are PARTLY replayed, PARTLY read from the stream
            assertThat(bytesReadAfter - bytesReadBefore, Matchers.is(read2.length - readLen));
            // assert buffer is appended and fully replayed
            assertThat(test2.getCurrentBufferCount(), Matchers.is(read2.length));
            assertThat(test2.getRemainingBufferCapacity(), Matchers.is(bufferSize - read2.length));
            assertThat(test2.markCalled, Matchers.is(true));
            assertThat(test2.resetCalled, Matchers.is(false));
            // mark
            test2.mark(1 + Randomness.get().nextInt(bufferSize));
            assertThat(test2.getCurrentBufferCount(), Matchers.is(0));
            assertThat(test2.getRemainingBufferCapacity(), Matchers.is(bufferSize));
            assertThat(test2.getRemainingBufferToRead(), Matchers.is(0));
            assertThat(test2.markCalled, Matchers.is(true));
            assertThat(test2.resetCalled, Matchers.is(false));
        }
    }

    public void testNoMockSimpleMarkResetAtBeginning() throws Exception {
        for (int length = 1; length <= 8; length++) {
            for (int mark = 1; mark <= length; mark++) {
                try (BufferOnMarkInputStream in = new BufferOnMarkInputStream(new NoMarkByteArrayInputStream(testArray, 0, length), mark)) {
                    in.mark(mark);
                    assertThat(in.getCurrentBufferCount(), Matchers.is(0));
                    assertThat(in.getRemainingBufferCapacity(), Matchers.is(mark));
                    byte[] test1 = in.readNBytes(mark);
                    assertArray(0, test1);
                    assertThat(in.getCurrentBufferCount(), Matchers.is(mark));
                    assertThat(in.getRemainingBufferCapacity(), Matchers.is(0));
                    in.reset();
                    assertThat(in.getCurrentBufferCount(), Matchers.is(mark));
                    assertThat(in.getRemainingBufferCapacity(), Matchers.is(0));
                    byte[] test2 = in.readNBytes(mark);
                    assertArray(0, test2);
                    assertThat(in.getCurrentBufferCount(), Matchers.is(mark));
                    assertThat(in.getRemainingBufferCapacity(), Matchers.is(0));
                }
            }
        }
    }

    public void testNoMockMarkResetAtBeginning() throws Exception {
        for (int length = 1; length <= 8; length++) {
            try (BufferOnMarkInputStream in = new BufferOnMarkInputStream(new NoMarkByteArrayInputStream(testArray, 0, length), length)) {
                in.mark(length);
                // increasing length read/reset
                for (int readLen = 1; readLen <= length; readLen++) {
                    byte[] test1 = in.readNBytes(readLen);
                    assertArray(0, test1);
                    in.reset();
                }
            }
            try (BufferOnMarkInputStream in = new BufferOnMarkInputStream(new NoMarkByteArrayInputStream(testArray, 0, length), length)) {
                in.mark(length);
                // decreasing length read/reset
                for (int readLen = length; readLen >= 1; readLen--) {
                    byte[] test1 = in.readNBytes(readLen);
                    assertArray(0, test1);
                    in.reset();
                }
            }
        }
    }

    public void testNoMockSimpleMarkResetEverywhere() throws Exception {
        for (int length = 1; length <= 10; length++) {
            for (int offset = 0; offset < length; offset++) {
                for (int mark = 1; mark <= length - offset; mark++) {
                    try (BufferOnMarkInputStream in = new BufferOnMarkInputStream(new
                            NoMarkByteArrayInputStream(testArray, 0, length), mark)) {
                        // skip first offset bytes
                        in.readNBytes(offset);
                        in.mark(mark);
                        assertThat(in.getCurrentBufferCount(), Matchers.is(0));
                        assertThat(in.getRemainingBufferCapacity(), Matchers.is(mark));
                        byte[] test1 = in.readNBytes(mark);
                        assertArray(offset, test1);
                        assertThat(in.getCurrentBufferCount(), Matchers.is(mark));
                        assertThat(in.getRemainingBufferCapacity(), Matchers.is(0));
                        in.reset();
                        assertThat(in.getCurrentBufferCount(), Matchers.is(mark));
                        assertThat(in.getRemainingBufferCapacity(), Matchers.is(0));
                        byte[] test2 = in.readNBytes(mark);
                        assertThat(in.getCurrentBufferCount(), Matchers.is(mark));
                        assertThat(in.getRemainingBufferCapacity(), Matchers.is(0));
                        assertArray(offset, test2);
                    }
                }
            }
        }
    }

    public void testNoMockMarkResetEverywhere() throws Exception {
        for (int length = 1; length <= 8; length++) {
            for (int offset = 0; offset < length; offset++) {
                try (BufferOnMarkInputStream in = new BufferOnMarkInputStream(new NoMarkByteArrayInputStream(testArray, 0, length),
                        length)) {
                    // skip first offset bytes
                    in.readNBytes(offset);
                    in.mark(length);
                    // increasing read lengths
                    for (int readLen = 1; readLen <= length - offset; readLen++) {
                        byte[] test = in.readNBytes(readLen);
                        assertArray(offset, test);
                        in.reset();
                    }
                }
                try (BufferOnMarkInputStream in = new BufferOnMarkInputStream(new NoMarkByteArrayInputStream(testArray, 0, length),
                        length)) {
                    // skip first offset bytes
                    in.readNBytes(offset);
                    in.mark(length);
                    // decreasing read lengths
                    for (int readLen = length - offset; readLen >= 1; readLen--) {
                        byte[] test = in.readNBytes(readLen);
                        assertArray(offset, test);
                        in.reset();
                    }
                }
            }
        }
    }

    public void testNoMockDoubleMarkEverywhere() throws Exception {
        for (int length = 1; length <= 16; length++) {
            for (int offset = 0; offset < length; offset++) {
                for (int readLen = 1; readLen <= length - offset; readLen++) {
                    for (int markLen = 1; markLen <= length - offset; markLen++) {
                        try (BufferOnMarkInputStream in = new BufferOnMarkInputStream(new NoMarkByteArrayInputStream(testArray, 0, length),
                                length)) {
                            in.readNBytes(offset);
                            assertThat(in.getCurrentBufferCount(), Matchers.is(0));
                            assertThat(in.getRemainingBufferCapacity(), Matchers.is(length));
                            assertThat(in.getRemainingBufferToRead(), Matchers.is(0));
                            // first mark
                            in.mark(length - offset);
                            assertThat(in.getCurrentBufferCount(), Matchers.is(0));
                            assertThat(in.getRemainingBufferCapacity(), Matchers.is(length));
                            assertThat(in.getRemainingBufferToRead(), Matchers.is(0));
                            byte[] test = in.readNBytes(readLen);
                            assertArray(offset, test);
                            assertThat(in.getCurrentBufferCount(), Matchers.is(readLen));
                            assertThat(in.getRemainingBufferCapacity(), Matchers.is(length - readLen));
                            assertThat(in.getRemainingBufferToRead(), Matchers.is(readLen));
                            // reset to first
                            in.reset();
                            assertThat(in.getCurrentBufferCount(), Matchers.is(readLen));
                            assertThat(in.getRemainingBufferCapacity(), Matchers.is(length - readLen));
                            assertThat(in.getRemainingBufferToRead(), Matchers.is(readLen));
                            // advance before/after the first read length
                            test = in.readNBytes(markLen);
                            assertThat(in.getCurrentBufferCount(), Matchers.is(Math.max(readLen, markLen)));
                            assertThat(in.getRemainingBufferCapacity(), Matchers.is(length - Math.max(readLen, markLen)));
                            if (markLen <= readLen) {
                                assertThat(in.getRemainingBufferToRead(), Matchers.is(readLen - markLen));
                            } else {
                                assertThat(in.resetCalled, Matchers.is(false));
                            }
                            assertArray(offset, test);
                            // second mark
                            in.mark(length - offset - markLen);
                            if (markLen <= readLen) {
                                assertThat(in.getCurrentBufferCount(), Matchers.is(readLen - markLen));
                                assertThat(in.getRemainingBufferCapacity(), Matchers.is(length - readLen + markLen));
                                assertThat(in.getRemainingBufferToRead(), Matchers.is(readLen - markLen));
                            } else {
                                assertThat(in.getCurrentBufferCount(), Matchers.is(0));
                                assertThat(in.getRemainingBufferCapacity(), Matchers.is(length));
                                assertThat(in.getRemainingBufferToRead(), Matchers.is(0));
                            }
                            for (int readLen2 = 1; readLen2 <= length - offset - markLen; readLen2++) {
                                byte[] test2 = in.readNBytes(readLen2);
                                if (markLen + readLen2 <= readLen) {
                                    assertThat(in.getCurrentBufferCount(), Matchers.is(readLen - markLen));
                                    assertThat(in.getRemainingBufferCapacity(), Matchers.is(length - readLen + markLen));
                                    assertThat(in.resetCalled, Matchers.is(true));
                                    assertThat(in.getRemainingBufferToRead(), Matchers.is(readLen - markLen - readLen2));
                                } else {
                                    assertThat(in.getCurrentBufferCount(), Matchers.is(readLen2));
                                    assertThat(in.getRemainingBufferCapacity(), Matchers.is(length - readLen2));
                                    assertThat(in.resetCalled, Matchers.is(false));
                                }
                                assertArray(offset + markLen, test2);
                                in.reset();
                                assertThat(in.resetCalled, Matchers.is(true));
                                if (markLen + readLen2 <= readLen) {
                                    assertThat(in.getCurrentBufferCount(), Matchers.is(readLen - markLen));
                                    assertThat(in.getRemainingBufferCapacity(), Matchers.is(length - readLen + markLen));
                                    assertThat(in.getRemainingBufferToRead(), Matchers.is(readLen - markLen));
                                } else {
                                    assertThat(in.getCurrentBufferCount(), Matchers.is(readLen2));
                                    assertThat(in.getRemainingBufferCapacity(), Matchers.is(length - readLen2));
                                    assertThat(in.getRemainingBufferToRead(), Matchers.is(readLen2));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public void testNoMockMarkWithoutReset() throws Exception {
        int maxMark = 8;
        BufferOnMarkInputStream in = new BufferOnMarkInputStream(new NoMarkByteArrayInputStream(testArray, 0, testArray.length), maxMark);
        int offset = 0;
        while (offset < testArray.length) {
            int readLen = Math.min(1 + Randomness.get().nextInt(maxMark), testArray.length - offset);
            in.mark(Randomness.get().nextInt(readLen));
            assertThat(in.getCurrentBufferCount(), Matchers.is(0));
            assertThat(in.getRemainingBufferCapacity(), Matchers.is(maxMark));
            assertThat(in.getRemainingBufferToRead(), Matchers.is(0));
            byte[] test = in.readNBytes(readLen);
            assertThat(in.getCurrentBufferCount(), Matchers.is(readLen));
            assertThat(in.getRemainingBufferCapacity(), Matchers.is(maxMark - readLen));
            assertThat(in.getRemainingBufferToRead(), Matchers.is(readLen));
            assertArray(offset, test);
            offset += readLen;
        }
    }

    public void testNoMockThreeMarkResetMarkSteps() throws Exception {
        int length = 8 + Randomness.get().nextInt(8);
        int stepLen = 4 + Randomness.get().nextInt(4);
        BufferOnMarkInputStream in = new BufferOnMarkInputStream(new NoMarkByteArrayInputStream(testArray, 0, length), stepLen);
        testMarkResetMarkStep(in, 0, length, stepLen, 2);
    }

    private void testMarkResetMarkStep(BufferOnMarkInputStream stream, int offset, int length, int stepLen, int step) throws Exception {
        stream.mark(stepLen);
        for (int readLen = 1; readLen <= Math.min(stepLen, length - offset); readLen++) {
            for (int markLen = 1; markLen <= Math.min(stepLen, length - offset); markLen++) {
                BufferOnMarkInputStream cloneStream = cloneBufferOnMarkStream(stream) ;
                // read ahead
                byte[] test = cloneStream.readNBytes(readLen);
                assertArray(offset, test);
                // reset back
                cloneStream.reset();
                // read ahead different length
                test = cloneStream.readNBytes(markLen);
                assertArray(offset, test);
                if (step > 0) {
                    testMarkResetMarkStep(cloneStream, offset + markLen, length, stepLen, step - 1);
                }
            }
        }
    }

    private BufferOnMarkInputStream cloneBufferOnMarkStream(BufferOnMarkInputStream orig) {
        int origOffset = ((NoMarkByteArrayInputStream) orig.getWrapped()).getPos();
        int origLen = ((NoMarkByteArrayInputStream) orig.getWrapped()).getCount();
        BufferOnMarkInputStream cloneStream = new BufferOnMarkInputStream(new NoMarkByteArrayInputStream(testArray,
                origOffset, origLen - origOffset), orig.bufferSize);
        if (orig.ringBuffer != null) {
            cloneStream.ringBuffer = Arrays.copyOf(orig.ringBuffer, orig.ringBuffer.length);
        } else {
            cloneStream.ringBuffer = null;
        }
        cloneStream.head = orig.head;
        cloneStream.tail = orig.tail;
        cloneStream.position = orig.position;
        cloneStream.markCalled = orig.markCalled;
        cloneStream.resetCalled = orig.resetCalled;
        cloneStream.closed = orig.closed;
        return cloneStream;
    }

    private void cloneBufferOnMarkStream(BufferOnMarkInputStream clone, BufferOnMarkInputStream orig) {
        if (orig.ringBuffer != null) {
            clone.ringBuffer = Arrays.copyOf(orig.ringBuffer, orig.ringBuffer.length);
        } else {
            clone.ringBuffer = null;
        }
        clone.head = orig.head;
        clone.tail = orig.tail;
        clone.position = orig.position;
        clone.markCalled = orig.markCalled;
        clone.resetCalled = orig.resetCalled;
        clone.closed = orig.closed;
    }

    private void assertArray(int offset, byte[] test) {
        for (int i = 0; i < test.length; i++) {
            Assert.assertThat(test[i], Matchers.is(testArray[offset + i]));
        }
    }

    private Tuple<AtomicInteger, InputStream> getMockInfiniteInputStream() throws IOException {
        InputStream mockSource = mock(InputStream.class);
        AtomicInteger bytesRead = new AtomicInteger(0);
        when(mockSource.read(org.mockito.Matchers.<byte[]>any(), org.mockito.Matchers.anyInt(), org.mockito.Matchers.anyInt())).
                thenAnswer(invocationOnMock -> {
                    final byte[] b = (byte[]) invocationOnMock.getArguments()[0];
                    final int off = (int) invocationOnMock.getArguments()[1];
                    final int len = (int) invocationOnMock.getArguments()[2];
                    if (len == 0) {
                        return 0;
                    } else {
                        int bytesCount = 1 + Randomness.get().nextInt(len);
                        bytesRead.addAndGet(bytesCount);
                        return bytesCount;
                    }
                });
        return new Tuple<>(bytesRead, mockSource);
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

    static class NoMarkByteArrayInputStream extends ByteArrayInputStream {

        NoMarkByteArrayInputStream(byte[] buf) {
            super(buf);
        }

        NoMarkByteArrayInputStream(byte[] buf, int offset, int length) {
            super(buf, offset, length);
        }

        int getPos() {
            return pos;
        }

        int getCount() {
            return count;
        }

        @Override
        public void mark(int readlimit) {
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public void reset() {
            throw new IllegalStateException("Mark not called or has been invalidated");
        }
    }

}
