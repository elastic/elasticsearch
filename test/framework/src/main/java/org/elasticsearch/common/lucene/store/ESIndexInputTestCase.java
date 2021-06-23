/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.lucene.store;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test harness for verifying {@link IndexInput} implementations.
 */
public class ESIndexInputTestCase extends ESTestCase {

    private static EsThreadPoolExecutor executor;

    private AtomicLong uniqueIdGenerator;

    @BeforeClass
    public static void createExecutor() {
        final String name = "TEST-" + getTestClass().getSimpleName() + "#randomReadAndSlice";
        executor = EsExecutors.newFixed(name, 10, 0, EsExecutors.daemonThreadFactory(name), new ThreadContext(Settings.EMPTY), false);
    }

    @AfterClass
    public static void destroyExecutor() {
        executor.shutdown();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        uniqueIdGenerator = new AtomicLong();
    }

    /**
     * Reads the contents of an {@link IndexInput} from {@code indexInput.getFilePointer()} to {@code length} using a wide variety of
     * different access methods. Returns an array of length {@code length} containing the bytes that were read starting at index
     * {@code indexInput.getFilePointer()}. The bytes of the returned array with indices below the initial value of
     * {@code indexInput.getFilePointer()} may contain anything. The final value of {@code indexInput.getFilePointer()} is {@code length}.
     */
    protected byte[] randomReadAndSlice(IndexInput indexInput, int length) throws IOException {
        int readPos = (int) indexInput.getFilePointer();
        byte[] output = new byte[length];
        while (readPos < length) {
            switch (randomIntBetween(0, 5)) {
                case 0:
                    // Read by one byte at a time
                    output[readPos++] = indexInput.readByte();
                    break;
                case 1:
                    // Read several bytes into target
                    int len = randomIntBetween(1, length - readPos);
                    indexInput.readBytes(output, readPos, len);
                    readPos += len;
                    break;
                case 2:
                    // Read several bytes into 0-offset target
                    len = randomIntBetween(1, length - readPos);
                    byte[] temp = new byte[len];
                    indexInput.readBytes(temp, 0, len);
                    System.arraycopy(temp, 0, output, readPos, len);
                    readPos += len;
                    break;
                case 3:
                    // Read using slice
                    len = randomIntBetween(1, length - readPos);
                    final String sliceExtension = randomValueOtherThan(".cfs", ESIndexInputTestCase::randomFileExtension);
                    IndexInput slice = indexInput.slice(randomUniqueSliceName() + sliceExtension, readPos, len);
                    temp = randomReadAndSlice(slice, len);
                    // assert that position in the original input didn't change
                    assertEquals(readPos, indexInput.getFilePointer());
                    System.arraycopy(temp, 0, output, readPos, len);
                    readPos += len;
                    indexInput.seek(readPos);
                    assertEquals(readPos, indexInput.getFilePointer());
                    break;
                case 4:
                    // Seek at a random position and read a single byte,
                    // then seek back to original position
                    final int lastReadPos = readPos;
                    readPos = randomIntBetween(0, length - 1);
                    indexInput.seek(readPos);
                    assertEquals(readPos, indexInput.getFilePointer());
                    final int bytesToRead = 1;
                    temp = randomReadAndSlice(indexInput, readPos + bytesToRead);
                    System.arraycopy(temp, readPos, output, readPos, bytesToRead);
                    readPos = lastReadPos;
                    indexInput.seek(readPos);
                    assertEquals(readPos, indexInput.getFilePointer());
                    break;
                case 5:
                    // Read clone or slice concurrently
                    final int cloneCount = between(1, 3);
                    final CountDownLatch startLatch = new CountDownLatch(1 + cloneCount);
                    final CountDownLatch finishLatch = new CountDownLatch(cloneCount);

                    final PlainActionFuture<byte[]> mainThreadResultFuture = new PlainActionFuture<>();
                    final int mainThreadReadStart = readPos;
                    final int mainThreadReadEnd = randomIntBetween(readPos + 1, length);

                    for (int i = 0; i < cloneCount; i++) {
                        executor.execute(new AbstractRunnable() {
                            @Override
                            public void onFailure(Exception e) {
                                throw new AssertionError(e);
                            }

                            @Override
                            protected void doRun() throws Exception {
                                final IndexInput clone;
                                final int readStart = between(0, length);
                                final int readEnd = between(readStart, length);
                                if (randomBoolean()) {
                                    clone = indexInput.clone();
                                } else {
                                    final int sliceEnd = between(readEnd, length);
                                    final String sliceExtension = randomValueOtherThan(".cfs", ESIndexInputTestCase::randomFileExtension);
                                    clone = indexInput.slice("slice" + randomUniqueSliceName() + sliceExtension, 0L, sliceEnd);
                                }
                                startLatch.countDown();
                                startLatch.await();
                                clone.seek(readStart);
                                final byte[] cloneResult = randomReadAndSlice(clone, readEnd);
                                if (randomBoolean()) {
                                    clone.close();
                                }

                                // the read from the clone should agree with the read from the main input on their overlap
                                final int maxStart = Math.max(mainThreadReadStart, readStart);
                                final int minEnd = Math.min(mainThreadReadEnd, readEnd);
                                if (maxStart < minEnd) {
                                    final byte[] mainThreadResult = mainThreadResultFuture.actionGet();
                                    final int overlapLen = minEnd - maxStart;
                                    final byte[] fromMainThread = new byte[overlapLen];
                                    final byte[] fromClone = new byte[overlapLen];
                                    System.arraycopy(mainThreadResult, maxStart, fromMainThread, 0, overlapLen);
                                    System.arraycopy(cloneResult, maxStart, fromClone, 0, overlapLen);
                                    assertArrayEquals(fromMainThread, fromClone);
                                }
                            }

                            @Override
                            public void onAfter() {
                                finishLatch.countDown();
                            }

                            @Override
                            public void onRejection(Exception e) {
                                // all threads are busy, and queueing can lead this test to deadlock, so we need take no action
                                startLatch.countDown();
                            }
                        });
                    }

                    try {
                        startLatch.countDown();
                        startLatch.await();
                        ActionListener.completeWith(mainThreadResultFuture, () -> randomReadAndSlice(indexInput, mainThreadReadEnd));
                        System.arraycopy(mainThreadResultFuture.actionGet(), readPos, output, readPos, mainThreadReadEnd - readPos);
                        readPos = mainThreadReadEnd;
                        finishLatch.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    break;
                default:
                    fail();
            }
            assertEquals(readPos, indexInput.getFilePointer());
        }
        assertEquals(length, indexInput.getFilePointer());
        return output;
    }

    // generate unique slice names for slicing index inputs because using the same slice name together with different slice sizes
    // is not supported for .cfs files and thus we have to avoid slice name collisions
    private String randomUniqueSliceName() {
        return randomAlphaOfLength(10) + uniqueIdGenerator.incrementAndGet();
    }

    protected static String randomFileExtension() {
        return '.' + randomFrom(LuceneFilesExtensions.values()).getExtension();
    }
}
