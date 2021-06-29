/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CountingInputStreamTests extends ESTestCase {

    private static byte[] testArray;

    @BeforeClass
    static void createTestArray() throws Exception {
        testArray = new byte[32];
        for (int i = 0; i < testArray.length; i++) {
            testArray[i] = (byte) i;
        }
    }

    public void testWrappedMarkAndClose() throws Exception {
        AtomicBoolean isClosed = new AtomicBoolean(false);
        InputStream mockIn = mock(InputStream.class);
        doAnswer(new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) {
                isClosed.set(true);
                return null;
            }
        }).when(mockIn).close();
        new CountingInputStream(mockIn, true).close();
        assertThat(isClosed.get(), Matchers.is(true));
        isClosed.set(false);
        new CountingInputStream(mockIn, false).close();
        assertThat(isClosed.get(), Matchers.is(false));
        when(mockIn.markSupported()).thenAnswer(invocationOnMock -> { return false; });
        assertThat(new CountingInputStream(mockIn, randomBoolean()).markSupported(), Matchers.is(false));
        when(mockIn.markSupported()).thenAnswer(invocationOnMock -> { return true; });
        assertThat(new CountingInputStream(mockIn, randomBoolean()).markSupported(), Matchers.is(true));
    }

    public void testSimpleCountForRead() throws Exception {
        CountingInputStream test = new CountingInputStream(new ByteArrayInputStream(testArray), randomBoolean());
        assertThat(test.getCount(), Matchers.is(0L));
        int readLen = Randomness.get().nextInt(testArray.length);
        test.readNBytes(readLen);
        assertThat(test.getCount(), Matchers.is((long) readLen));
        readLen = testArray.length - readLen;
        test.readNBytes(readLen);
        assertThat(test.getCount(), Matchers.is((long) testArray.length));
        test.close();
        assertThat(test.getCount(), Matchers.is((long) testArray.length));
    }

    public void testSimpleCountForSkip() throws Exception {
        CountingInputStream test = new CountingInputStream(new ByteArrayInputStream(testArray), randomBoolean());
        assertThat(test.getCount(), Matchers.is(0L));
        int skipLen = Randomness.get().nextInt(testArray.length);
        test.skip(skipLen);
        assertThat(test.getCount(), Matchers.is((long) skipLen));
        skipLen = testArray.length - skipLen;
        test.readNBytes(skipLen);
        assertThat(test.getCount(), Matchers.is((long) testArray.length));
        test.close();
        assertThat(test.getCount(), Matchers.is((long) testArray.length));
    }

    public void testCountingForMarkAndReset() throws Exception {
        CountingInputStream test = new CountingInputStream(new ByteArrayInputStream(testArray), randomBoolean());
        assertThat(test.getCount(), Matchers.is(0L));
        assertThat(test.markSupported(), Matchers.is(true));
        int offset1 = Randomness.get().nextInt(testArray.length - 1);
        if (randomBoolean()) {
            test.skip(offset1);
        } else {
            test.read(new byte[offset1]);
        }
        assertThat(test.getCount(), Matchers.is((long) offset1));
        test.mark(testArray.length);
        int offset2 = 1 + Randomness.get().nextInt(testArray.length - offset1 - 1);
        if (randomBoolean()) {
            test.skip(offset2);
        } else {
            test.read(new byte[offset2]);
        }
        assertThat(test.getCount(), Matchers.is((long) offset1 + offset2));
        test.reset();
        assertThat(test.getCount(), Matchers.is((long) offset1));
        int offset3 = Randomness.get().nextInt(offset2);
        if (randomBoolean()) {
            test.skip(offset3);
        } else {
            test.read(new byte[offset3]);
        }
        assertThat(test.getCount(), Matchers.is((long) offset1 + offset3));
        test.reset();
        assertThat(test.getCount(), Matchers.is((long) offset1));
        test.readAllBytes();
        assertThat(test.getCount(), Matchers.is((long) testArray.length));
        test.close();
        assertThat(test.getCount(), Matchers.is((long) testArray.length));
    }

    public void testCountingForMarkAfterReset() throws Exception {
        CountingInputStream test = new CountingInputStream(new ByteArrayInputStream(testArray), randomBoolean());
        assertThat(test.getCount(), Matchers.is(0L));
        assertThat(test.markSupported(), Matchers.is(true));
        int offset1 = Randomness.get().nextInt(testArray.length - 1);
        if (randomBoolean()) {
            test.skip(offset1);
        } else {
            test.read(new byte[offset1]);
        }
        assertThat(test.getCount(), Matchers.is((long) offset1));
        test.mark(testArray.length);
        int offset2 = 1 + Randomness.get().nextInt(testArray.length - offset1 - 1);
        if (randomBoolean()) {
            test.skip(offset2);
        } else {
            test.read(new byte[offset2]);
        }
        assertThat(test.getCount(), Matchers.is((long) offset1 + offset2));
        test.reset();
        assertThat(test.getCount(), Matchers.is((long) offset1));
        int offset3 = Randomness.get().nextInt(offset2);
        if (randomBoolean()) {
            test.skip(offset3);
        } else {
            test.read(new byte[offset3]);
        }
        test.mark(testArray.length);
        assertThat(test.getCount(), Matchers.is((long) offset1 + offset3));
        int offset4 = Randomness.get().nextInt(testArray.length - offset1 - offset3);
        if (randomBoolean()) {
            test.skip(offset4);
        } else {
            test.read(new byte[offset4]);
        }
        assertThat(test.getCount(), Matchers.is((long) offset1 + offset3 + offset4));
        test.reset();
        assertThat(test.getCount(), Matchers.is((long) offset1 + offset3));
        test.readAllBytes();
        assertThat(test.getCount(), Matchers.is((long) testArray.length));
        test.close();
        assertThat(test.getCount(), Matchers.is((long) testArray.length));
    }

}
