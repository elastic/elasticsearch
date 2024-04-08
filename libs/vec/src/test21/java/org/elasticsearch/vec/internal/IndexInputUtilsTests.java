/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.vec.internal;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.stream.IntStream;

import static org.hamcrest.core.IsEqual.equalTo;

public class IndexInputUtilsTests extends ESTestCase {

    public void testSingleSegment() throws IOException {
        try (Directory dir = new MMapDirectory(createTempDir(getTestName()))) {
            for (int times = 0; times < TIMES; times++) {
                String fileName = getTestName() + times;
                int size = randomIntBetween(10, 127);
                try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                    byte[] ba = new byte[size];
                    IntStream.range(0, size).forEach(i -> ba[i] = (byte) i);
                    out.writeBytes(ba, 0, ba.length);
                }
                try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                    var input = IndexInputUtils.unwrapAndCheckInputOrNull(in);
                    assertNotNull(input);
                    {
                        var segArray = IndexInputUtils.segmentArray(input);
                        assertThat(segArray.length, equalTo(1));
                        assertThat(segArray[0].byteSize(), equalTo((long) size));

                        // Out of Bounds - cannot retrieve the segment
                        assertNull(IndexInputUtils.segmentSlice(input, 0, size + 1));
                        assertNull(IndexInputUtils.segmentSlice(input, size - 1, 2));

                        var fullSeg = IndexInputUtils.segmentSlice(input, 0, size);
                        assertNotNull(fullSeg);
                        for (int i = 0; i < size; i++) {
                            assertThat(fullSeg.get(ValueLayout.JAVA_BYTE, i), equalTo((byte) i));
                        }

                        var partialSeg = IndexInputUtils.segmentSlice(input, 1, size - 1);
                        assertNotNull(partialSeg);
                        for (int i = 0; i < size - 2; i++) {
                            assertThat(partialSeg.get(ValueLayout.JAVA_BYTE, i), equalTo((byte) (i + 1)));
                        }
                    }
                    // IndexInput::slice
                    {
                        var slice = input.slice("partial slice", 1, size - 2);
                        var sliceSgArray = IndexInputUtils.segmentArray(slice);
                        assertThat(sliceSgArray.length, equalTo(1));
                        assertThat(sliceSgArray[0].byteSize(), equalTo((long) size - 2));

                        var fullSeg = IndexInputUtils.segmentSlice(slice, 0, size - 2);
                        assertNotNull(fullSeg);
                        for (int i = 0; i < size - 2; i++) {
                            assertThat(fullSeg.get(ValueLayout.JAVA_BYTE, i), equalTo((byte) (i + 1)));
                        }
                    }
                }
            }
        }
    }

    public void testMultiSegment() throws IOException {
        try (Directory dir = new MMapDirectory(createTempDir(getTestName()), 32L)) {
            for (int times = 0; times < TIMES; times++) {
                String fileName = getTestName() + times;
                int size = randomIntBetween(65, 250);
                int expectedNumSegs = size / 32 + 1;
                try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                    byte[] ba = new byte[size];
                    out.writeBytes(ba, 0, ba.length);
                }
                try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                    var input = IndexInputUtils.unwrapAndCheckInputOrNull(in);
                    assertNotNull(input);
                    var segArray = IndexInputUtils.segmentArray(input);
                    assertThat(segArray.length, equalTo(expectedNumSegs));
                    assertThat(Arrays.stream(segArray).mapToLong(MemorySegment::byteSize).sum(), equalTo((long) size));
                    assertThat(IndexInputUtils.offset(input), equalTo(0L));

                    var slice = input.slice("partial slice", 1, size - 1);
                    assertThat(IndexInputUtils.offset(slice), equalTo(1L));

                    var slice2 = input.slice("full segment slice", 32, size - 32);
                    var segArray2 = IndexInputUtils.segmentArray(slice2);
                    assertThat(Arrays.stream(segArray2).mapToLong(MemorySegment::byteSize).sum(), equalTo((long) size - 32));
                    assertThat(IndexInputUtils.offset(slice2), equalTo(0L));
                }
            }
        }
    }

    static final int TIMES = 100; // a loop iteration times

}
