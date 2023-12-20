/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.codec.postings;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.util.Arrays;

public class ForUtilTests extends LuceneTestCase {

    public void testEncodeDecode() throws IOException {
        final int iterations = RandomNumbers.randomIntBetween(random(), 50, 1000);
        final int[] values = new int[iterations * ForUtil.BLOCK_SIZE];

        for (int i = 0; i < iterations; ++i) {
            final int bpv = TestUtil.nextInt(random(), 1, 31);
            for (int j = 0; j < ForUtil.BLOCK_SIZE; ++j) {
                values[i * ForUtil.BLOCK_SIZE + j] = RandomNumbers.randomIntBetween(random(), 0, (int) PackedInts.maxValue(bpv));
            }
        }

        final Directory d = new ByteBuffersDirectory();
        final long endPointer;

        {
            // encode
            IndexOutput out = d.createOutput("test.bin", IOContext.DEFAULT);
            final ForUtil forUtil = new ForUtil();

            for (int i = 0; i < iterations; ++i) {
                long[] source = new long[ForUtil.BLOCK_SIZE];
                long or = 0;
                for (int j = 0; j < ForUtil.BLOCK_SIZE; ++j) {
                    source[j] = values[i * ForUtil.BLOCK_SIZE + j];
                    or |= source[j];
                }
                final int bpv = PackedInts.bitsRequired(or);
                out.writeByte((byte) bpv);
                forUtil.encode(source, bpv, out);
            }
            endPointer = out.getFilePointer();
            out.close();
        }

        {
            // decode
            IndexInput in = d.openInput("test.bin", IOContext.READONCE);
            final ForUtil forUtil = new ForUtil();
            for (int i = 0; i < iterations; ++i) {
                final int bitsPerValue = in.readByte();
                final long currentFilePointer = in.getFilePointer();
                final long[] restored = new long[ForUtil.BLOCK_SIZE];
                forUtil.decode(bitsPerValue, in, restored);
                int[] ints = new int[ForUtil.BLOCK_SIZE];
                for (int j = 0; j < ForUtil.BLOCK_SIZE; ++j) {
                    ints[j] = Math.toIntExact(restored[j]);
                }
                assertArrayEquals(
                    Arrays.toString(ints),
                    ArrayUtil.copyOfSubArray(values, i * ForUtil.BLOCK_SIZE, (i + 1) * ForUtil.BLOCK_SIZE),
                    ints
                );
                assertEquals(forUtil.numBytes(bitsPerValue), in.getFilePointer() - currentFilePointer);
            }
            assertEquals(endPointer, in.getFilePointer());
            in.close();
        }

        d.close();
    }
}
