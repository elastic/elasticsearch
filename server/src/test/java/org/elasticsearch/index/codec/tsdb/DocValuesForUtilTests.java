/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.tsdb;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
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
import java.util.Random;

public class DocValuesForUtilTests extends LuceneTestCase {
    private final ES87TSDBDocValuesEncoder encoder = new ES87TSDBDocValuesEncoder();
    private static final ForUtil forUtil = new ForUtil();

    public void testEncodeDecode() throws IOException {
        final int iterations = RandomNumbers.randomIntBetween(random(), 50, 1000);
        final long[] values = new long[iterations * ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE];
        final int[] bpvs = new int[iterations];

        for (int i = 0; i < iterations; ++i) {
            final int bpv = TestUtil.nextInt(random(), 1, 64);
            bpvs[i] = DocValuesForUtil.roundBits(bpv);
            for (int j = 0; j < ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE; ++j) {
                values[i * ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE + j] = bpv == 64
                    ? random().nextLong()
                    : TestUtil.nextLong(random(), 0, PackedInts.maxValue(bpv));
            }
        }

        final Directory d = new ByteBuffersDirectory();
        final long endPointer;

        {
            // encode
            IndexOutput out = d.createOutput("test.bin", IOContext.DEFAULT);
            final DocValuesForUtil forUtil = new DocValuesForUtil();

            for (int i = 0; i < iterations; ++i) {
                long[] source = new long[ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE];
                for (int j = 0; j < ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE; ++j) {
                    source[j] = values[i * ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE + j];
                }
                out.writeByte((byte) bpvs[i]);
                forUtil.encode(source, bpvs[i], out);
            }
            endPointer = out.getFilePointer();
            out.close();
        }

        {
            // decode
            IndexInput in = d.openInput("test.bin", IOContext.READONCE);
            final DocValuesForUtil forUtil = new DocValuesForUtil();
            final long[] restored = new long[ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE];
            for (int i = 0; i < iterations; ++i) {
                final int bitsPerValue = in.readByte();
                forUtil.decode(bitsPerValue, in, restored);
                assertArrayEquals(
                    Arrays.toString(restored),
                    ArrayUtil.copyOfSubArray(
                        values,
                        i * ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE,
                        (i + 1) * ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE
                    ),
                    restored
                );
            }
            assertEquals(endPointer, in.getFilePointer());
            in.close();
        }

        d.close();
    }

    public void testEncodeDecodeBitsPerValue() throws IOException {
        int size = 128;
        byte[] dataOutputBuffer = new byte[16 * 1024];
        byte[] dataInputBuffer = new byte[16 * 1024];
        long[] decodeBuffer = new long[size];

        for (int bitsPerValue = 1; bitsPerValue < 32; bitsPerValue++) {
            long[] values = randomValues(bitsPerValue, size);

            // Encode
            DataOutput dataOutput = new ByteArrayDataOutput(dataOutputBuffer);
            long[] encodeBuffer = Arrays.copyOf(values, values.length);
            forUtil.encode(encodeBuffer, bitsPerValue, dataOutput);

            // Prepare for decoding
            DataInput dataInput = new ByteArrayDataInput(dataInputBuffer);
            System.arraycopy(dataOutputBuffer, 0, dataInputBuffer, 0, dataOutputBuffer.length);

            // Decode
            forUtil.decode(bitsPerValue, dataInput, decodeBuffer);

            assertArrayEquals(decodeBuffer, values);
        }
    }

    private long[] randomValues(int bitsPerValue, int size) {
        final Random random = new Random(0);
        long[] values = new long[size];
        long upperBound = 1L << bitsPerValue;
        for (int i = 0; i < size; i++) {
            values[i] = random.nextLong(upperBound);
        }
        return values;
    }
}
