/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.benchmark.common.util;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.UnsafeUtils;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class BytesRefComparisonsBenchmark {

    private static final Random R = new Random(0);
    private static final int ITERS = 100;

    // To avoid JVM optimizations
    @SuppressWarnings("unused")
    private static boolean DUMMY;

    enum Comparator {
        SAFE {
            boolean compare(BytesRef b1, BytesRef b2) {
                return b1.bytesEquals(b2);
            }
        },
        UNSAFE {
            @Override
            boolean compare(BytesRef b1, BytesRef b2) {
                return UnsafeUtils.equals(b1, b2);
            }
        };
        abstract boolean compare(BytesRef b1, BytesRef b2);
    }

    private static BytesRef[] buildBytesRefs(int minLen, int maxLen, int count, int uniqueCount) {
        final BytesRef[] uniqueRefs = new BytesRef[uniqueCount];
        for (int i = 0; i < uniqueCount; ++i) {
            final int len = RandomInts.randomIntBetween(R, minLen, maxLen);
            final byte[] bytes = new byte[len];
            for (int j = 0; j < bytes.length; ++j) {
                bytes[j] = (byte) R.nextInt(2); // so that some instances have common prefixes
            }
            uniqueRefs[i] = new BytesRef(bytes);
        }
        final BytesRef[] result = new BytesRef[count];
        for (int i = 0; i < count; ++i) {
            result[i] = RandomPicks.randomFrom(R, uniqueRefs);
        }
        int totalLen = 0;
        for (BytesRef b : result) {
            totalLen += b.length;
        }
        final byte[] data = new byte[totalLen];
        int offset = 0;
        for (int i = 0; i < count; ++i) {
            final BytesRef b = result[i];
            System.arraycopy(b.bytes, b.offset, data, offset, b.length);
            result[i] = new BytesRef(data, offset, b.length);
            offset += b.length;
        }
        if (offset != totalLen) {
            throw new AssertionError();
        }
        return result;
    }

    private static long bench(Comparator comparator, BytesRef[] refs, int iters) {
        boolean xor = false;
        final long start = System.nanoTime();
        for (int iter = 0; iter < iters; ++iter) {
            for (int i = 0; i < refs.length; ++i) {
                for (int j = i + 1; j < refs.length; ++j) {
                    xor ^= comparator.compare(refs[i], refs[j]);
                }
            }
        }
        DUMMY = xor;
        return System.nanoTime() - start;
    }

    public static void main(String[] args) throws InterruptedException {
        // warmup
        BytesRef[] bytes = buildBytesRefs(2, 20, 1000, 100);
        final long start = System.nanoTime();
        while (System.nanoTime() - start < TimeUnit.SECONDS.toNanos(10)) {
            for (Comparator comparator : Comparator.values()) {
                bench(comparator, bytes, 1);
            }
        }

        System.out.println("## Various lengths");
        // make sure GC doesn't hurt results
        System.gc();
        Thread.sleep(2000);
        for (Comparator comparator : Comparator.values()) {
            bench(comparator, bytes, ITERS);
        }
        for (int i = 0; i < 3; ++i) {
            for (Comparator comparator : Comparator.values()) {
                System.out.println(comparator + " " + new TimeValue(bench(comparator, bytes, ITERS), TimeUnit.NANOSECONDS));
            }
        }

        for (int len = 2; len <= 20; ++len) {
            System.out.println("## Length = " + len);
            bytes = buildBytesRefs(len, len, 1000, 100);
            System.gc();
            Thread.sleep(2000);
            for (Comparator comparator : Comparator.values()) {
                bench(comparator, bytes, ITERS);
            }
            for (int i = 0; i < 3; ++i) {
                for (Comparator comparator : Comparator.values()) {
                    System.out.println(comparator + " " + new TimeValue(bench(comparator, bytes, ITERS), TimeUnit.NANOSECONDS));
                }
            }
        }
    }

}
