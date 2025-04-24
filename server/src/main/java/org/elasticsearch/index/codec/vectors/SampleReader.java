/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2025 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.MathUtil;

import java.io.IOException;
import java.util.Random;
import java.util.function.IntUnaryOperator;

class SampleReader extends FloatVectorValues implements HasIndexSlice {
    private final FloatVectorValues origin;
    private final int sampleSize;
    private final IntUnaryOperator sampleFunction;

    SampleReader(FloatVectorValues origin, int sampleSize, IntUnaryOperator sampleFunction) {
        this.origin = origin;
        this.sampleSize = sampleSize;
        this.sampleFunction = sampleFunction;
    }

    @Override
    public int size() {
        return sampleSize;
    }

    @Override
    public int dimension() {
        return origin.dimension();
    }

    @Override
    public FloatVectorValues copy() throws IOException {
        throw new IllegalStateException("Not supported");
    }

    @Override
    public IndexInput getSlice() {
        return ((HasIndexSlice) origin).getSlice();
    }

    @Override
    public float[] vectorValue(int targetOrd) throws IOException {
        return origin.vectorValue(sampleFunction.applyAsInt(targetOrd));
    }

    @Override
    public int getVectorByteLength() {
        return origin.getVectorByteLength();
    }

    @Override
    public int ordToDoc(int ord) {
        throw new IllegalStateException("Not supported");
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
        throw new IllegalStateException("Not supported");
    }

    static SampleReader createSampleReader(FloatVectorValues origin, int k, long seed) {
        // TODO can we do something algorithmically that aligns an ordinal with a unique integer between 0 and numVectors?
        if (k >= origin.size()) {
            new SampleReader(origin, origin.size(), i -> i);
        }
        Random rnd = new Random(seed);
        RandomLinearCongruentialMapper mapper = new RandomLinearCongruentialMapper(k, origin.size(), rnd);
        return new SampleReader(origin, k, i -> (int) mapper.map(i));
    }

    /**
     * RandomLinearCongruentialMapper is used to map a range of integers [1, n] to a range of integers [1, m]
     */
    static class RandomLinearCongruentialMapper {
        private final long n;
        private final long m;
        private final long multiplier;
        private final int randomLinearShift;

        RandomLinearCongruentialMapper(long smaller, long larger, Random random) {
            assert smaller > 0 && larger > 0;
            assert smaller < larger;
            this.n = smaller;
            this.m = larger;
            this.multiplier = findLargeOddCoprime(n);
            this.randomLinearShift = random.nextInt(0, 1024 * 1024);
        }

        // need to ensure positive modulus only
        private static long mod(long x, long m) {
            long r = x % m;
            return r < 0 ? r + m : r;
        }

        long map(long i) {
            if (i < 0 || i >= n) {
                throw new IllegalArgumentException("i out of range");
            }
            long permuted = mod((i * multiplier + randomLinearShift), n);
            return 1 + mod(permuted, m);
        }

        private static long findLargeOddCoprime(long n) {
            long candidate = n | 1; // make sure it's odd
            while (MathUtil.gcd(candidate, n) != 1) {
                candidate += 2;
            }
            return candidate;
        }
    }
}
