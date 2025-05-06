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
        // TODO maybe use bigArrays?
        int[] samples = reservoirSample(origin.size(), k, seed);
        return new SampleReader(origin, samples.length, i -> samples[i]);
    }

    /**
     * Sample k elements from n elements according to reservoir sampling algorithm.
     *
     * @param n number of elements
     * @param k number of samples
     * @param seed random seed
     * @return array of k samples
     */
    public static int[] reservoirSample(int n, int k, long seed) {
        Random rnd = new Random(seed);
        int[] reservoir = new int[k];
        for (int i = 0; i < k; i++) {
            reservoir[i] = i;
        }
        for (int i = k; i < n; i++) {
            int j = rnd.nextInt(i + 1);
            if (j < k) {
                reservoir[j] = i;
            }
        }
        return reservoir;
    }

}
