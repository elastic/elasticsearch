/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorScorer;
import org.apache.lucene.codecs.lucene99.OffHeapQuantizedByteVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizer;
import org.elasticsearch.simdvec.VectorScorerFactory;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

class BenchmarkUtils {
    // Unsigned int7 byte vectors have values in the range of 0 to 127 (inclusive).
    static final byte MIN_INT7_VALUE = 0;
    static final byte MAX_INT7_VALUE = 127;

    static void randomInt7BytesBetween(byte[] bytes) {
        var random = ThreadLocalRandom.current();
        for (int i = 0, len = bytes.length; i < len;) {
            bytes[i++] = (byte) random.nextInt(MIN_INT7_VALUE, MAX_INT7_VALUE + 1);
        }
    }

    static void createRandomInt7VectorData(ThreadLocalRandom random, Directory dir, int dims, int numVectors) throws IOException {
        try (IndexOutput out = dir.createOutput("vector.data", IOContext.DEFAULT)) {
            var vec = new byte[dims];
            for (int v = 0; v < numVectors; v++) {
                randomInt7BytesBetween(vec);
                var vecOffset = random.nextFloat();
                out.writeBytes(vec, 0, vec.length);
                out.writeInt(Float.floatToIntBits(vecOffset));
            }
        }
    }

    static VectorScorerFactory getScorerFactoryOrDie() {
        var optionalVectorScorerFactory = VectorScorerFactory.instance();
        if (optionalVectorScorerFactory.isEmpty()) {
            String msg = "JDK=["
                + Runtime.version()
                + "], os.name=["
                + System.getProperty("os.name")
                + "], os.arch=["
                + System.getProperty("os.arch")
                + "]";
            throw new AssertionError("Vector scorer factory not present. Cannot run the benchmark. " + msg);
        }
        return optionalVectorScorerFactory.get();
    }

    static boolean supportsHeapSegments() {
        return Runtime.version().feature() >= 22;
    }

    static QuantizedByteVectorValues vectorValues(int dims, int size, IndexInput in, VectorSimilarityFunction sim) throws IOException {
        var sq = new ScalarQuantizer(0.1f, 0.9f, (byte) 7);
        var slice = in.slice("values", 0, in.length());
        return new OffHeapQuantizedByteVectorValues.DenseOffHeapVectorValues(dims, size, sq, false, sim, null, slice);
    }

    static RandomVectorScorerSupplier luceneScoreSupplier(QuantizedByteVectorValues values, VectorSimilarityFunction sim)
        throws IOException {
        return new Lucene99ScalarQuantizedVectorScorer(null).getRandomVectorScorerSupplier(sim, values);
    }

    static RandomVectorScorer luceneScorer(QuantizedByteVectorValues values, VectorSimilarityFunction sim, float[] queryVec)
        throws IOException {
        return new Lucene99ScalarQuantizedVectorScorer(null).getRandomVectorScorer(sim, values, queryVec);
    }

    static float readNodeCorrectionConstant(QuantizedByteVectorValues values, int targetOrd) throws IOException {
        var vectorByteSize = values.getVectorByteLength();
        var input = (MemorySegmentAccessInput) values.getSlice();
        long byteOffset = (long) targetOrd * (vectorByteSize + Float.BYTES);
        return Float.intBitsToFloat(input.readInt(byteOffset + vectorByteSize));
    }
}
