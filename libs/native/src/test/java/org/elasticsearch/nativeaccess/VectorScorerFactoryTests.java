/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static org.elasticsearch.nativeaccess.VectorSimilarityType.DOT_PRODUCT;
import static org.hamcrest.Matchers.equalTo;

public class VectorScorerFactoryTests extends AbstractVectorTestCase {

    // Tests that the provider instance is null or not null on expected platforms/architectures
    public void testSupport() {
        supported();
    }

    // TODO: test maximum inner product, cosine, euclidean

    public void testDotProductSimple() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        Path topLevelDir = createTempDir(getTestName());

        // dimensions that cross the scalar / native boundary (stride)
        for (int dims : List.of(31, 32, 33)) {
            byte[] vec1 = new byte[dims];
            byte[] vec2 = new byte[dims];
            for (int i = 0; i < dims; i++) {
                vec1[i] = (byte) i;
                vec2[i] = (byte) (dims - i);
            }
            var oneFactor = floatToByteArray(1f);
            byte[] bytes = concat(vec1, oneFactor, vec2, oneFactor);
            Path path = topLevelDir.resolve("data-" + dims + ".vec");
            Files.write(path, bytes, CREATE_NEW);

            try (var scorer = factory.getScalarQuantizedVectorScorer(dims, 2, 1, DOT_PRODUCT, path)) {
                assertThat(scorer.score(0, 1), equalTo(scalarQuantizedDotProductScore(vec1, vec2, 1, 1, 1)));
            }
        }
    }

    public void testDotProductRandom() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        final Path topLevelDir = createTempDir(getTestName());
        testDotProductRandom(topLevelDir, ESTestCase::randomByteArrayOfLength);
    }

    public void testDotProductRandomMax() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        final Path topLevelDir = createTempDir(getTestName());
        Function<Integer, byte[]> byteArraySupplier = size -> {
            byte[] ba = new byte[size];
            Arrays.fill(ba, Byte.MAX_VALUE);
            return ba;
        };
        testDotProductRandom(topLevelDir, byteArraySupplier);
    }

    public void testDotProductRandomMin() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        final Path topLevelDir = createTempDir(getTestName());
        Function<Integer, byte[]> byteArraySupplier = size -> {
            byte[] ba = new byte[size];
            Arrays.fill(ba, Byte.MIN_VALUE);
            return ba;
        };
        testDotProductRandom(topLevelDir, byteArraySupplier);
    }

    static void testDotProductRandom(Path topLevelDir, Function<Integer, byte[]> byteArraySupplier) throws IOException {

        for (int times = 0; times < TIMES; times++) {
            final int dims = randomIntBetween(1, 4096);
            final int size = randomIntBetween(2, 100);
            final float correction = randomFloat();
            final byte[][] vectors = new byte[size][];
            final float[] offsets = new float[size];
            Path path = topLevelDir.resolve("data-" + times + ".vec");

            try (var os = Files.newOutputStream(path, CREATE_NEW)) {
                for (int i = 0; i < size; i++) {
                    var vec = byteArraySupplier.apply(dims);
                    var off = randomFloat();
                    os.write(vec);
                    os.write(floatToByteArray(off));
                    vectors[i] = vec;
                    offsets[i] = off;
                }
            }
            try (var scorer = factory.getScalarQuantizedVectorScorer(dims, size, correction, DOT_PRODUCT, path)) {
                int idx0 = randomIntBetween(0, size - 1);
                int idx1 = randomIntBetween(0, size - 1); // may be the same as idx0 - which is ok.
                float expected = scalarQuantizedDotProductScore(vectors[idx0], vectors[idx1], correction, offsets[idx0], offsets[idx1]);
                assertThat(scorer.score(idx0, idx1), equalTo(expected));
            }
        }
    }

    static final int TIMES = 100; // a loop iteration times
}
