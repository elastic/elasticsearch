/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.simdvec.ES91Int4VectorsScorer;

public class ES91Int4VectorScorerTests extends BaseVectorizationTests {

    public void testInt4DotProduct() throws Exception {
        // only even dimensions are supported
        final int dimensions = random().nextInt(1, 1000) * 2;
        final int numVectors = random().nextInt(1, 100);
        final byte[] vector = new byte[dimensions];
        try (Directory dir = new MMapDirectory(createTempDir())) {
            try (IndexOutput out = dir.createOutput("tests.bin", IOContext.DEFAULT)) {
                for (int i = 0; i < numVectors; i++) {
                    for (int j = 0; j < dimensions; j++) {
                        vector[j] = (byte) random().nextInt(16); // 4-bit quantization
                    }
                    out.writeBytes(vector, 0, dimensions);
                }
            }
            final byte[] query = new byte[dimensions];
            for (int j = 0; j < dimensions; j++) {
                query[j] = (byte) random().nextInt(16); // 4-bit quantization
            }
            try (IndexInput in = dir.openInput("tests.bin", IOContext.DEFAULT)) {
                // Work on a slice that has just the right number of bytes to make the test fail with an
                // index-out-of-bounds in case the implementation reads more than the allowed number of
                // padding bytes.
                final IndexInput slice = in.slice("test", 0, (long) dimensions * numVectors);
                final IndexInput slice2 = in.slice("test2", 0, (long) dimensions * numVectors);
                final ES91Int4VectorsScorer defaultScorer = defaultProvider().newES91Int4VectorsScorer(slice, dimensions);
                final ES91Int4VectorsScorer panamaScorer = maybePanamaProvider().newES91Int4VectorsScorer(slice2, dimensions);
                for (int i = 0; i < numVectors; i++) {
                    in.readBytes(vector, 0, dimensions);
                    long val = VectorUtil.int4DotProduct(vector, query);
                    assertEquals(val, defaultScorer.int4DotProduct(query));
                    assertEquals(val, panamaScorer.int4DotProduct(query));
                    assertEquals(in.getFilePointer(), slice.getFilePointer());
                    assertEquals(in.getFilePointer(), slice2.getFilePointer());
                }
                assertEquals((long) dimensions * numVectors, in.getFilePointer());
            }
        }
    }
}
