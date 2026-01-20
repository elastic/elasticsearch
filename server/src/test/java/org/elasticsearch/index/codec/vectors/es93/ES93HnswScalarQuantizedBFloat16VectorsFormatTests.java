/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.index.codec.vectors.BFloat16;
import org.elasticsearch.index.codec.vectors.BaseHnswBFloat16VectorsFormatTestCase;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasToString;

public class ES93HnswScalarQuantizedBFloat16VectorsFormatTests extends BaseHnswBFloat16VectorsFormatTestCase {

    @Override
    protected KnnVectorsFormat createFormat() {
        return new ES93HnswScalarQuantizedVectorsFormat(
            16,
            100,
            DenseVectorFieldMapper.ElementType.BFLOAT16,
            null,
            7,
            false,
            random().nextBoolean()
        );
    }

    @Override
    protected KnnVectorsFormat createFormat(int maxConn, int beamWidth) {
        return new ES93HnswScalarQuantizedVectorsFormat(
            maxConn,
            beamWidth,
            DenseVectorFieldMapper.ElementType.BFLOAT16,
            null,
            7,
            false,
            random().nextBoolean()
        );
    }

    @Override
    protected KnnVectorsFormat createFormat(int maxConn, int beamWidth, int numMergeWorkers, ExecutorService service) {
        return new ES93HnswScalarQuantizedVectorsFormat(
            maxConn,
            beamWidth,
            DenseVectorFieldMapper.ElementType.BFLOAT16,
            null,
            7,
            false,
            random().nextBoolean(),
            numMergeWorkers,
            service
        );
    }

    @Override
    public void testSingleVectorCase() throws Exception {
        throw new AssumptionViolatedException("Scalar quantization changes the score significantly for MAXIMUM_INNER_PRODUCT");
    }

    public void testToString() {
        KnnVectorsFormat format = createFormat(10, 20, 1, null);
        assertThat(format, hasToString(containsString("name=ES93HnswScalarQuantizedVectorsFormat")));
        assertThat(format, hasToString(containsString("maxConn=10")));
        assertThat(format, hasToString(containsString("beamWidth=20")));
        assertThat(format, hasToString(containsString("hnswGraphThreshold=" + ES93HnswVectorsFormat.DEFAULT_HNSW_GRAPH_THRESHOLD)));
    }

    public void testSimpleOffHeapSize() throws IOException {
        float[] vector = randomVector(random().nextInt(12, 500));
        // Use threshold=0 to ensure HNSW graph is always built
        var format = new ES93HnswScalarQuantizedVectorsFormat(
            16,
            100,
            DenseVectorFieldMapper.ElementType.BFLOAT16,
            null,
            7,
            false,
            random().nextBoolean(),
            1,
            null,
            0
        );
        IndexWriterConfig config = newIndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(format));
        try (Directory dir = newDirectory()) {
            testSimpleOffHeapSize(
                dir,
                config,
                vector,
                allOf(
                    aMapWithSize(3),
                    hasEntry("vec", (long) vector.length * BFloat16.BYTES),
                    hasEntry("vex", 1L),
                    hasEntry(equalTo("veq"), greaterThan(0L))
                )
            );
        }
    }
}
