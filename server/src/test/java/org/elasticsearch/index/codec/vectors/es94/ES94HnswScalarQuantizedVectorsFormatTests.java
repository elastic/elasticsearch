/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es94;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.index.codec.vectors.BaseHnswVectorsFormatTestCase;
import org.elasticsearch.index.codec.vectors.es93.ES93GenericFlatVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.junit.AssumptionViolatedException;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;

public class ES94HnswScalarQuantizedVectorsFormatTests extends BaseHnswVectorsFormatTestCase {

    private int bits;

    @Before
    @Override
    public void setUp() throws Exception {
        bits = randomFrom(1, 2, 4, 7);
        super.setUp();
    }

    @Override
    protected KnnVectorsFormat createFormat() {
        return new ES94HnswScalarQuantizedVectorsFormat(
            DEFAULT_MAX_CONN,
            DEFAULT_BEAM_WIDTH,
            DenseVectorFieldMapper.ElementType.FLOAT,
            bits,
            false
        );
    }

    @Override
    protected KnnVectorsFormat createFormat(int maxConn, int beamWidth) {
        return new ES94HnswScalarQuantizedVectorsFormat(maxConn, beamWidth, DenseVectorFieldMapper.ElementType.FLOAT, bits, false);
    }

    @Override
    protected KnnVectorsFormat createFormat(int maxConn, int beamWidth, int numMergeWorkers, ExecutorService service) {
        return new ES94HnswScalarQuantizedVectorsFormat(
            maxConn,
            beamWidth,
            DenseVectorFieldMapper.ElementType.FLOAT,
            bits,
            false,
            numMergeWorkers,
            service
        );
    }

    @Override
    public void testSingleVectorCase() throws Exception {
        throw new AssumptionViolatedException("Scalar quantization changes the score significantly for MAXIMUM_INNER_PRODUCT");
    }

    public void testSimpleOffHeapSize() throws IOException {
        float[] vector = randomVector(random().nextInt(12, 500));
        // Use threshold=0 to ensure HNSW graph is always built, but keep assertion tolerant to implementation details.
        KnnVectorsFormat format = createFormat(16, 100, 1, null, 0);
        var config = newIndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(format));
        try (Directory dir = newDirectory()) {
            testSimpleOffHeapSize(
                dir,
                config,
                vector,
                allOf(
                    aMapWithSize(3),
                    hasEntry("vec", (long) vector.length * Float.BYTES),
                    hasEntry(equalTo("vex"), greaterThanOrEqualTo(0L)),
                    hasEntry(equalTo("veq"), greaterThan(0L))
                )
            );
        }
    }

    public void testToString() {
        KnnVectorsFormat format = new ES94HnswScalarQuantizedVectorsFormat(10, 20, DenseVectorFieldMapper.ElementType.FLOAT, 2, false);
        assertThat(
            format,
            hasToString(
                is(
                    "ES94HnswScalarQuantizedVectorsFormat(name=ES94HnswScalarQuantizedVectorsFormat, maxConn=10, beamWidth=20, "
                        + "hnswGraphThreshold="
                        + ES93HnswVectorsFormat.HNSW_GRAPH_THRESHOLD
                        + ", flatVectorFormat=ES94ScalarQuantizedVectorsFormat("
                        + "name=ES94ScalarQuantizedVectorsFormat, encoding=DIBIT_QUERY_NIBBLE, "
                        + "flatVectorScorer="
                        + ES94ScalarQuantizedVectorsFormat.flatVectorScorer
                        + ", rawVectorFormat="
                        + new ES93GenericFlatVectorsFormat(DenseVectorFieldMapper.ElementType.FLOAT, false)
                        + "))"
                )
            )
        );
    }

    protected KnnVectorsFormat createFormat(
        int maxConn,
        int beamWidth,
        int numMergeWorkers,
        ExecutorService service,
        int hnswGraphThreshold
    ) {
        return new ES94HnswScalarQuantizedVectorsFormat(
            maxConn,
            beamWidth,
            DenseVectorFieldMapper.ElementType.FLOAT,
            bits,
            false,
            numMergeWorkers,
            service,
            hnswGraphThreshold
        );
    }

    public void testDefaultHnswGraphThreshold() {
        KnnVectorsFormat format = createFormat(16, 100);
        assertThat(format.toString().contains("hnswGraphThreshold=" + ES93HnswVectorsFormat.HNSW_GRAPH_THRESHOLD), is(true));
    }

    public void testHnswGraphThresholdWithCustomValue() {
        int customThreshold = random().nextInt(1, 1001);
        KnnVectorsFormat format = createFormat(16, 100, 1, null, customThreshold);
        assertThat(format.toString().contains("hnswGraphThreshold=" + customThreshold), is(true));
    }

    public void testHnswGraphThresholdWithZeroValue() {
        // When threshold is 0, hnswGraphThreshold is omitted from toString (always build graph)
        KnnVectorsFormat format = createFormat(16, 100, 1, null, 0);
        assertThat(format.toString().contains("hnswGraphThreshold"), is(false));
    }

    public void testHnswGraphThresholdWithNegativeValueFallsBackToDefault() {
        KnnVectorsFormat format = createFormat(16, 100, 1, null, -1);
        assertThat(format.toString().contains("hnswGraphThreshold=" + ES93HnswVectorsFormat.HNSW_GRAPH_THRESHOLD), is(true));
    }
}
