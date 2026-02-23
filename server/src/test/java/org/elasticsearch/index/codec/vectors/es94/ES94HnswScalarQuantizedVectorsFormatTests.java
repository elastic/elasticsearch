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
import org.elasticsearch.index.codec.vectors.BaseHnswVectorsFormatTestCase;
import org.elasticsearch.index.codec.vectors.es93.ES93GenericFlatVectorsFormat;
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
        try (Directory dir = newDirectory()) {
            testSimpleOffHeapSize(
                dir,
                newIndexWriterConfig(),
                vector,
                allOf(
                    aMapWithSize(3),
                    hasEntry("vec", (long) vector.length * Float.BYTES),
                    hasEntry("vex", 1L),
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
                        + "flatVectorFormat=ES94ScalarQuantizedVectorsFormat("
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
}
