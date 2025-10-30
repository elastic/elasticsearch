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
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.codec.vectors.BFloat16;
import org.elasticsearch.index.codec.vectors.BaseHnswBFloat16VectorsFormatTestCase;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.ExecutorService;

import static java.lang.String.format;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class ES93HnswBFloat16VectorsFormatTests extends BaseHnswBFloat16VectorsFormatTestCase {

    @Override
    protected KnnVectorsFormat createFormat() {
        return new ES93HnswVectorsFormat(DenseVectorFieldMapper.ElementType.BFLOAT16);
    }

    @Override
    protected KnnVectorsFormat createFormat(int maxConn, int beamWidth) {
        return new ES93HnswVectorsFormat(maxConn, beamWidth, DenseVectorFieldMapper.ElementType.BFLOAT16);
    }

    @Override
    protected KnnVectorsFormat createFormat(int maxConn, int beamWidth, int numMergeWorkers, ExecutorService service) {
        return new ES93HnswVectorsFormat(maxConn, beamWidth, DenseVectorFieldMapper.ElementType.BFLOAT16, numMergeWorkers, service);
    }

    public void testToString() {
        String expected = "ES93HnswVectorsFormat(name=ES93HnswVectorsFormat, maxConn=10, beamWidth=20, flatVectorFormat=%s)";
        expected = format(Locale.ROOT, expected, "ES93GenericFlatVectorsFormat(name=ES93GenericFlatVectorsFormat, format=%s)");
        expected = format(
            Locale.ROOT,
            expected,
            "ES93BFloat16FlatVectorsFormat(name=ES93BFloat16FlatVectorsFormat, flatVectorScorer=%s())"
        );
        String defaultScorer = format(Locale.ROOT, expected, "DefaultFlatVectorScorer");
        String memSegScorer = format(Locale.ROOT, expected, "Lucene99MemorySegmentFlatVectorsScorer");

        KnnVectorsFormat format = createFormat(10, 20, 1, null);
        assertThat(format, hasToString(is(oneOf(defaultScorer, memSegScorer))));
    }

    public void testSimpleOffHeapSize() throws IOException {
        float[] vector = randomVector(random().nextInt(12, 500));
        try (Directory dir = newDirectory()) {
            testSimpleOffHeapSize(
                dir,
                newIndexWriterConfig(),
                vector,
                allOf(aMapWithSize(2), hasEntry("vec", (long) vector.length * BFloat16.BYTES), hasEntry("vex", 1L))
            );
        }
    }
}
