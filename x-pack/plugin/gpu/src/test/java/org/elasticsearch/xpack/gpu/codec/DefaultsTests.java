/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.gpu.GPUPlugin;

import static org.elasticsearch.xpack.gpu.codec.ES92GpuHnswSQVectorsFormatTests.createES92GpuHnswSQVectorsFormat;
import static org.elasticsearch.xpack.gpu.codec.ES92GpuHnswVectorsFormatTests.createES92GpuHnswVectorsFormat;
import static org.hamcrest.Matchers.startsWith;

/** Tests for various non-functional things that do not require a GPU or even cuVS. */
public class DefaultsTests extends ESTestCase {

    public void testDefaultTinySegmentSize() {
        assertEquals(GPUPlugin.MIN_NUM_VECTORS_FOR_GPU_BUILD, 10_000);
    }

    public void testES92GpuHnswVectorsFormatString() {
        var format = createES92GpuHnswVectorsFormat(2);
        String expectedStr = "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, "
            + "maxConn=16, beamWidth=128, tinySegmentsThreshold=2, flatVectorFormat=Lucene99FlatVectorsFormat)";
        assertEquals(expectedStr, format.toString());

        format = createES92GpuHnswVectorsFormat(10);
        expectedStr = "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, "
            + "maxConn=16, beamWidth=128, tinySegmentsThreshold=10, flatVectorFormat=Lucene99FlatVectorsFormat)";
        assertEquals(expectedStr, format.toString());

        format = createES92GpuHnswVectorsFormat(1_001_001);
        expectedStr = "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, "
            + "maxConn=16, beamWidth=128, tinySegmentsThreshold=1001001, flatVectorFormat=Lucene99FlatVectorsFormat)";
        assertEquals(expectedStr, format.toString());

        assertThrows(IllegalArgumentException.class, () -> createES92GpuHnswVectorsFormat(1));
        assertThrows(IllegalArgumentException.class, () -> createES92GpuHnswVectorsFormat(0));
        assertThrows(IllegalArgumentException.class, () -> createES92GpuHnswVectorsFormat(-1));
    }

    public void testES92GpuHnswSQVectorsFormatString() {
        var format = createES92GpuHnswSQVectorsFormat(2);
        String expectedStr = "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, "
            + "maxConn=16, beamWidth=128, tinySegmentsThreshold=2, flatVectorFormat=ES814ScalarQuantizedVectorsFormat(";
        assertThat(format.toString(), startsWith(expectedStr));

        format = createES92GpuHnswSQVectorsFormat(10);
        expectedStr = "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, "
            + "maxConn=16, beamWidth=128, tinySegmentsThreshold=10, flatVectorFormat=ES814ScalarQuantizedVectorsFormat(";
        assertThat(format.toString(), startsWith(expectedStr));

        format = createES92GpuHnswSQVectorsFormat(1_001_001);
        expectedStr = "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, "
            + "maxConn=16, beamWidth=128, tinySegmentsThreshold=1001001, flatVectorFormat=ES814ScalarQuantizedVectorsFormat(";
        assertThat(format.toString(), startsWith(expectedStr));

        assertThrows(IllegalArgumentException.class, () -> createES92GpuHnswSQVectorsFormat(1));
        assertThrows(IllegalArgumentException.class, () -> createES92GpuHnswSQVectorsFormat(0));
        assertThrows(IllegalArgumentException.class, () -> createES92GpuHnswSQVectorsFormat(-1));
    }
}
