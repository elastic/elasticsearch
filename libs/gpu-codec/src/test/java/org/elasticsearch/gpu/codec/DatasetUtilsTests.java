/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu.codec;

import com.nvidia.cuvs.CuVSMatrix;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.elasticsearch.gpu.GPUSupport;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;

public class DatasetUtilsTests extends ESTestCase {

    DatasetUtils datasetUtils;

    @Before
    public void setup() {  // TODO: abstract out setup in to common GPUTestcase
        assumeTrue("cuvs runtime only supported on 22 or greater, your JDK is " + Runtime.version(), Runtime.version().feature() >= 22);
        assumeTrue("cuvs not supported", GPUSupport.isSupported());
        datasetUtils = DatasetUtils.getInstance();
    }

    static final ValueLayout.OfFloat JAVA_FLOAT_LE = ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    public void testBasic() throws Exception {
        try (Directory dir = new MMapDirectory(createTempDir("testBasic"))) {
            int numVecs = randomIntBetween(1, 100);
            int dims = randomIntBetween(128, 2049);

            try (var out = dir.createOutput("vector.data", IOContext.DEFAULT)) {
                var ba = new byte[dims * Float.BYTES];
                var seg = MemorySegment.ofArray(ba);
                for (int v = 0; v < numVecs; v++) {
                    var src = MemorySegment.ofArray(randomVector(dims));
                    MemorySegment.copy(src, JAVA_FLOAT_UNALIGNED, 0L, seg, JAVA_FLOAT_LE, 0L, numVecs);
                    out.writeBytes(ba, 0, ba.length);
                }
            }
            try (
                var in = dir.openInput("vector.data", IOContext.DEFAULT);
                var dataset = datasetUtils.fromInput(
                    ((MemorySegmentAccessInput) in).segmentSliceOrNull(0L, in.length()),
                    numVecs,
                    dims,
                    CuVSMatrix.DataType.FLOAT
                )
            ) {
                assertEquals(numVecs, dataset.size());
                assertEquals(dims, dataset.columns());
            }
        }
    }

    static final Class<IllegalArgumentException> IAE = IllegalArgumentException.class;

    public void testIllegal() {
        MemorySegment in = null; // TODO: make this non-null
        expectThrows(IAE, () -> datasetUtils.fromInput(in, -1, 1, CuVSMatrix.DataType.FLOAT));
        expectThrows(IAE, () -> datasetUtils.fromInput(in, 1, -1, CuVSMatrix.DataType.FLOAT));
    }

    float[] randomVector(int dims) {
        float[] fa = new float[dims];
        for (int i = 0; i < dims; ++i) {
            fa[i] = random().nextFloat();
        }
        return fa;
    }
}
