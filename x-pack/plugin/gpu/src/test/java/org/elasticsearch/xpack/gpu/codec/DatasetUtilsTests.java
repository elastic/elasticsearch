/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;

public class DatasetUtilsTests extends ESTestCase {

    @Before
    public void setup() {  // TODO: abstract out setup in to common GPUTestcase
        assumeTrue("cuvs runtime only supported on 22 or greater, your JDK is " + Runtime.version(), Runtime.version().feature() >= 22);
        try (var resources = GPUVectorsFormat.cuVSResourcesOrNull()) {
            assumeTrue("cuvs not supported", resources != null);
        }
    }

    static final ValueLayout.OfFloat JAVA_FLOAT_LE = ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    final DatasetUtils datasetUtils = DatasetUtils.getInstance();

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
                var dataset = datasetUtils.fromInput((MemorySegmentAccessInput) in, numVecs, dims)
            ) {
                assertEquals(numVecs, dataset.size());
                assertEquals(dims, dataset.dimensions());
            }
        }
    }

    static final Class<IllegalArgumentException> IAE = IllegalArgumentException.class;

    public void testIllegal() {
        MemorySegmentAccessInput in = null; // TODO: make this non-null
        expectThrows(IAE, () -> datasetUtils.fromInput(in, -1, 1));
        expectThrows(IAE, () -> datasetUtils.fromInput(in, 1, -1));
    }

    float[] randomVector(int dims) {
        float[] fa = new float[dims];
        for (int i = 0; i < dims; ++i) {
            fa[i] = random().nextFloat();
        }
        return fa;
    }
}
