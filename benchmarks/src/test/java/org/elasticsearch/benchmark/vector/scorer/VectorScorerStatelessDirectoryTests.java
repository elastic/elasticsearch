/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.benchmark.store.DirectoryType;
import org.elasticsearch.benchmark.vector.VectorImplementation;
import org.elasticsearch.blobcache.common.BlobCacheBufferedIndexInput;
import org.elasticsearch.core.DirectAccessInput;
import org.elasticsearch.index.store.StoreMetricsIndexInput;
import org.elasticsearch.simdvec.VectorSimilarityType;
import org.junit.BeforeClass;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.supportsHeapSegments;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

/**
 * Covers the {@link DirectoryType#STATELESS_INDEX_LOCAL} arm of the vector scorer benchmarks, which reads through a
 * {@code ReopeningIndexInput} the way a stateless indexing node does when a merge reopens a just-written flat vector file.
 *
 * <p>Two properties are asserted: that the arm reaches the vector data through {@link DirectAccessInput} rather than through a heap
 * copy (everything the benchmark measures is downstream of that one fact), and that it produces the same scores as the
 * memory-mapped path.
 */
public class VectorScorerStatelessDirectoryTests extends BenchmarkTest {

    private static final int DIMS = 1024;
    private static final int NUM_VECTORS = 1000;
    private static final int NUM_VECTORS_TO_SCORE = 200;
    private static final float DELTA = 1e-3f;
    private static final int SINGLE_READ_LENGTH = BlobCacheBufferedIndexInput.BUFFER_SIZE;

    @BeforeClass
    public static void skipUnsupported() {
        assumeTrue("native requires JDK22+", supportsHeapSegments());
    }

    /**
     * A file written to the directory and not yet uploaded is read back through a {@code ReopeningIndexInput}, wrapped in
     * the same {@link StoreMetricsIndexInput} a real shard sees. Both zero-copy routes resolve through that chain down to
     * the memory-mapped delegate, so {@code IndexInputUtils} never reaches its heap-copy fallback.
     */
    public void testLocalFileIsReadWithDirectAccess() throws IOException {
        try (Directory dir = DirectoryType.STATELESS_INDEX_LOCAL.newDirectory(createTempDir())) {
            try (IndexOutput out = dir.createOutput("vector.data", IOContext.DEFAULT)) {
                out.writeBytes(new byte[2 * SINGLE_READ_LENGTH], 2 * SINGLE_READ_LENGTH);
            }
            try (IndexInput in = dir.openInput("vector.data", IOContext.DEFAULT)) {
                assertThat(in, instanceOf(StoreMetricsIndexInput.class));
                assertThat(asInstanceOf(FilterIndexInput.class, in).getDelegate().toString(), startsWith("ReopeningIndexInput"));

                var directAccess = asInstanceOf(DirectAccessInput.class, in);
                var invoked = new AtomicBoolean();
                assertTrue(directAccess.withMemorySegmentSlice(0, SINGLE_READ_LENGTH, segment -> invoked.set(true)));
                assertTrue("the zero-copy path was not taken", invoked.get());

                try (Arena arena = Arena.ofConfined()) {
                    var addresses = arena.allocate(2 * ADDRESS.byteSize(), ADDRESS.byteAlignment());
                    invoked.set(false);
                    assertTrue(directAccess.withSliceAddresses(new long[] { 0, 32 }, 32, 2, addresses, segment -> invoked.set(true)));
                    assertTrue("the bulk gather path was not taken", invoked.get());
                }
            }
        }
    }

    public void testBulkScoresMatchMemoryMapped() throws IOException {
        var data = new VectorScorerInt4BulkBenchmark.VectorData(DIMS, NUM_VECTORS, NUM_VECTORS_TO_SCORE, random());

        var memoryMapped = createBenchmark(data, DirectoryType.MMAP);
        try {
            float[] expected = memoryMapped.scoreMultipleRandomBulk();
            var stateless = createBenchmark(data, DirectoryType.STATELESS_INDEX_LOCAL);
            try {
                assertArrayEquals(expected, stateless.scoreMultipleRandomBulk(), DELTA);
            } finally {
                stateless.teardown();
            }
        } finally {
            memoryMapped.teardown();
        }
    }

    private static VectorScorerInt4BulkBenchmark createBenchmark(VectorScorerInt4BulkBenchmark.VectorData data, DirectoryType directoryType)
        throws IOException {
        var bench = new VectorScorerInt4BulkBenchmark();
        bench.function = VectorSimilarityType.DOT_PRODUCT;
        // only the native scorers route through IndexInputUtils; the others never reach it
        bench.implementation = VectorImplementation.NATIVE;
        bench.directoryType = directoryType;
        bench.dims = DIMS;
        bench.numVectors = NUM_VECTORS;
        bench.numVectorsToScore = NUM_VECTORS_TO_SCORE;
        bench.bulkSize = NUM_VECTORS_TO_SCORE;
        bench.setup(data);
        return bench;
    }
}
