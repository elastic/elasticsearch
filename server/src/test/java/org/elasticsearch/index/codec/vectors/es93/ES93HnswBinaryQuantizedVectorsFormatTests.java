/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2024 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.elasticsearch.index.codec.vectors.BaseHnswVectorsFormatTestCase;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import static org.apache.lucene.tests.util.TestUtil.alwaysKnnVectorsFormat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;

public class ES93HnswBinaryQuantizedVectorsFormatTests extends BaseHnswVectorsFormatTestCase {

    @Override
    protected KnnVectorsFormat createFormat() {
        return new ES93HnswBinaryQuantizedVectorsFormat();
    }

    @Override
    protected KnnVectorsFormat createFormat(int maxConn, int beamWidth) {
        return new ES93HnswBinaryQuantizedVectorsFormat(
            maxConn,
            beamWidth,
            DenseVectorFieldMapper.ElementType.FLOAT,
            random().nextBoolean()
        );
    }

    @Override
    protected KnnVectorsFormat createFormat(int maxConn, int beamWidth, int numMergeWorkers, ExecutorService service) {
        return new ES93HnswBinaryQuantizedVectorsFormat(
            maxConn,
            beamWidth,
            DenseVectorFieldMapper.ElementType.FLOAT,
            random().nextBoolean(),
            numMergeWorkers,
            service
        );
    }

    protected KnnVectorsFormat createFormat(
        int maxConn,
        int beamWidth,
        int numMergeWorkers,
        ExecutorService service,
        int hnswGraphThreshold
    ) {
        return new ES93HnswBinaryQuantizedVectorsFormat(
            maxConn,
            beamWidth,
            DenseVectorFieldMapper.ElementType.FLOAT,
            random().nextBoolean(),
            numMergeWorkers,
            service,
            hnswGraphThreshold
        );
    }

    public void testDefaultHnswGraphThreshold() {
        KnnVectorsFormat format = createFormat(16, 100);
        assertThat(
            format,
            hasToString(containsString("hnswGraphThreshold=" + ES93HnswBinaryQuantizedVectorsFormat.DEFAULT_HNSW_GRAPH_THRESHOLD))
        );
    }

    public void testHnswGraphThresholdWithCustomValue() {
        int customThreshold = random().nextInt(1, 1001);
        KnnVectorsFormat format = createFormat(16, 100, 1, null, customThreshold);
        assertThat(format, hasToString(containsString("hnswGraphThreshold=" + customThreshold)));
    }

    public void testHnswGraphThresholdWithZeroValue() {
        // When threshold is 0, hnswGraphThreshold is omitted from toString (always build graph)
        KnnVectorsFormat format = createFormat(16, 100, 1, null, 0);
        assertThat(format.toString().contains("hnswGraphThreshold"), is(false));
    }

    public void testHnswGraphThresholdWithNegativeValueFallsBackToDefault() {
        KnnVectorsFormat format = createFormat(16, 100, 1, null, -1);
        assertThat(
            format,
            hasToString(containsString("hnswGraphThreshold=" + ES93HnswBinaryQuantizedVectorsFormat.DEFAULT_HNSW_GRAPH_THRESHOLD))
        );
    }

    public void testToString() {
        int hnswGraphThreshold = random().nextInt(1, 1001);
        KnnVectorsFormat format = createFormat(10, 20, 1, null, hnswGraphThreshold);
        assertThat(format, hasToString(containsString("name=ES93HnswBinaryQuantizedVectorsFormat")));
        assertThat(format, hasToString(containsString("maxConn=10")));
        assertThat(format, hasToString(containsString("beamWidth=20")));
        assertThat(format, hasToString(containsString("hnswGraphThreshold=" + hnswGraphThreshold)));
    }

    public void testSimpleOffHeapSize() throws IOException {
        try (Directory dir = newDirectory()) {
            testSimpleOffHeapSizeImpl(dir, newIndexWriterConfig(), true);
        }
    }

    public void testSimpleOffHeapSizeMMapDir() throws IOException {
        try (Directory dir = newMMapDirectory()) {
            testSimpleOffHeapSizeImpl(dir, newIndexWriterConfig(), true);
        }
    }

    public void testSimpleOffHeapSizeImpl(Directory dir, IndexWriterConfig config, boolean expectVecOffHeap) throws IOException {
        float[] vector = randomVector(random().nextInt(12, 500));
        // Use threshold=0 to ensure HNSW graph is always built
        var format = new ES93HnswBinaryQuantizedVectorsFormat(
            16,
            100,
            DenseVectorFieldMapper.ElementType.FLOAT,
            random().nextBoolean(),
            1,
            null,
            0
        );
        config.setCodec(alwaysKnnVectorsFormat(format));
        var matcher = expectVecOffHeap
            ? allOf(
                aMapWithSize(3),
                hasEntry("vex", 1L),
                hasEntry(equalTo("veb"), greaterThan(0L)),
                hasEntry("vec", (long) vector.length * Float.BYTES)
            )
            : allOf(aMapWithSize(2), hasEntry("vex", 1L), hasEntry(equalTo("veb"), greaterThan(0L)));

        testSimpleOffHeapSize(dir, config, vector, matcher);
    }

    static Directory newMMapDirectory() throws IOException {
        Directory dir = new MMapDirectory(createTempDir("ES93BinaryQuantizedVectorsFormatTests"));
        if (random().nextBoolean()) {
            dir = new MockDirectoryWrapper(random(), dir);
        }
        return dir;
    }
}
