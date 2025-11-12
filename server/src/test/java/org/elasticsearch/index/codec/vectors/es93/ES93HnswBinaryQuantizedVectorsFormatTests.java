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
import java.util.Locale;
import java.util.concurrent.ExecutorService;

import static java.lang.String.format;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.oneOf;

public class ES93HnswBinaryQuantizedVectorsFormatTests extends BaseHnswVectorsFormatTestCase {

    @Override
    protected KnnVectorsFormat createFormat() {
        return new ES93HnswBinaryQuantizedVectorsFormat(DenseVectorFieldMapper.ElementType.FLOAT, random().nextBoolean());
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

    public void testToString() {
        String expected = "ES93HnswBinaryQuantizedVectorsFormat("
            + "name=ES93HnswBinaryQuantizedVectorsFormat, maxConn=10, beamWidth=20, flatVectorFormat=%s)";
        expected = format(
            Locale.ROOT,
            expected,
            "ES93BinaryQuantizedVectorsFormat(name=ES93BinaryQuantizedVectorsFormat, rawVectorFormat=%s,"
                + " scorer=ES818BinaryFlatVectorsScorer(nonQuantizedDelegate={}()))"
        );
        expected = format(Locale.ROOT, expected, "ES93GenericFlatVectorsFormat(name=ES93GenericFlatVectorsFormat, format=%s)");
        expected = format(Locale.ROOT, expected, "Lucene99FlatVectorsFormat(name=Lucene99FlatVectorsFormat, flatVectorScorer={}())");
        String defaultScorer = expected.replaceAll("\\{}", "DefaultFlatVectorScorer");
        String memSegScorer = expected.replaceAll("\\{}", "Lucene99MemorySegmentFlatVectorsScorer");

        KnnVectorsFormat format = createFormat(10, 20, 1, null);
        assertThat(format, hasToString(oneOf(defaultScorer, memSegScorer)));
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
