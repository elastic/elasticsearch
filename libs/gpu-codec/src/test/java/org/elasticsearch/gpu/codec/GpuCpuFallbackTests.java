/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu.codec;

import com.nvidia.cuvs.CagraIndexParams;
import com.nvidia.cuvs.CuVSMatrix;

import org.apache.logging.log4j.Level;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Exercises the CPU fallback of the GPU formats when the GPU can't build the graph, for flush and for merge, with a resource
 * manager that always fails the way it does when the estimated memory exceeds the GPU total memory. No GPU is used, so
 * unlike the GPU format tests, these tests also run on nodes without a GPU.
 */
public class GpuCpuFallbackTests extends ESTestCase {

    private static final String FIELD = "v";
    private static final int DIMS = 64;
    private static final int DOCS_PER_SEGMENT = 500;
    private static final int NUM_SEGMENTS = 3;

    public void testFloatFallback() throws Exception {
        testFallback(new ES92GpuHnswVectorsFormat(AlwaysFailingResourceManager::new, 0, DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH));
    }

    public void testInt8Fallback() throws Exception {
        testFallback(
            new ES92GpuHnswSQVectorsFormat(AlwaysFailingResourceManager::new, 0, DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH, null, 7, false)
        );
    }

    public void testBBQFallback() throws Exception {
        // a graph threshold of 0, so that the small flushed segments get a graph rather than the default bbq threshold skipping it
        testFallback(
            new ES97GpuHnswBBQVectorsFormat(AlwaysFailingResourceManager::new, 0, DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH, false, 0, false)
        );
    }

    /**
     * Flushes several segments, each falling back to CPU, then force merges them, which falls back to CPU too. The merged
     * segment must have a graph over all its vectors with a recall comparable to a graph built by the CPU format, and the
     * merge must not leave temporary files behind, such as the quantized query vectors bbq writes to build the graph.
     */
    private void testFallback(KnnVectorsFormat format) throws IOException {
        int numDocs = DOCS_PER_SEGMENT * NUM_SEGMENTS;
        VectorSimilarityFunction similarity = randomFrom(VectorSimilarityFunction.EUCLIDEAN, VectorSimilarityFunction.COSINE);
        float[][] vectors = new float[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            vectors[i] = randomVector();
            if (similarity == VectorSimilarityFunction.COSINE) {
                // as Elasticsearch does before indexing and searching; bbq requires it
                VectorUtil.l2normalize(vectors[i]);
            }
        }

        // Merges take the fast path when the merged vectors are mmapped, and otherwise the slow path, which copies them into a
        // cuVS host matrix before acquiring the GPU. Both must fall back to CPU, including when cuVS fails to copy the vectors,
        // as it does on platforms it doesn't support.
        Path path = createTempDir();
        try (Directory dir = randomBoolean() ? new MMapDirectory(path) : new NIOFSDirectory(path)) {
            // no merges while indexing, so that each flushed segment is kept as is until the force merge
            IndexWriterConfig config = new IndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(format))
                .setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                MockLog.assertThatLogger(() -> {
                    try {
                        for (int i = 0; i < numDocs; i++) {
                            Document doc = new Document();
                            doc.add(new StringField("id", Integer.toString(i), Field.Store.YES));
                            doc.add(new KnnFloatVectorField(FIELD, vectors[i], similarity));
                            writer.addDocument(doc);
                            if ((i + 1) % DOCS_PER_SEGMENT == 0) {
                                writer.flush();
                            }
                        }
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                },
                    ES92GpuHnswVectorsWriter.class,
                    new MockLog.SeenEventExpectation(
                        "flush fallback",
                        ES92GpuHnswVectorsWriter.class.getCanonicalName(),
                        Level.WARN,
                        "Failed to do flush on GPU for field [v] with [500] vectors because of [*"
                    )
                );

                writer.getConfig().setMergePolicy(new TieredMergePolicy());
                MockLog.assertThatLogger(() -> {
                    try {
                        writer.forceMerge(1);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                },
                    ES92GpuHnswVectorsWriter.class,
                    new MockLog.SeenEventExpectation(
                        "merge fallback",
                        ES92GpuHnswVectorsWriter.class.getCanonicalName(),
                        Level.WARN,
                        "Failed to do merge on GPU for field [v] with [" + numDocs + "] vectors because of [*"
                    )
                );
                writer.commit();
            }

            for (String file : dir.listAll()) {
                assertFalse("temporary file left behind: " + file, file.endsWith(".tmp"));
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                KnnVectorsReader vectorsReader = ((CodecReader) leaf).getVectorReader();
                if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
                    vectorsReader = fieldsReader.getFieldReader(FIELD);
                }
                HnswGraph graph = ((HnswGraphProvider) vectorsReader).getGraph(FIELD);
                assertThat(graph, not(nullValue()));
                assertThat(graph.size(), equalTo(numDocs));

                // Recall of the true top 10 among the top 100 found, averaged over queries. On this data the graphs built
                // by the CPU formats of the same types get about 0.95 to 0.99, and 0.7 for bbq with cosine; a graph that
                // doesn't match the vectors would get about 100 / numDocs.
                IndexSearcher searcher = newSearcher(reader);
                StoredFields storedFields = reader.storedFields();
                int numQueries = 20;
                double recallSum = 0;
                for (int q = 0; q < numQueries; q++) {
                    float[] query = vectors[randomIntBetween(0, numDocs - 1)];
                    Set<Integer> trueTop10 = IntStream.range(0, numDocs)
                        .boxed()
                        .sorted(Comparator.comparingDouble(doc -> -similarity.compare(query, vectors[doc])))
                        .limit(10)
                        .collect(Collectors.toSet());
                    TopDocs topDocs = searcher.search(new KnnFloatVectorQuery(FIELD, query, 100), 100);
                    assertThat(topDocs.scoreDocs.length, equalTo(100));
                    int found = 0;
                    for (var scoreDoc : topDocs.scoreDocs) {
                        if (trueTop10.contains(Integer.parseInt(storedFields.document(scoreDoc.doc).get("id")))) {
                            found++;
                        }
                    }
                    recallSum += found / 10.0;
                }
                assertThat(recallSum / numQueries, greaterThan(0.5));
            }
        }
    }

    private static float[] randomVector() {
        float[] vector = new float[DIMS];
        for (int i = 0; i < DIMS; i++) {
            vector[i] = randomFloat() * 2 - 1;
        }
        return vector;
    }

    /** A resource manager that always fails, as when the estimated memory for the build exceeds the GPU total memory */
    private static class AlwaysFailingResourceManager implements CuVSResourceManager {
        @Override
        public ManagedCuVSResources acquire(
            int numVectors,
            int dims,
            CuVSMatrix.DataType dataType,
            CagraIndexParams cagraIndexParams,
            String reason
        ) throws GPUGraphBuildException {
            throw new GPUGraphBuildException("simulated GPU graph build failure for [" + reason + "]");
        }

        @Override
        public ManagedCuVSResources tryAcquire(
            int numVectors,
            int dims,
            CuVSMatrix.DataType dataType,
            CagraIndexParams cagraIndexParams,
            String reason
        ) throws GPUGraphBuildException {
            throw new GPUGraphBuildException("simulated GPU graph build failure for [" + reason + "]");
        }

        @Override
        public void finishedComputation(ManagedCuVSResources resources) {
            throw new AssertionError("no resource is ever acquired");
        }

        @Override
        public void release(ManagedCuVSResources resources) {
            throw new AssertionError("no resource is ever acquired");
        }

        @Override
        public void shutdown() {}
    }
}
