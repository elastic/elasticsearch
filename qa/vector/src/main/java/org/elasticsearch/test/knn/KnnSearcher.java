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
 * a copy and modification from Lucene util
 * Modifications copyright (C) 2025 Elasticsearch B.V.
 */

package org.elasticsearch.test.knn;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.valuesource.ByteKnnVectorFieldSource;
import org.apache.lucene.queries.function.valuesource.ByteVectorSimilarityFunction;
import org.apache.lucene.queries.function.valuesource.ConstKnnByteVectorValueSource;
import org.apache.lucene.queries.function.valuesource.ConstKnnFloatValueSource;
import org.apache.lucene.queries.function.valuesource.FloatKnnVectorFieldSource;
import org.apache.lucene.queries.function.valuesource.FloatVectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.search.profile.query.QueryProfiler;
import org.elasticsearch.search.vectors.ESKnnByteVectorQuery;
import org.elasticsearch.search.vectors.ESKnnFloatVectorQuery;
import org.elasticsearch.search.vectors.IVFKnnFloatVectorQuery;
import org.elasticsearch.search.vectors.QueryProfilerProvider;
import org.elasticsearch.search.vectors.RescoreKnnVectorQuery;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.elasticsearch.test.knn.KnnIndexTester.logger;
import static org.elasticsearch.test.knn.KnnIndexer.ID_FIELD;
import static org.elasticsearch.test.knn.KnnIndexer.VECTOR_FIELD;

class KnnSearcher {

    private final Path docPath;
    private final Path indexPath;
    private final Path queryPath;
    private final int numDocs;
    private final int numQueryVectors;
    private final long randomSeed = 42;
    private final float selectivity = 1f;
    private final int topK;
    private final int efSearch;
    private final int nProbe;
    private final KnnIndexTester.IndexType indexType;
    private final int dim;
    private final VectorSimilarityFunction similarityFunction;
    private final VectorEncoding vectorEncoding;
    private final float overSamplingFactor;
    private final int searchThreads;

    KnnSearcher(Path indexPath, CmdLineArgs cmdLineArgs) {
        this.docPath = cmdLineArgs.docVectors();
        this.indexPath = indexPath;
        this.queryPath = cmdLineArgs.queryVectors();
        this.numDocs = cmdLineArgs.numDocs();
        this.numQueryVectors = cmdLineArgs.numQueries();
        this.topK = cmdLineArgs.k();
        this.dim = cmdLineArgs.dimensions();
        this.similarityFunction = cmdLineArgs.vectorSpace();
        this.vectorEncoding = cmdLineArgs.vectorEncoding();
        this.overSamplingFactor = cmdLineArgs.overSamplingFactor();
        if (numQueryVectors <= 0) {
            throw new IllegalArgumentException("numQueryVectors must be > 0");
        }
        this.efSearch = cmdLineArgs.numCandidates();
        this.nProbe = cmdLineArgs.nProbe();
        this.indexType = cmdLineArgs.indexType();
        this.searchThreads = cmdLineArgs.searchThreads();
    }

    void runSearch(KnnIndexTester.Results finalResults) throws IOException {
        TopDocs[] results = new TopDocs[numQueryVectors];
        int[][] resultIds = new int[numQueryVectors][];
        long elapsed, totalCpuTimeMS, totalVisited = 0;
        try (
            FileChannel input = FileChannel.open(queryPath);
            ExecutorService executorService = Executors.newFixedThreadPool(searchThreads, r -> new Thread(r, "KnnSearcher-Thread"))
        ) {
            long queryPathSizeInBytes = input.size();
            logger.info(
                "queryPath size: "
                    + queryPathSizeInBytes
                    + " bytes, assuming vector count is "
                    + (queryPathSizeInBytes / ((long) dim * vectorEncoding.byteSize))
            );
            KnnIndexer.VectorReader targetReader = KnnIndexer.VectorReader.create(input, dim, vectorEncoding);
            long startNS;
            try (MMapDirectory dir = new MMapDirectory(indexPath)) {
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    IndexSearcher searcher = searchThreads > 1 ? new IndexSearcher(reader, executorService) : new IndexSearcher(reader);
                    byte[] targetBytes = new byte[dim];
                    float[] target = new float[dim];
                    // warm up
                    for (int i = 0; i < numQueryVectors; i++) {
                        if (vectorEncoding.equals(VectorEncoding.BYTE)) {
                            targetReader.next(targetBytes);
                            doVectorQuery(targetBytes, searcher);
                        } else {
                            targetReader.next(target);
                            doVectorQuery(target, searcher);
                        }
                    }
                    targetReader.reset();
                    startNS = System.nanoTime();
                    KnnIndexTester.ThreadDetails startThreadDetails = new KnnIndexTester.ThreadDetails();
                    for (int i = 0; i < numQueryVectors; i++) {
                        if (vectorEncoding.equals(VectorEncoding.BYTE)) {
                            targetReader.next(targetBytes);
                            results[i] = doVectorQuery(targetBytes, searcher);
                        } else {
                            targetReader.next(target);
                            results[i] = doVectorQuery(target, searcher);
                        }
                    }
                    KnnIndexTester.ThreadDetails endThreadDetails = new KnnIndexTester.ThreadDetails();
                    elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNS);
                    long startCPUTimeNS = 0;
                    long endCPUTimeNS = 0;
                    for (int i = 0; i < startThreadDetails.threadInfos.length; i++) {
                        if (startThreadDetails.threadInfos[i].getThreadName().startsWith("KnnSearcher-Thread")) {
                            startCPUTimeNS += startThreadDetails.cpuTimesNS[i];
                        }
                    }

                    for (int i = 0; i < endThreadDetails.threadInfos.length; i++) {
                        if (endThreadDetails.threadInfos[i].getThreadName().startsWith("KnnSearcher-Thread")) {
                            endCPUTimeNS += endThreadDetails.cpuTimesNS[i];
                        }
                    }
                    totalCpuTimeMS = TimeUnit.NANOSECONDS.toMillis(endCPUTimeNS - startCPUTimeNS);

                    // Fetch, validate and write result document ids.
                    StoredFields storedFields = reader.storedFields();
                    for (int i = 0; i < numQueryVectors; i++) {
                        totalVisited += results[i].totalHits.value();
                        resultIds[i] = getResultIds(results[i], storedFields);
                    }
                    logger.info(
                        "completed {} searches in {} ms: {} QPS CPU time={}ms",
                        numQueryVectors,
                        elapsed,
                        (1000L * numQueryVectors) / elapsed,
                        totalCpuTimeMS
                    );
                }
            }
        }
        logger.info("checking results");
        int[][] nn = getOrCalculateExactNN();
        finalResults.avgRecall = checkResults(resultIds, nn, topK);
        finalResults.qps = (1000f * numQueryVectors) / elapsed;
        finalResults.avgLatency = (float) elapsed / numQueryVectors;
        finalResults.averageVisited = (double) totalVisited / numQueryVectors;
        finalResults.netCpuTimeMS = (double) totalCpuTimeMS / numQueryVectors;
        finalResults.avgCpuCount = (double) totalCpuTimeMS / elapsed;
    }

    private int[][] getOrCalculateExactNN() throws IOException {
        // look in working directory for cached nn file
        String hash = Integer.toString(
            Objects.hash(
                docPath,
                indexPath,
                queryPath,
                numDocs,
                numQueryVectors,
                topK,
                similarityFunction.ordinal(),
                selectivity,
                randomSeed
            ),
            36
        );
        String nnFileName = "nn-" + hash + ".bin";
        Path nnPath = PathUtils.get("target/" + nnFileName);
        if (Files.exists(nnPath) && isNewer(nnPath, docPath, indexPath, queryPath)) {
            logger.info("read pre-cached exact match vectors from cache file \"" + nnPath + "\"");
            return readExactNN(nnPath);
        } else {
            logger.info("computing brute-force exact KNN matches for " + numQueryVectors + " query vectors from \"" + queryPath + "\"");
            long startNS = System.nanoTime();
            // TODO: enable computing NN from high precision vectors when
            // checking low-precision recall
            int[][] nn;
            if (vectorEncoding.equals(VectorEncoding.BYTE)) {
                nn = computeExactNNByte(queryPath);
            } else {
                nn = computeExactNN(queryPath);
            }
            writeExactNN(nn, nnPath);
            long elapsedMS = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNS); // ns -> ms
            logger.info("computed " + numQueryVectors + " exact matches in " + elapsedMS + " ms");
            return nn;
        }
    }

    private boolean isNewer(Path path, Path... others) throws IOException {
        FileTime modified = Files.getLastModifiedTime(path);
        for (Path other : others) {
            if (Files.getLastModifiedTime(other).compareTo(modified) >= 0) {
                return false;
            }
        }
        return true;
    }

    TopDocs doVectorQuery(byte[] vector, IndexSearcher searcher) throws IOException {
        Query knnQuery;
        if (overSamplingFactor > 1f) {
            throw new IllegalArgumentException("oversampling factor > 1 is not supported for byte vectors");
        }
        if (indexType == KnnIndexTester.IndexType.IVF) {
            throw new IllegalArgumentException("IVF index type does not support byte vectors");
        } else {
            knnQuery = new ESKnnByteVectorQuery(
                VECTOR_FIELD,
                vector,
                topK,
                efSearch,
                null,
                DenseVectorFieldMapper.FilterHeuristic.ACORN.getKnnSearchStrategy()
            );
        }
        QueryProfiler profiler = new QueryProfiler();
        TopDocs docs = searcher.search(knnQuery, this.topK);
        QueryProfilerProvider queryProfilerProvider = (QueryProfilerProvider) knnQuery;
        queryProfilerProvider.profile(profiler);
        return new TopDocs(new TotalHits(profiler.getVectorOpsCount(), docs.totalHits.relation()), docs.scoreDocs);
    }

    TopDocs doVectorQuery(float[] vector, IndexSearcher searcher) throws IOException {
        Query knnQuery;
        int topK = this.topK;
        if (overSamplingFactor > 1f) {
            // oversample the topK results to get more candidates for the final result
            topK = (int) Math.ceil(topK * overSamplingFactor);
        }
        int efSearch = Math.max(topK, this.efSearch);
        if (indexType == KnnIndexTester.IndexType.IVF) {
            knnQuery = new IVFKnnFloatVectorQuery(VECTOR_FIELD, vector, topK, efSearch, null, nProbe);
        } else {
            knnQuery = new ESKnnFloatVectorQuery(
                VECTOR_FIELD,
                vector,
                topK,
                efSearch,
                null,
                DenseVectorFieldMapper.FilterHeuristic.ACORN.getKnnSearchStrategy()
            );
        }
        if (overSamplingFactor > 1f) {
            // oversample the topK results to get more candidates for the final result
            knnQuery = new RescoreKnnVectorQuery(VECTOR_FIELD, vector, similarityFunction, this.topK, knnQuery);
        }
        QueryProfiler profiler = new QueryProfiler();
        TopDocs docs = searcher.search(knnQuery, this.topK);
        QueryProfilerProvider queryProfilerProvider = (QueryProfilerProvider) knnQuery;
        queryProfilerProvider.profile(profiler);
        return new TopDocs(new TotalHits(profiler.getVectorOpsCount(), docs.totalHits.relation()), docs.scoreDocs);
    }

    private static float checkResults(int[][] results, int[][] nn, int topK) {
        int totalMatches = 0;
        int totalResults = results.length * topK;
        for (int i = 0; i < results.length; i++) {
            totalMatches += compareNN(nn[i], results[i], topK);
        }
        return totalMatches / (float) totalResults;
    }

    private static int compareNN(int[] expected, int[] results, int topK) {
        int matched = 0;
        Set<Integer> expectedSet = new HashSet<>();
        Set<Integer> alreadySeen = new HashSet<>();
        for (int i = 0; i < topK; i++) {
            expectedSet.add(expected[i]);
        }
        for (int docId : results) {
            if (alreadySeen.add(docId) == false) {
                throw new IllegalStateException("duplicate docId=" + docId);
            }
            if (expectedSet.contains(docId)) {
                ++matched;
            }
        }
        return matched;
    }

    private int[][] readExactNN(Path nnPath) throws IOException {
        int[][] result = new int[numQueryVectors][];
        try (FileChannel in = FileChannel.open(nnPath)) {
            IntBuffer intBuffer = in.map(FileChannel.MapMode.READ_ONLY, 0, (long) numQueryVectors * topK * Integer.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN)
                .asIntBuffer();
            for (int i = 0; i < numQueryVectors; i++) {
                result[i] = new int[topK];
                intBuffer.get(result[i]);
            }
        }
        return result;
    }

    private void writeExactNN(int[][] nn, Path nnPath) throws IOException {
        logger.info("writing true nearest neighbors to cache file \"" + nnPath + "\"");
        ByteBuffer tmp = ByteBuffer.allocate(nn[0].length * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        try (OutputStream out = Files.newOutputStream(nnPath)) {
            for (int i = 0; i < numQueryVectors; i++) {
                tmp.asIntBuffer().put(nn[i]);
                out.write(tmp.array());
            }
        }
    }

    private int[][] computeExactNN(Path queryPath) throws IOException {
        int[][] result = new int[numQueryVectors][];
        try (Directory dir = FSDirectory.open(indexPath); DirectoryReader reader = DirectoryReader.open(dir)) {
            List<Callable<Void>> tasks = new ArrayList<>();
            try (FileChannel qIn = FileChannel.open(queryPath)) {
                KnnIndexer.VectorReader queryReader = KnnIndexer.VectorReader.create(qIn, dim, VectorEncoding.FLOAT32);
                for (int i = 0; i < numQueryVectors; i++) {
                    float[] queryVector = new float[dim];
                    queryReader.next(queryVector);
                    tasks.add(new ComputeNNFloatTask(i, topK, queryVector, result, reader, similarityFunction));
                }
                ForkJoinPool.commonPool().invokeAll(tasks);
            }
            return result;
        }
    }

    private int[][] computeExactNNByte(Path queryPath) throws IOException {
        int[][] result = new int[numQueryVectors][];
        try (Directory dir = FSDirectory.open(indexPath); DirectoryReader reader = DirectoryReader.open(dir)) {
            List<Callable<Void>> tasks = new ArrayList<>();
            try (FileChannel qIn = FileChannel.open(queryPath)) {
                KnnIndexer.VectorReader queryReader = KnnIndexer.VectorReader.create(qIn, dim, VectorEncoding.BYTE);
                for (int i = 0; i < numQueryVectors; i++) {
                    byte[] queryVector = new byte[dim];
                    queryReader.next(queryVector);
                    tasks.add(new ComputeNNByteTask(i, queryVector, result, reader, similarityFunction));
                }
                ForkJoinPool.commonPool().invokeAll(tasks);
            }
            return result;
        }
    }

    static class ComputeNNFloatTask implements Callable<Void> {

        private final int queryOrd;
        private final float[] query;
        private final int[][] result;
        private final IndexReader reader;
        private final VectorSimilarityFunction similarityFunction;
        private final int topK;

        ComputeNNFloatTask(
            int queryOrd,
            int topK,
            float[] query,
            int[][] result,
            IndexReader reader,
            VectorSimilarityFunction similarityFunction
        ) {
            this.queryOrd = queryOrd;
            this.query = query;
            this.result = result;
            this.reader = reader;
            this.similarityFunction = similarityFunction;
            this.topK = topK;
        }

        @Override
        public Void call() {
            IndexSearcher searcher = new IndexSearcher(reader);
            try {
                var queryVector = new ConstKnnFloatValueSource(query);
                var docVectors = new FloatKnnVectorFieldSource(VECTOR_FIELD);
                Query query = new FunctionQuery(new FloatVectorSimilarityFunction(similarityFunction, queryVector, docVectors));
                var topDocs = searcher.search(query, topK);
                result[queryOrd] = getResultIds(topDocs, reader.storedFields());
                if ((queryOrd + 1) % 10 == 0) {
                    logger.info(" exact knn scored " + (queryOrd + 1));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return null;
        }
    }

    static class ComputeNNByteTask implements Callable<Void> {

        private final int queryOrd;
        private final byte[] query;
        private final int[][] result;
        private final IndexReader reader;
        private final VectorSimilarityFunction similarityFunction;

        ComputeNNByteTask(int queryOrd, byte[] query, int[][] result, IndexReader reader, VectorSimilarityFunction similarityFunction) {
            this.queryOrd = queryOrd;
            this.query = query;
            this.result = result;
            this.reader = reader;
            this.similarityFunction = similarityFunction;
        }

        @Override
        public Void call() {
            IndexSearcher searcher = new IndexSearcher(reader);
            int topK = result[0].length;
            try {
                var queryVector = new ConstKnnByteVectorValueSource(query);
                var docVectors = new ByteKnnVectorFieldSource(VECTOR_FIELD);
                Query query = new FunctionQuery(new ByteVectorSimilarityFunction(similarityFunction, queryVector, docVectors));
                var topDocs = searcher.search(query, topK);
                result[queryOrd] = getResultIds(topDocs, reader.storedFields());
                if ((queryOrd + 1) % 10 == 0) {
                    logger.info(" exact knn scored " + (queryOrd + 1));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return null;
        }
    }

    static int[] getResultIds(TopDocs topDocs, StoredFields storedFields) throws IOException {
        int[] resultIds = new int[topDocs.scoreDocs.length];
        int i = 0;
        for (ScoreDoc doc : topDocs.scoreDocs) {
            if (doc.doc != NO_MORE_DOCS) {
                // there is a bug somewhere that can result in doc=NO_MORE_DOCS! I think it happens
                // in some degenerate case (like input query has NaN in it?) that causes no results to
                // be returned from HNSW search?
                resultIds[i++] = Integer.parseInt(storedFields.document(doc.doc).get(ID_FIELD));
            }
        }
        return resultIds;
    }

}
