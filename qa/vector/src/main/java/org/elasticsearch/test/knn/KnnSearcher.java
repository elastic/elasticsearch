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
import org.apache.lucene.index.LeafReaderContext;
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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilterDocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.io.Channels;
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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.elasticsearch.test.knn.KnnIndexTester.logger;
import static org.elasticsearch.test.knn.KnnIndexer.ID_FIELD;
import static org.elasticsearch.test.knn.KnnIndexer.VECTOR_FIELD;

class KnnSearcher {

    private final List<Path> docPath;
    private final Path indexPath;
    private final Path queryPath;
    private final int numDocs;
    private final int numQueryVectors;
    private final long randomSeed;
    private final float selectivity;
    private final int topK;
    private final int efSearch;
    private final double visitPercentage;
    private final KnnIndexTester.IndexType indexType;
    private int dim;
    private final VectorSimilarityFunction similarityFunction;
    private final VectorEncoding vectorEncoding;
    private final float overSamplingFactor;
    private final int searchThreads;
    private final int numSearchers;
    private final boolean filterCached;

    KnnSearcher(Path indexPath, CmdLineArgs cmdLineArgs, double visitPercentage) {
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
        this.visitPercentage = visitPercentage;
        this.indexType = cmdLineArgs.indexType();
        this.searchThreads = cmdLineArgs.searchThreads();
        this.numSearchers = cmdLineArgs.numSearchers();
        this.randomSeed = cmdLineArgs.seed();
        this.selectivity = cmdLineArgs.filterSelectivity();
        this.filterCached = cmdLineArgs.filterCached();
    }

    void runSearch(KnnIndexTester.Results finalResults, boolean earlyTermination) throws IOException {
        Query filterQuery = this.selectivity < 1f
            ? generateRandomQuery(new Random(randomSeed), indexPath, numDocs, selectivity, filterCached)
            : null;
        TopDocs[] results = new TopDocs[numQueryVectors];
        int[][] resultIds = new int[numQueryVectors][];
        long elapsed, totalCpuTimeMS, totalVisited = 0;
        int offsetByteSize = 0;
        try (
            FileChannel input = FileChannel.open(queryPath);
            ExecutorService executorService = Executors.newFixedThreadPool(searchThreads, r -> new Thread(r, "KnnSearcher-Thread"));
            ExecutorService numSearchersExecutor = numSearchers > 1
                ? Executors.newFixedThreadPool(numSearchers, r -> new Thread(r, "KnnSearcher-Caller"))
                : null
        ) {
            long queryPathSizeInBytes = input.size();
            if (dim == -1) {
                offsetByteSize = 4;
                ByteBuffer preamble = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                int bytesRead = Channels.readFromFileChannel(input, 0, preamble);
                if (bytesRead < 4) {
                    throw new IllegalArgumentException("queryPath \"" + queryPath + "\" does not contain a valid dims?");
                }
                dim = preamble.getInt(0);
                if (dim <= 0) {
                    throw new IllegalArgumentException("queryPath \"" + queryPath + "\" has invalid dimension: " + dim);
                }
            }
            if (queryPathSizeInBytes % (((long) dim * vectorEncoding.byteSize + offsetByteSize)) != 0) {
                throw new IllegalArgumentException(
                    "docsPath \"" + queryPath + "\" does not contain a whole number of vectors?  size=" + queryPathSizeInBytes
                );
            }
            logger.info(
                "queryPath size: "
                    + queryPathSizeInBytes
                    + " bytes, assuming vector count is "
                    + (queryPathSizeInBytes / ((long) dim * vectorEncoding.byteSize + offsetByteSize))
            );
            KnnIndexer.VectorReader targetReader = KnnIndexer.VectorReader.create(input, dim, vectorEncoding, offsetByteSize);
            long startNS;
            try (Directory dir = KnnIndexer.getDirectory(indexPath)) {
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    IndexSearcher searcher = searchThreads > 1 ? new IndexSearcher(reader, executorService) : new IndexSearcher(reader);
                    byte[] targetBytes = new byte[dim];
                    float[] target = new float[dim];
                    // warm up
                    for (int i = 0; i < numQueryVectors; i++) {
                        if (vectorEncoding.equals(VectorEncoding.BYTE)) {
                            targetReader.next(targetBytes);
                            doVectorQuery(targetBytes, searcher, filterQuery, earlyTermination);
                        } else {
                            targetReader.next(target);
                            doVectorQuery(target, searcher, filterQuery, earlyTermination);
                        }
                    }
                    targetReader.reset();
                    final IntConsumer[] queryConsumers = new IntConsumer[numSearchers];
                    if (vectorEncoding.equals(VectorEncoding.BYTE)) {
                        byte[][] queries = new byte[numQueryVectors][dim];
                        for (int i = 0; i < numQueryVectors; i++) {
                            targetReader.next(queries[i]);
                        }
                        for (int s = 0; s < numSearchers; s++) {
                            queryConsumers[s] = i -> {
                                try {
                                    results[i] = doVectorQuery(queries[i], searcher, filterQuery, earlyTermination);
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            };
                        }
                    } else {
                        float[][] queries = new float[numQueryVectors][dim];
                        for (int i = 0; i < numQueryVectors; i++) {
                            targetReader.next(queries[i]);
                        }
                        for (int s = 0; s < numSearchers; s++) {
                            queryConsumers[s] = i -> {
                                try {
                                    results[i] = doVectorQuery(queries[i], searcher, filterQuery, earlyTermination);
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            };
                        }
                    }
                    int[][] querySplits = new int[numSearchers][];
                    int queriesPerSearcher = numQueryVectors / numSearchers;
                    for (int s = 0; s < numSearchers; s++) {
                        int start = s * queriesPerSearcher;
                        int end = (s == numSearchers - 1) ? numQueryVectors : (s + 1) * queriesPerSearcher;
                        querySplits[s] = new int[end - start];
                        for (int i = start; i < end; i++) {
                            querySplits[s][i - start] = i;
                        }
                    }
                    targetReader.reset();
                    startNS = System.nanoTime();
                    KnnIndexTester.ThreadDetails startThreadDetails = new KnnIndexTester.ThreadDetails();
                    if (numSearchersExecutor != null) {
                        // use multiple searchers
                        var futures = new ArrayList<Future<Void>>();
                        for (int s = 0; s < numSearchers; s++) {
                            int[] split = querySplits[s];
                            IntConsumer queryConsumer = queryConsumers[s];
                            futures.add(numSearchersExecutor.submit(() -> {
                                for (int j : split) {
                                    queryConsumer.accept(j);
                                }
                                return null;
                            }));
                        }
                        for (Future<Void> future : futures) {
                            try {
                                future.get();
                            } catch (Exception e) {
                                throw new RuntimeException("Error executing searcher thread", e);
                            }
                        }
                    } else {
                        // use a single searcher
                        for (int i = 0; i < numQueryVectors; i++) {
                            queryConsumers[0].accept(i);
                        }
                    }
                    KnnIndexTester.ThreadDetails endThreadDetails = new KnnIndexTester.ThreadDetails();
                    elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNS);
                    long startCPUTimeNS = 0;
                    long endCPUTimeNS = 0;
                    for (int i = 0; i < startThreadDetails.threadInfos.length; i++) {
                        if (startThreadDetails.threadInfos[i].getThreadName().startsWith("KnnSearcher")) {
                            startCPUTimeNS += startThreadDetails.cpuTimesNS[i];
                        }
                    }

                    for (int i = 0; i < endThreadDetails.threadInfos.length; i++) {
                        if (endThreadDetails.threadInfos[i].getThreadName().startsWith("KnnSearcher")) {
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
        int[][] nn = getOrCalculateExactNN(offsetByteSize, filterQuery);
        finalResults.visitPercentage = indexType == KnnIndexTester.IndexType.IVF ? visitPercentage : 0;
        finalResults.avgRecall = checkResults(resultIds, nn, topK);
        finalResults.qps = (1000f * numQueryVectors) / elapsed;
        finalResults.avgLatency = (float) elapsed / numQueryVectors;
        finalResults.averageVisited = (double) totalVisited / numQueryVectors;
        finalResults.netCpuTimeMS = (double) totalCpuTimeMS / numQueryVectors;
        finalResults.avgCpuCount = (double) totalCpuTimeMS / elapsed;
        finalResults.filterCached = this.filterCached;
        finalResults.overSamplingFactor = this.overSamplingFactor;
    }

    private static Query generateRandomQuery(Random random, Path indexPath, int size, float selectivity, boolean filterCached)
        throws IOException {
        FixedBitSet bitSet = new FixedBitSet(size);
        for (int i = 0; i < size; i++) {
            if (random.nextFloat() < selectivity) {
                bitSet.set(i);
            } else {
                bitSet.clear(i);
            }
        }

        try (Directory dir = FSDirectory.open(indexPath); DirectoryReader reader = DirectoryReader.open(dir)) {
            BitSet[] segmentDocs = new BitSet[reader.leaves().size()];
            for (var leafContext : reader.leaves()) {
                var storedFields = leafContext.reader().storedFields();
                FixedBitSet segmentBitSet = new FixedBitSet(reader.maxDoc());
                for (int d = 0; d < leafContext.reader().maxDoc(); d++) {
                    int docID = Integer.parseInt(storedFields.document(d, Set.of(ID_FIELD)).get(ID_FIELD));
                    if (bitSet.get(docID)) {
                        segmentBitSet.set(d);
                    }
                }
                segmentDocs[leafContext.ord] = segmentBitSet;
            }
            return new BitSetQuery(segmentDocs, filterCached);
        }
    }

    private int[][] getOrCalculateExactNN(int vectorFileOffsetBytes, Query filterQuery) throws IOException {
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
                nn = computeExactNNByte(queryPath, filterQuery, vectorFileOffsetBytes);
            } else {
                nn = computeExactNN(queryPath, filterQuery, vectorFileOffsetBytes);
            }
            writeExactNN(nn, nnPath);
            long elapsedMS = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNS); // ns -> ms
            logger.info("computed " + numQueryVectors + " exact matches in " + elapsedMS + " ms");
            return nn;
        }
    }

    private boolean isNewer(Path path, List<Path> paths, Path... others) throws IOException {
        FileTime modified = Files.getLastModifiedTime(path);
        for (Path p : paths) {
            if (Files.getLastModifiedTime(p).compareTo(modified) >= 0) {
                return false;
            }
        }
        for (Path other : others) {
            if (Files.getLastModifiedTime(other).compareTo(modified) >= 0) {
                return false;
            }
        }
        return true;
    }

    TopDocs doVectorQuery(byte[] vector, IndexSearcher searcher, Query filterQuery, boolean earlyTermination) throws IOException {
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
                filterQuery,
                DenseVectorFieldMapper.FilterHeuristic.ACORN.getKnnSearchStrategy(),
                indexType == KnnIndexTester.IndexType.HNSW && earlyTermination
            );
        }
        QueryProfiler profiler = new QueryProfiler();
        TopDocs docs = searcher.search(knnQuery, this.topK);
        assert knnQuery instanceof QueryProfilerProvider : "this knnQuery doesn't support profiling";
        QueryProfilerProvider queryProfilerProvider = (QueryProfilerProvider) knnQuery;
        queryProfilerProvider.profile(profiler);
        return new TopDocs(new TotalHits(profiler.getVectorOpsCount(), docs.totalHits.relation()), docs.scoreDocs);
    }

    TopDocs doVectorQuery(float[] vector, IndexSearcher searcher, Query filterQuery, boolean earlyTermination) throws IOException {
        Query knnQuery;
        int topK = this.topK;
        if (overSamplingFactor > 1f) {
            // oversample the topK results to get more candidates for the final result
            topK = (int) Math.ceil(topK * overSamplingFactor);
        }
        int efSearch = Math.max(topK, this.efSearch);
        if (indexType == KnnIndexTester.IndexType.IVF) {
            float visitRatio = (float) (visitPercentage / 100);
            knnQuery = new IVFKnnFloatVectorQuery(VECTOR_FIELD, vector, topK, efSearch, filterQuery, visitRatio);
        } else {
            knnQuery = new ESKnnFloatVectorQuery(
                VECTOR_FIELD,
                vector,
                topK,
                efSearch,
                filterQuery,
                DenseVectorFieldMapper.FilterHeuristic.ACORN.getKnnSearchStrategy(),
                indexType == KnnIndexTester.IndexType.HNSW && earlyTermination
            );
        }
        if (overSamplingFactor > 1f) {
            // oversample the topK results to get more candidates for the final result
            knnQuery = RescoreKnnVectorQuery.fromInnerQuery(VECTOR_FIELD, vector, similarityFunction, this.topK, topK, knnQuery);
        }
        QueryProfiler profiler = new QueryProfiler();
        TopDocs docs = searcher.search(knnQuery, this.topK);
        assert knnQuery instanceof QueryProfilerProvider : "this knnQuery doesn't support profiling";
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

    private int[][] computeExactNN(Path queryPath, Query filterQuery, int vectorFileOffsetBytes) throws IOException {
        int[][] result = new int[numQueryVectors][];
        try (Directory dir = FSDirectory.open(indexPath); DirectoryReader reader = DirectoryReader.open(dir)) {
            List<Callable<Void>> tasks = new ArrayList<>();
            try (FileChannel qIn = FileChannel.open(queryPath)) {
                KnnIndexer.VectorReader queryReader = KnnIndexer.VectorReader.create(
                    qIn,
                    dim,
                    VectorEncoding.FLOAT32,
                    vectorFileOffsetBytes
                );
                for (int i = 0; i < numQueryVectors; i++) {
                    float[] queryVector = new float[dim];
                    queryReader.next(queryVector);
                    tasks.add(new ComputeNNFloatTask(i, topK, queryVector, result, reader, filterQuery, similarityFunction));
                }
                ForkJoinPool.commonPool().invokeAll(tasks);
            }
            return result;
        }
    }

    private int[][] computeExactNNByte(Path queryPath, Query filterQuery, int vectorFileOffsetBytes) throws IOException {
        int[][] result = new int[numQueryVectors][];
        try (Directory dir = FSDirectory.open(indexPath); DirectoryReader reader = DirectoryReader.open(dir)) {
            List<Callable<Void>> tasks = new ArrayList<>();
            try (FileChannel qIn = FileChannel.open(queryPath)) {
                KnnIndexer.VectorReader queryReader = KnnIndexer.VectorReader.create(qIn, dim, VectorEncoding.BYTE, vectorFileOffsetBytes);
                for (int i = 0; i < numQueryVectors; i++) {
                    byte[] queryVector = new byte[dim];
                    queryReader.next(queryVector);
                    tasks.add(new ComputeNNByteTask(i, queryVector, result, reader, filterQuery, similarityFunction));
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
        private final Query filterQuery;
        private final int topK;

        ComputeNNFloatTask(
            int queryOrd,
            int topK,
            float[] query,
            int[][] result,
            IndexReader reader,
            Query filterQuery,
            VectorSimilarityFunction similarityFunction
        ) {
            this.queryOrd = queryOrd;
            this.query = query;
            this.result = result;
            this.reader = reader;
            this.similarityFunction = similarityFunction;
            this.filterQuery = filterQuery;
            this.topK = topK;
        }

        @Override
        public Void call() {
            IndexSearcher searcher = new IndexSearcher(reader);
            try {
                var queryVector = new ConstKnnFloatValueSource(query);
                var docVectors = new FloatKnnVectorFieldSource(VECTOR_FIELD);
                Query query = new FunctionQuery(new FloatVectorSimilarityFunction(similarityFunction, queryVector, docVectors));
                if (filterQuery != null) {
                    query = new BooleanQuery.Builder().add(query, BooleanClause.Occur.SHOULD)
                        .add(filterQuery, BooleanClause.Occur.FILTER)
                        .build();
                }
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
        private final Query filterQuery;
        private final VectorSimilarityFunction similarityFunction;

        ComputeNNByteTask(
            int queryOrd,
            byte[] query,
            int[][] result,
            IndexReader reader,
            Query filterQuery,
            VectorSimilarityFunction similarityFunction
        ) {
            this.queryOrd = queryOrd;
            this.query = query;
            this.result = result;
            this.reader = reader;
            this.filterQuery = filterQuery;
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
                if (filterQuery != null) {
                    query = new BooleanQuery.Builder().add(query, BooleanClause.Occur.SHOULD)
                        .add(filterQuery, BooleanClause.Occur.FILTER)
                        .build();
                }
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

    private static class BitSetQuery extends Query {
        private final BitSet[] segmentDocs;
        private final int[] cardinalities;
        private final int segmentHash;
        private final boolean filterCached;

        BitSetQuery(BitSet[] segmentDocs, boolean filterCached) {
            this.segmentDocs = segmentDocs;
            this.cardinalities = new int[segmentDocs.length];
            for (int i = 0; i < segmentDocs.length; i++) {
                cardinalities[i] = segmentDocs[i].cardinality();
            }
            this.segmentHash = Arrays.hashCode(cardinalities);
            this.filterCached = filterCached;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new ConstantScoreWeight(this, boost) {
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    var bitSet = segmentDocs[context.ord];
                    var cardinality = cardinalities[context.ord];
                    final DocIdSetIterator bitSetIterator = new BitSetIterator(bitSet, cardinality);
                    final DocIdSetIterator iterator = filterCached ? bitSetIterator : new FilterDocIdSetIterator(bitSetIterator);

                    var scorer = new ConstantScoreScorer(score(), scoreMode, iterator);
                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long leadCost) throws IOException {
                            return scorer;
                        }

                        @Override
                        public long cost() {
                            return cardinality;
                        }
                    };
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return false;
                }
            };
        }

        @Override
        public void visit(QueryVisitor visitor) {}

        @Override
        public String toString(String field) {
            return "BitSetQuery";
        }

        @Override
        public boolean equals(Object other) {
            return sameClassAs(other) && Arrays.equals(segmentDocs, ((BitSetQuery) other).segmentDocs);
        }

        @Override
        public int hashCode() {
            return 31 * classHash() + segmentHash;
        }
    }

}
