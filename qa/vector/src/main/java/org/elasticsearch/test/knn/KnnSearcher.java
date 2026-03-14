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
import org.apache.lucene.index.Term;
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
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.unit.ByteSizeValue;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import static org.elasticsearch.test.knn.KnnIndexer.PARTITION_ID_FIELD;
import static org.elasticsearch.test.knn.KnnIndexer.VECTOR_FIELD;

class KnnSearcher {

    private final List<Path> docPath;
    private final Path indexPath;
    private final Path queryPath;
    private final int numDocs;
    private final int numQueryVectors;
    private final KnnIndexTester.IndexType indexType;
    private int dim;
    private final VectorSimilarityFunction similarityFunction;
    private final VectorEncoding vectorEncoding;
    private final boolean doPrecondition;

    KnnSearcher(Path indexPath, TestConfiguration testConfiguration) {
        this.docPath = testConfiguration.docVectors();
        this.indexPath = indexPath;
        this.queryPath = testConfiguration.queryVectors();
        this.numDocs = testConfiguration.numDocs();
        this.numQueryVectors = testConfiguration.numQueries();
        this.dim = testConfiguration.dimensions();
        this.similarityFunction = testConfiguration.vectorSpace();
        this.vectorEncoding = testConfiguration.vectorEncoding().luceneEncoding();
        if (numQueryVectors <= 0) {
            throw new IllegalArgumentException("numQueryVectors must be > 0");
        }
        this.indexType = testConfiguration.indexType();
        this.doPrecondition = testConfiguration.doPrecondition();
    }

    /** Provides the filter query and query-vector-to-search mapping for each search operation. */
    interface FilterQueryProvider {
        /** Total number of search operations to execute */
        int searchCount();

        /** The filter query for the i-th saerch operation, may be null */
        Query filter(int searchIndex);

        /** Maps a search operation index to a query vector array index */
        int queryIndex(int searchIndex);
    }

    /** Consumes search result IDs and computes recall metrics. */
    interface ResultsConsumer {
        void accept(int[][] resultIds, KnnIndexTester.Results results, SearchParameters searchParameters) throws IOException;
    }

    void runSearch(KnnIndexTester.Results finalResults, SearchParameters searchParameters, Directory dir) throws IOException {
        Query filterQuery = searchParameters.filterSelectivity() < 1f
            ? generateRandomQuery(
                new Random(searchParameters.seed()),
                indexPath,
                numDocs,
                searchParameters.filterSelectivity(),
                searchParameters.filterCached()
            )
            : null;
        float[][] floatQueries = null;
        byte[][] byteQueries = null;
        int offsetByteSize = 0;
        try (FileChannel input = FileChannel.open(queryPath)) {
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
            if (vectorEncoding.equals(VectorEncoding.BYTE)) {
                byteQueries = new byte[numQueryVectors][dim];
                for (int i = 0; i < numQueryVectors; i++) {
                    targetReader.next(byteQueries[i]);
                }
            } else {
                floatQueries = new float[numQueryVectors][dim];
                for (int i = 0; i < numQueryVectors; i++) {
                    targetReader.next(floatQueries[i]);
                }
            }
        }

        FilterQueryProvider provider = new FilterQueryProvider() {
            @Override
            public int searchCount() {
                return numQueryVectors;
            }

            @Override
            public Query filter(int searchIndex) {
                return filterQuery;
            }

            @Override
            public int queryIndex(int searchIndex) {
                return searchIndex;
            }
        };
        final int finalOffsetByteSize = offsetByteSize;
        ResultsConsumer consumer = (resultIds, results, params) -> {
            logger.info("checking results");
            int[][] nn = getOrCalculateExactNN(finalOffsetByteSize, params, filterQuery);
            results.avgRecall = checkResults(resultIds, nn, params.topK());
        };

        doSearch(finalResults, searchParameters, dir, floatQueries, byteQueries, provider, consumer);
    }

    /**
     * Runs partitioned search: samples a subset of partitions and runs all queries against each,
     * ensuring every tested partition gets sufficient data for meaningful recall measurement.
     * Recall is computed per-partition and reported as a per-partition map plus average.
     */
    void runPartitionSearch(
        KnnIndexTester.Results finalResults,
        SearchParameters searchParameters,
        Directory dir,
        PartitionDataGenerator generator
    ) throws IOException {
        List<String> partitionIds = new ArrayList<>(generator.getPartitionAssignments().keySet());
        int numPartitions = partitionIds.size();
        Random queryRandom = new Random(searchParameters.seed());

        int numSampledPartitions = Math.min(numPartitions, 10);
        List<String> sampledPartitions = new ArrayList<>(partitionIds);
        Collections.shuffle(sampledPartitions, queryRandom);
        sampledPartitions = sampledPartitions.subList(0, numSampledPartitions);
        logger.info("Sampled {} of {} partitions for search", numSampledPartitions, numPartitions);

        Query selectivityFilter = searchParameters.filterSelectivity() < 1f
            ? generateRandomQuery(
                new Random(searchParameters.seed()),
                indexPath,
                numDocs,
                searchParameters.filterSelectivity(),
                searchParameters.filterCached()
            )
            : null;

        float[][] floatQueries = null;
        byte[][] byteQueries = null;
        if (vectorEncoding.equals(VectorEncoding.BYTE)) {
            byteQueries = generator.generateQueryByteVectors(numQueryVectors);
        } else {
            floatQueries = generator.generateQueryVectors(numQueryVectors);
        }

        int totalSearches = numSampledPartitions * numQueryVectors;
        List<String> finalSampledPartitions = sampledPartitions;
        FilterQueryProvider provider = new FilterQueryProvider() {
            @Override
            public int searchCount() {
                return totalSearches;
            }

            @Override
            public Query filter(int searchIndex) {
                int p = searchIndex / numQueryVectors;
                Query partitionFilter = new TermQuery(new Term(PARTITION_ID_FIELD, finalSampledPartitions.get(p)));
                return combineFilters(partitionFilter, selectivityFilter);
            }

            @Override
            public int queryIndex(int searchIndex) {
                return searchIndex % numQueryVectors;
            }
        };

        final float[][] finalFloatQueries = floatQueries;
        final byte[][] finalByteQueries = byteQueries;
        ResultsConsumer consumer = (resultIds, results, params) -> {
            logger.info("computing brute-force exact partitioned KNN matches for {} total queries", totalSearches);
            long nnStartNS = System.nanoTime();
            int[][] nn = new int[totalSearches][];
            try (Directory indexDir = FSDirectory.open(indexPath); DirectoryReader reader = DirectoryReader.open(indexDir)) {
                List<Callable<Void>> tasks = new ArrayList<>();
                for (int p = 0; p < numSampledPartitions; p++) {
                    Query partitionFilter = new TermQuery(new Term(PARTITION_ID_FIELD, finalSampledPartitions.get(p)));
                    Query combinedFilter = combineFilters(partitionFilter, selectivityFilter);
                    for (int q = 0; q < numQueryVectors; q++) {
                        int idx = p * numQueryVectors + q;
                        if (vectorEncoding.equals(VectorEncoding.BYTE)) {
                            tasks.add(
                                new ComputeNNByteTask(
                                    idx,
                                    params.topK(),
                                    finalByteQueries[q],
                                    nn,
                                    reader,
                                    combinedFilter,
                                    similarityFunction
                                )
                            );
                        } else {
                            tasks.add(
                                new ComputeNNFloatTask(
                                    idx,
                                    params.topK(),
                                    finalFloatQueries[q],
                                    nn,
                                    reader,
                                    combinedFilter,
                                    similarityFunction
                                )
                            );
                        }
                    }
                }
                ForkJoinPool.commonPool().invokeAll(tasks);
            }
            long nnElapsedMS = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - nnStartNS);
            logger.info("computed {} exact partitioned NN matches in {} ms", totalSearches, nnElapsedMS);

            Map<String, Float> perPartitionRecall = new LinkedHashMap<>();
            float totalRecall = 0;
            for (int p = 0; p < numSampledPartitions; p++) {
                int partitionMatches = 0;
                int partitionTotal = numQueryVectors * params.topK();
                for (int q = 0; q < numQueryVectors; q++) {
                    int idx = p * numQueryVectors + q;
                    partitionMatches += compareNN(nn[idx], resultIds[idx], params.topK());
                }
                float partitionRecall = partitionMatches / (float) partitionTotal;
                perPartitionRecall.put(finalSampledPartitions.get(p), partitionRecall);
                totalRecall += partitionRecall;
            }
            float avgRecall = numSampledPartitions > 0 ? totalRecall / numSampledPartitions : 0;

            float minRecall = perPartitionRecall.values().stream().min(Float::compareTo).orElse(0f);
            float maxRecall = perPartitionRecall.values().stream().max(Float::compareTo).orElse(0f);
            logger.info(
                "Partitioned recall: avg={}, min={}, max={}, sampled_partitions={}",
                String.format("%.4f", avgRecall),
                String.format("%.4f", minRecall),
                String.format("%.4f", maxRecall),
                numSampledPartitions
            );

            results.avgRecall = avgRecall;
            results.perPartitionRecall = perPartitionRecall;
        };

        doSearch(finalResults, searchParameters, dir, floatQueries, byteQueries, provider, consumer);
    }

    /**
     * Core search loop shared by both file-based and partition-based search paths.
     * Handles warm-up, multi-searcher parallelism, timing, CPU tracking, and
     * common result field population; delegates recall computation to the consumer.
     */
    private void doSearch(
        KnnIndexTester.Results finalResults,
        SearchParameters searchParameters,
        Directory dir,
        float[][] floatQueries,
        byte[][] byteQueries,
        FilterQueryProvider filterProvider,
        ResultsConsumer resultsConsumer
    ) throws IOException {
        int totalSearches = filterProvider.searchCount();
        TopDocs[] results = new TopDocs[totalSearches];
        int[][] resultIds = new int[totalSearches][];
        long elapsed, totalCpuTimeMS, totalVisited = 0;
        try (
            ExecutorService executorService = Executors.newFixedThreadPool(
                searchParameters.searchThreads(),
                r -> new Thread(r, "KnnSearcher-Thread")
            );
            ExecutorService numSearchersExecutor = searchParameters.numSearchers() > 1
                ? Executors.newFixedThreadPool(searchParameters.numSearchers(), r -> new Thread(r, "KnnSearcher-Caller"))
                : null
        ) {
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = searchParameters.searchThreads() > 1
                    ? new IndexSearcher(reader, executorService)
                    : new IndexSearcher(reader);

                // warm up
                for (int i = 0; i < totalSearches; i++) {
                    int qIdx = filterProvider.queryIndex(i);
                    Query filter = filterProvider.filter(i);
                    if (vectorEncoding.equals(VectorEncoding.BYTE)) {
                        doVectorQuery(byteQueries[qIdx], searcher, filter, searchParameters);
                    } else {
                        doVectorQuery(floatQueries[qIdx], searcher, filter, searchParameters);
                    }
                }

                IntConsumer queryConsumer = searchIdx -> {
                    int qIdx = filterProvider.queryIndex(searchIdx);
                    Query filter = filterProvider.filter(searchIdx);
                    try {
                        if (vectorEncoding.equals(VectorEncoding.BYTE)) {
                            results[searchIdx] = doVectorQuery(byteQueries[qIdx], searcher, filter, searchParameters);
                        } else {
                            results[searchIdx] = doVectorQuery(floatQueries[qIdx], searcher, filter, searchParameters);
                        }
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                };

                int[][] searchSplits = new int[searchParameters.numSearchers()][];
                int searchesPerWorker = totalSearches / searchParameters.numSearchers();
                for (int s = 0; s < searchParameters.numSearchers(); s++) {
                    int start = s * searchesPerWorker;
                    int end = (s == searchParameters.numSearchers() - 1) ? totalSearches : (s + 1) * searchesPerWorker;
                    searchSplits[s] = new int[end - start];
                    for (int i = start; i < end; i++) {
                        searchSplits[s][i - start] = i;
                    }
                }

                long startNS = System.nanoTime();
                KnnIndexTester.ThreadDetails startThreadDetails = new KnnIndexTester.ThreadDetails();
                if (numSearchersExecutor != null) {
                    var futures = new ArrayList<Future<Void>>();
                    for (int s = 0; s < searchParameters.numSearchers(); s++) {
                        int[] split = searchSplits[s];
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
                    for (int i = 0; i < totalSearches; i++) {
                        queryConsumer.accept(i);
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

                StoredFields storedFields = reader.storedFields();
                for (int i = 0; i < totalSearches; i++) {
                    totalVisited += results[i].totalHits.value();
                    resultIds[i] = getResultIds(results[i], storedFields);
                }
                logger.info(
                    "completed {} searches in {} ms: {} QPS CPU time={}ms",
                    totalSearches,
                    elapsed,
                    (1000L * totalSearches) / elapsed,
                    totalCpuTimeMS
                );
            }
        }

        resultsConsumer.accept(resultIds, finalResults, searchParameters);
        finalResults.visitPercentage = indexType == KnnIndexTester.IndexType.IVF ? searchParameters.visitPercentage() : 0;
        finalResults.qps = (1000f * totalSearches) / elapsed;
        finalResults.avgLatency = (float) elapsed / totalSearches;
        finalResults.averageVisited = (double) totalVisited / totalSearches;
        finalResults.netCpuTimeMS = (double) totalCpuTimeMS / totalSearches;
        finalResults.avgCpuCount = (double) totalCpuTimeMS / elapsed;
        finalResults.filterCached = searchParameters.filterCached();
        finalResults.overSamplingFactor = searchParameters.overSamplingFactor();
        finalResults.filterSelectivity = searchParameters.filterSelectivity();
        finalResults.numCandidates = searchParameters.numCandidates();
        finalResults.earlyTermination = searchParameters.earlyTermination();
    }

    /**
     * Pre-warms the searchable snapshot cache by sequentially reading every
     * file in the directory through a single {@link IndexInput} per file.
     */
    static void preWarmDirectory(Directory dir) throws IOException {
        long startNS = System.nanoTime();
        long totalBytes = 0;
        byte[] buf = new byte[64 * 1024];
        for (String file : dir.listAll()) {
            long fileLength = dir.fileLength(file);
            try (IndexInput in = dir.openInput(file, IOContext.READONCE)) {
                long remaining = fileLength;
                while (remaining > 0) {
                    int toRead = (int) Math.min(buf.length, remaining);
                    in.readBytes(buf, 0, toRead);
                    remaining -= toRead;
                }
            }
            totalBytes += fileLength;
        }
        long elapsedMS = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNS);
        logger.info(
            "Pre-warmed searchable snapshot cache: {} across {} files in {} ms",
            ByteSizeValue.ofBytes(totalBytes),
            dir.listAll().length,
            elapsedMS
        );
    }

    private static Query combineFilters(Query primary, Query secondary) {
        if (secondary == null) {
            return primary;
        }
        return new BooleanQuery.Builder().add(primary, BooleanClause.Occur.FILTER).add(secondary, BooleanClause.Occur.FILTER).build();
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

    private int[][] getOrCalculateExactNN(int vectorFileOffsetBytes, SearchParameters searchParameters, Query filterQuery)
        throws IOException {
        // look in working directory for cached nn file
        String hash = Integer.toString(
            Objects.hash(
                docPath,
                indexPath,
                queryPath,
                numDocs,
                numQueryVectors,
                searchParameters.topK(),
                similarityFunction.ordinal(),
                searchParameters.filterSelectivity()
            ),
            36
        );
        String nnFileName = "nn-" + hash + ".bin";
        Path nnPath = PathUtils.get("target/" + nnFileName);
        if (Files.exists(nnPath) && isNewer(nnPath, docPath, indexPath, queryPath)) {
            logger.info("read pre-cached exact match vectors from cache file \"" + nnPath + "\"");
            return readExactNN(nnPath, searchParameters.topK());
        } else {
            logger.info("computing brute-force exact KNN matches for " + numQueryVectors + " query vectors from \"" + queryPath + "\"");
            long startNS = System.nanoTime();
            // TODO: enable computing NN from high precision vectors when
            // checking low-precision recall
            int[][] nn = switch (vectorEncoding) {
                case BYTE -> computeExactNNByte(queryPath, filterQuery, searchParameters.topK(), vectorFileOffsetBytes);
                case FLOAT32 -> computeExactNN(queryPath, filterQuery, searchParameters.topK(), vectorFileOffsetBytes);
            };
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

    TopDocs doVectorQuery(byte[] vector, IndexSearcher searcher, Query filterQuery, SearchParameters searchParameters) throws IOException {
        Query knnQuery;
        if (searchParameters.overSamplingFactor() > 1f) {
            throw new IllegalArgumentException("oversampling factor > 1 is not supported for byte vectors");
        }
        if (indexType == KnnIndexTester.IndexType.IVF) {
            throw new IllegalArgumentException("IVF index type does not support byte vectors");
        } else {
            knnQuery = new ESKnnByteVectorQuery(
                VECTOR_FIELD,
                vector,
                searchParameters.topK(),
                searchParameters.numCandidates(),
                filterQuery,
                DenseVectorFieldMapper.FilterHeuristic.ACORN.getKnnSearchStrategy(),
                indexType == KnnIndexTester.IndexType.HNSW && searchParameters.earlyTermination()
            );
        }
        QueryProfiler profiler = new QueryProfiler();
        TopDocs docs = searcher.search(knnQuery, searchParameters.topK());
        assert knnQuery instanceof QueryProfilerProvider : "this knnQuery doesn't support profiling";
        QueryProfilerProvider queryProfilerProvider = (QueryProfilerProvider) knnQuery;
        queryProfilerProvider.profile(profiler);
        return new TopDocs(new TotalHits(profiler.getVectorOpsCount(), docs.totalHits.relation()), docs.scoreDocs);
    }

    TopDocs doVectorQuery(float[] vector, IndexSearcher searcher, Query filterQuery, SearchParameters searchParameters) throws IOException {
        Query knnQuery;
        int overSampledTopK = searchParameters.topK();
        if (searchParameters.overSamplingFactor() > 1f) {
            // oversample the topK results to get more candidates for the final result
            overSampledTopK = (int) Math.ceil(overSampledTopK * searchParameters.overSamplingFactor());
        }
        int efSearch = Math.max(overSampledTopK, searchParameters.numCandidates());
        if (indexType == KnnIndexTester.IndexType.IVF) {
            float visitRatio = (float) (searchParameters.visitPercentage() / 100);
            knnQuery = new IVFKnnFloatVectorQuery(VECTOR_FIELD, vector, overSampledTopK, efSearch, filterQuery, visitRatio, doPrecondition);
        } else {
            knnQuery = new ESKnnFloatVectorQuery(
                VECTOR_FIELD,
                vector,
                overSampledTopK,
                efSearch,
                filterQuery,
                DenseVectorFieldMapper.FilterHeuristic.ACORN.getKnnSearchStrategy(),
                indexType == KnnIndexTester.IndexType.HNSW && searchParameters.earlyTermination()
            );
        }
        if (searchParameters.overSamplingFactor() > 1f) {
            // oversample the topK results to get more candidates for the final result
            knnQuery = RescoreKnnVectorQuery.fromInnerQuery(
                VECTOR_FIELD,
                vector,
                similarityFunction,
                searchParameters.topK(),
                overSampledTopK,
                knnQuery
            );
        }
        QueryProfiler profiler = new QueryProfiler();
        TopDocs docs = searcher.search(knnQuery, searchParameters.topK());
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

    private int[][] readExactNN(Path nnPath, int topK) throws IOException {
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

    private int[][] computeExactNN(Path queryPath, Query filterQuery, int topK, int vectorFileOffsetBytes) throws IOException {
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

    private int[][] computeExactNNByte(Path queryPath, Query filterQuery, int topK, int vectorFileOffsetBytes) throws IOException {
        int[][] result = new int[numQueryVectors][];
        try (Directory dir = FSDirectory.open(indexPath); DirectoryReader reader = DirectoryReader.open(dir)) {
            List<Callable<Void>> tasks = new ArrayList<>();
            try (FileChannel qIn = FileChannel.open(queryPath)) {
                KnnIndexer.VectorReader queryReader = KnnIndexer.VectorReader.create(qIn, dim, VectorEncoding.BYTE, vectorFileOffsetBytes);
                for (int i = 0; i < numQueryVectors; i++) {
                    byte[] queryVector = new byte[dim];
                    queryReader.next(queryVector);
                    tasks.add(new ComputeNNByteTask(i, topK, queryVector, result, reader, filterQuery, similarityFunction));
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
                if ((queryOrd + 1) % 100 == 0) {
                    logger.debug(" exact knn scored " + (queryOrd + 1));
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
        private final Query filterQuery;
        private final int topK;

        ComputeNNByteTask(
            int queryOrd,
            int topK,
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
            this.similarityFunction = similarityFunction;
            this.filterQuery = filterQuery;
            this.topK = topK;
        }

        @Override
        public Void call() {
            IndexSearcher searcher = new IndexSearcher(reader);
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
                if ((queryOrd + 1) % 100 == 0) {
                    logger.debug(" exact knn scored " + (queryOrd + 1));
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
