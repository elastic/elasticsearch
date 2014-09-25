/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.benchmark;

import org.apache.lucene.util.PriorityQueue;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.benchmark.start.*;
import org.elasticsearch.action.benchmark.status.*;
import org.elasticsearch.action.benchmark.competition.*;
import org.elasticsearch.action.benchmark.exception.*;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.*;
import java.util.concurrent.*;

/**
 * Handles execution, listing, pausing, and aborting of benchmarks
 */
public class BenchmarkExecutor {

    private static final ESLogger logger = Loggers.getLogger(BenchmarkExecutor.class);

    private final Client client;
    protected final ClusterService clusterService;

    public BenchmarkExecutor(Client client, ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
    }

    public BenchmarkStatusNodeActionResponse start(BenchmarkStartRequest request, BenchmarkStartResponse benchmarkStartResponse,
                                                   BenchmarkExecutorService.BenchmarkSemaphores benchmarkSemaphores) throws ElasticsearchException {
        try {
            for (BenchmarkCompetitor competitor : request.competitors()) {

                final BenchmarkSettings settings = competitor.settings();
                logger.debug("Executing [iterations: {}] [multiplier: {}] for [{}] on [{}]",
                        settings.iterations(), settings.multiplier(), request.benchmarkId(), nodeName());

                final List<CompetitionIteration> competitionIterations = new ArrayList<>(settings.iterations());
                final CompetitionResult competitionResult =
                        new CompetitionResult(competitor.name(), settings.concurrency(), settings.multiplier(), request.verbose(), request.percentiles());
                final CompetitionNodeResult competitionNodeResult =
                        new CompetitionNodeResult(competitor.name(), nodeName(), settings.iterations(), competitionIterations);

                competitionResult.addCompetitionNodeResult(competitionNodeResult);
                benchmarkStartResponse.competitionResults().put(competitor.name(), competitionResult);

                // Make sure headers and context are passed through to all searches
                final List<SearchRequest> searchRequests = bindOriginalRequest(competitor.settings().searchRequests(), request);

                // Perform warmup if requested
                if (settings.warmup()) {
                    final long start = System.nanoTime();
                    final List<String> errors = warmup(searchRequests, benchmarkSemaphores.competitorSemaphore(competitor.name()));
                    competitionNodeResult.warmUpTime(TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS));
                    if (!errors.isEmpty()) {
                        throw new BenchmarkExecutionException("Failed to execute warmup phase", errors);
                    }
                }

                for (int i = 0; i < settings.iterations(); i++) {

                    final Measurements measurements = new Measurements(settings.multiplier() * searchRequests.size());

                    // Run the iteration
                    final CompetitionIteration ci =
                            iterate(competitor, searchRequests, measurements, benchmarkSemaphores.competitorSemaphore(competitor.name()));

                    ci.percentiles(request.percentiles());
                    competitionIterations.add(ci);
                    competitionNodeResult.incrementCompletedIterations();
                }

                competitionNodeResult.totalExecutedQueries(settings.multiplier() * searchRequests.size() * settings.iterations());
            }

            benchmarkStartResponse.state(BenchmarkStartResponse.State.COMPLETED);

        } catch (BenchmarkExecutionException e) {
            benchmarkStartResponse.state(BenchmarkStartResponse.State.FAILED);
            benchmarkStartResponse.errors(e.errorMessages().toArray(new String[e.errorMessages().size()]));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            benchmarkStartResponse.state(BenchmarkStartResponse.State.ABORTED);
        } catch (Throwable ex) {
            logger.debug("Unexpected exception during benchmark", ex);
            benchmarkStartResponse.state(BenchmarkStartResponse.State.FAILED);
            benchmarkStartResponse.errors(ex.getMessage());
        } finally {
            benchmarkSemaphores.stopAllCompetitors();
        }

        final BenchmarkStatusNodeActionResponse response = new BenchmarkStatusNodeActionResponse(request.benchmarkId(), nodeId());
        response.response(benchmarkStartResponse);
        return response;
    }

    protected List<String> warmup(List<SearchRequest> searchRequests, StoppableSemaphore semaphore) throws InterruptedException {

        final CountDownLatch totalCount = new CountDownLatch(searchRequests.size());
        final CopyOnWriteArrayList<String> errorMessages = new CopyOnWriteArrayList<>();

        for (SearchRequest searchRequest : searchRequests) {
            semaphore.acquire();
            client.search(searchRequest, new BoundsManagingActionListener<SearchResponse>(semaphore, totalCount, errorMessages) { } );
        }
        totalCount.await();
        return errorMessages;
    }

    protected CompetitionIteration iterate(final BenchmarkCompetitor competitor,
                                           final List<SearchRequest> searchRequests,
                                           final Measurements measurements,
                                           final StoppableSemaphore semaphore) throws InterruptedException {

        assert measurements.timeBuckets().length == competitor.settings().multiplier() * searchRequests.size();
        assert measurements.docBuckets().length == competitor.settings().multiplier() * searchRequests.size();

        int id = 0;
        final CountDownLatch totalCount = new CountDownLatch(measurements.size());
        final CopyOnWriteArrayList<String> errorMessages = new CopyOnWriteArrayList<>();
        final long start = System.nanoTime();

        for (int i = 0; i < competitor.settings().multiplier(); i++) {
            for (SearchRequest searchRequest : searchRequests) {
                StatisticCollectionActionListener statsListener =
                        new StatisticCollectionActionListener(semaphore, measurements, id++, totalCount, errorMessages);
                semaphore.acquire();
                client.search(searchRequest, statsListener);
            }
        }

        totalCount.await();
        assert id == measurements.timeBuckets().length;
        final long totalTime = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);

        if (!errorMessages.isEmpty()) {
            throw new BenchmarkExecutionException("Too many execution failures", errorMessages);
        }

        final CompetitionIterationData iterationData = new CompetitionIterationData(measurements.timeBuckets());
        final long sumDocs = new CompetitionIterationData(measurements.docBuckets()).sum();

        // Don't track slowest request if there is only one request as that is redundant
        CompetitionIteration.SlowRequest[] topN = null;
        if ((competitor.settings().numSlowest() > 0) && (searchRequests.size() > 1)) {
            topN = getTopN(measurements.timeBuckets(), searchRequests, competitor.settings().multiplier(), competitor.settings().numSlowest());
        }

        return new CompetitionIteration(topN, totalTime, measurements.timeBuckets().length, sumDocs, iterationData);
    }

    private CompetitionIteration.SlowRequest[] getTopN(long[] buckets, List<SearchRequest> requests, int multiplier, int topN) {

        final int numRequests = requests.size();
        // collect the top N
        final PriorityQueue<IndexAndTime> topNQueue = new PriorityQueue<IndexAndTime>(topN) {
            @Override
            protected boolean lessThan(IndexAndTime a, IndexAndTime b) {
                return a.avgTime < b.avgTime;
            }
        };
        assert multiplier > 0;
        for (int i = 0; i < numRequests; i++) {
            long sum = 0;
            long max = Long.MIN_VALUE;
            for (int j = 0; j < multiplier; j++) {
                final int base = (numRequests * j);
                sum += buckets[i + base];
                max = Math.max(buckets[i + base], max);
            }
            final long avg = sum / multiplier;
            if (topNQueue.size() < topN) {
                topNQueue.add(new IndexAndTime(i, max, avg));
            } else if (topNQueue.top().avgTime < avg) {
                topNQueue.top().update(i, max, avg);
                topNQueue.updateTop();
            }
        }

        final CompetitionIteration.SlowRequest[] slowRequests = new CompetitionIteration.SlowRequest[topNQueue.size()];
        int i = topNQueue.size() - 1;

        while (topNQueue.size() > 0) {
            IndexAndTime pop = topNQueue.pop();
            CompetitionIteration.SlowRequest slow =
                    new CompetitionIteration.SlowRequest(pop.avgTime, pop.maxTime, requests.get(pop.index));
            slowRequests[i--] = slow;
        }

        return slowRequests;
    }

    private static class IndexAndTime {
        int index;
        long maxTime;
        long avgTime;

        public IndexAndTime(int index, long maxTime, long avgTime) {
            this.index = index;
            this.maxTime = maxTime;
            this.avgTime = avgTime;
        }

        public void update(int index, long maxTime, long avgTime) {
            this.index = index;
            this.maxTime = maxTime;
            this.avgTime = avgTime;
        }
    }

    protected static class Measurements {

        private final long[] timeBuckets;
        private final long[] docBuckets;

        public Measurements(final int size) {
            timeBuckets = new long[size];
            docBuckets = new long[size];
            Arrays.fill(timeBuckets, -1);   // wipe CPU cache     ;)
            Arrays.fill(docBuckets, -1);    // wipe CPU cache     ;)
        }

        public long[] timeBuckets() {
            return timeBuckets;
        }

        public long[] docBuckets() {
            return docBuckets;
        }

        public int size() {
            return timeBuckets.length;
        }

        public void invalidate(final int bucketId) {
            timeBuckets[bucketId] = -1;
            docBuckets[bucketId] = -1;
        }
    }

    private static class BoundsManagingActionListener<Response> implements ActionListener<Response> {

        private final StoppableSemaphore semaphore;
        private final CountDownLatch latch;
        private final CopyOnWriteArrayList<String> errorMessages;

        public BoundsManagingActionListener(StoppableSemaphore semaphore, CountDownLatch latch, CopyOnWriteArrayList<String> errorMessages) {
            this.semaphore = semaphore;
            this.latch = latch;
            this.errorMessages = errorMessages;
        }

        private void manage() {
            try {
                semaphore.release();
            } finally {
                latch.countDown();
            }
        }

        public void onResponse(Response response) {
            manage();
        }

        public void onFailure(Throwable e) {
            try {
                if (errorMessages.size() < 5) {
                    logger.debug("Failed to execute benchmark [{}]", e.getMessage(), e);
                    e = ExceptionsHelper.unwrapCause(e);
                    errorMessages.add(e.getLocalizedMessage());
                }
            } finally {
                manage();
            }
        }
    }

    private static class StatisticCollectionActionListener extends BoundsManagingActionListener<SearchResponse> {

        private final int bucketId;
        private final Measurements measurements;

        public StatisticCollectionActionListener(StoppableSemaphore semaphore, Measurements measurements,
                                                 int bucketId, CountDownLatch totalCount,
                                                 CopyOnWriteArrayList<String> errorMessages) {
            super(semaphore, totalCount, errorMessages);
            this.bucketId = bucketId;
            this.measurements = measurements;
        }

        @Override
        public void onResponse(SearchResponse searchResponse) {
            super.onResponse(searchResponse);
            measurements.timeBuckets()[bucketId] = searchResponse.getTookInMillis();
            if (searchResponse.getHits() != null) {
                measurements.docBuckets()[bucketId] = searchResponse.getHits().getTotalHits();
            }
        }

        @Override
        public void onFailure(Throwable e) {
            try {
                measurements.invalidate(bucketId);
            } finally {
                super.onFailure(e);
            }
        }
    }

    protected String nodeName() {
        return clusterService.localNode().name();
    }

    protected String nodeId() {
        return clusterService.localNode().id();
    }

    private final boolean assertBuckets(long[] buckets) {
        for (int i = 0; i < buckets.length; i++) {
            assert buckets[i] >= 0 : "Bucket value was negative: " + buckets[i] + " bucket id: " + i;
        }
        return true;
    }

    /**
     * Bind the originating request to the benchmark's search requests so that the headers and context are passed through.
     *
     * @param searchRequests    Benchmark search requests
     * @param originalRequest   Originating action request
     * @return                  Benchmark search requests re-constructed to use headers and context of originating request
     */
    private List<SearchRequest> bindOriginalRequest(final List<SearchRequest> searchRequests, final ActionRequest originalRequest) {
        final List<SearchRequest> newSearchRequests = new ArrayList<>(searchRequests.size());
        for (final SearchRequest searchRequest : searchRequests) {
            newSearchRequests.add(new SearchRequest(searchRequest, originalRequest));
        }
        return newSearchRequests;
    }
}
