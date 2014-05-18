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
package org.elasticsearch.action.bench;

import org.apache.lucene.util.PriorityQueue;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.google.common.collect.UnmodifiableIterator;

import java.util.*;
import java.util.concurrent.*;

/**
 * Handles execution, listing, and aborting of benchmarks
 */
public class BenchmarkExecutor {

    protected static final ESLogger logger = Loggers.getLogger(BenchmarkExecutor.class);

    private final Client client;
    private String nodeName;
    private final ClusterService clusterService;
    private volatile ImmutableOpenMap<String, BenchmarkState> activeBenchmarks = ImmutableOpenMap.of();
    private final Object lock = new Object();
    protected final Semaphore submissionControl = new Semaphore(1);


    public BenchmarkExecutor(Client client, ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
    }

    private static class BenchmarkState {

        final BenchmarkRequest request;
        final BenchmarkResponse response;
        final Map<String, StoppableSemaphore> warmupSemaphore;
        final Map<String, StoppableSemaphore> iterationSemaphore;

        BenchmarkState(BenchmarkRequest request, BenchmarkResponse response) {

            this.request = request;
            this.response = response;
            warmupSemaphore = new HashMap<>();
            iterationSemaphore = new HashMap<>();

            // Pre-allocate per-competitor semaphores so that we can pause/resume executions
            // without regard to where in its lifecycle a competition is.
            for (BenchmarkCompetitor competitor : request.competitors()) {
                warmupSemaphore.put(competitor.name(), new StoppableSemaphore(competitor.settings().concurrency()));
                iterationSemaphore.put(competitor.name(), new StoppableSemaphore(competitor.settings().concurrency()));
            }
        }

        StoppableSemaphore competitionWarmUpSemaphore(String competitionName) {
            final StoppableSemaphore semaphore = warmupSemaphore.get(competitionName);
            assert semaphore != null;
            return semaphore;
        }

        StoppableSemaphore competitionIterationSemaphore(String competitionName) {
            final StoppableSemaphore semaphore = iterationSemaphore.get(competitionName);
            assert semaphore != null;
            return semaphore;
        }

        synchronized void stopAllCompetitors() {
            for (BenchmarkCompetitor competitor : request.competitors()) {
                competitionWarmUpSemaphore(competitor.name()).stop();
                competitionIterationSemaphore(competitor.name()).stop();
            }
        }

        synchronized void pauseAllCompetitors() throws InterruptedException {
            for (BenchmarkCompetitor competitor : request.competitors()) {
                competitionWarmUpSemaphore(competitor.name()).pause();
                competitionIterationSemaphore(competitor.name()).pause();
            }
        }

        synchronized void resumeAllCompetitors() {
            for (BenchmarkCompetitor competitor : request.competitors()) {
                competitionWarmUpSemaphore(competitor.name()).resume();
                competitionIterationSemaphore(competitor.name()).resume();
            }
        }

        boolean anyCompetitorPaused() {
            for (BenchmarkCompetitor competitor : request.competitors()) {
                if (competitionWarmUpSemaphore(competitor.name()).paused ||
                    competitionIterationSemaphore(competitor.name()).paused) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Aborts a benchmark with the given id
     *
     * @param benchmarkName The benchmark to abort
     * @return              Abort response
     */
    public AbortBenchmarkNodeResponse abortBenchmark(String benchmarkName) {

        BenchmarkState state = activeBenchmarks.get(benchmarkName);
        if (state == null) {
            throw new BenchmarkMissingException("Benchmark [" + benchmarkName + "] not found on [" + nodeName() + "]");
        }
        state.stopAllCompetitors();
        activeBenchmarks = ImmutableOpenMap.builder(activeBenchmarks).fRemove(benchmarkName).build();
        logger.debug("Aborted benchmark [{}] on [{}]", benchmarkName, nodeName());
        return new AbortBenchmarkNodeResponse(benchmarkName, nodeName());
    }

    /**
     * Reports status of all active benchmarks
     *
     * @return  Benchmark status response
     */
    public BenchmarkStatusNodeResponse benchmarkStatus() {

        int totalActive = 0;
        int totalPaused = 0;

        BenchmarkStatusNodeResponse response = new BenchmarkStatusNodeResponse();
        final ImmutableOpenMap<String, BenchmarkState> activeBenchmarks = this.activeBenchmarks;
        UnmodifiableIterator<String> iter = activeBenchmarks.keysIt();
        while (iter.hasNext()) {
            String id = iter.next();
            BenchmarkState state = activeBenchmarks.get(id);
            response.addBenchResponse(state.response);

            if (state.anyCompetitorPaused()) {
                totalPaused++;
            } else {
                totalActive++;
            }
        }

        logger.debug("Reporting [{}] active, [{}] paused benchmarks on [{}]", totalActive, totalPaused, nodeName());
        return response;
    }

    /**
     * Pauses the execution of the named benchmark
     * @param benchmarkName     The benchmark to pause
     * @return                  Node status
     */
    public BenchmarkStatusNodeResponse pauseBenchmark(String benchmarkName) {

        final BenchmarkState state = benchmarkState(benchmarkName);
        try {
            state.pauseAllCompetitors();
        } catch (InterruptedException e) {
            throw new BenchmarkExecutionException("Unable to pause [" + benchmarkName + "] on [" + nodeName() + "]", e);
        }
        state.response.state(BenchmarkResponse.State.PAUSED);
        BenchmarkStatusNodeResponse response = new BenchmarkStatusNodeResponse();
        response.addBenchResponse(state.response);

        logger.debug("Pausing benchmark [{}] on [{}]", benchmarkName, nodeName());
        return response;
    }

    /**
     * Resumes execution of the named benchmark
     * @param benchmarkName     The benchmark to resume
     * @return                  Node status
     */
    public BenchmarkStatusNodeResponse resumeBenchmark(String benchmarkName) {

        final BenchmarkState state = benchmarkState(benchmarkName);
        state.resumeAllCompetitors();
        state.response.state(BenchmarkResponse.State.RUNNING);
        BenchmarkStatusNodeResponse response = new BenchmarkStatusNodeResponse();
        response.addBenchResponse(state.response);

        logger.debug("Resuming benchmark [{}] on [{}]", benchmarkName, nodeName());
        return response;
    }

    private BenchmarkState benchmarkState(String benchmarkName) {
        final BenchmarkState state = activeBenchmarks.get(benchmarkName);
        if (state == null) {
            throw new BenchmarkMissingException("Benchmark [" + benchmarkName + "] not found on [" + nodeName() + "]");
        }
        return state;
    }

    // Sub-class hook for mock implementations to control submissions
    protected void obtainSubmissionControl() throws InterruptedException { }

    // Sub-class hook for mock implementations to control submissions
    protected void releaseSubmissionControl() { }

    /**
     * Submits a search benchmark for execution
     *
     * @param request                   A benchmark request
     * @return                          Summary response of executed benchmark
     * @throws ElasticsearchException
     */
    public BenchmarkResponse benchmark(BenchmarkRequest request) throws ElasticsearchException {

        final Map<String, CompetitionResult> competitionResults = new HashMap<>();
        final BenchmarkResponse benchmarkResponse = new BenchmarkResponse(request.benchmarkName(), competitionResults);
        final BenchmarkState state = new BenchmarkState(request, benchmarkResponse);

        synchronized (lock) {
            if (activeBenchmarks.containsKey(request.benchmarkName())) {
                throw new ElasticsearchException("Benchmark [" + request.benchmarkName() + "] is already running on [" + nodeName() + "]");
            }
            activeBenchmarks = ImmutableOpenMap.builder(activeBenchmarks).fPut(request.benchmarkName(), state).build();
        }

        try {
            obtainSubmissionControl();  // No-op unless sub-classed

            for (BenchmarkCompetitor competitor : request.competitors()) {

                final BenchmarkSettings settings = competitor.settings();
                final int iterations = settings.iterations();
                logger.debug("Executing [iterations: {}] [multiplier: {}] for [{}] on [{}]",
                        iterations, settings.multiplier(), request.benchmarkName(), nodeName());

                final List<CompetitionIteration> competitionIterations = new CopyOnWriteArrayList<>();
                final CompetitionResult competitionResult =
                        new CompetitionResult(competitor.name(), settings.concurrency(), settings.multiplier(), request.percentiles());
                final CompetitionNodeResult competitionNodeResult =
                        new CompetitionNodeResult(competitor.name(), nodeName(), iterations, competitionIterations);

                competitionResult.addCompetitionNodeResult(competitionNodeResult);
                benchmarkResponse.competitionResults.put(competitor.name(), competitionResult);

                final List<SearchRequest> searchRequests = competitor.settings().searchRequests();

                if (settings.warmup()) {
                    final long beforeWarmup = System.nanoTime();
                    final List<String> warmUpErrors =
                            warmUp(searchRequests, state.competitionWarmUpSemaphore(competitor.name()));
                    final long afterWarmup = System.nanoTime();
                    competitionNodeResult.warmUpTime(TimeUnit.MILLISECONDS.convert(afterWarmup - beforeWarmup, TimeUnit.NANOSECONDS));
                    if (!warmUpErrors.isEmpty()) {
                        throw new BenchmarkExecutionException("Failed to execute warmup phase", warmUpErrors);
                    }
                    state.competitionWarmUpSemaphore(competitor.name()).stop();
                }

                final int numMeasurements = settings.multiplier() * searchRequests.size();
                final long[] timeBuckets = new long[numMeasurements];
                final long[] docBuckets = new long[numMeasurements];

                for (int i = 0; i < iterations; i++) {
                    if (settings.allowCacheClearing() && settings.clearCaches() != null) {
                        try {
                            client.admin().indices().clearCache(settings.clearCaches()).get();
                        } catch (ExecutionException e) {
                            throw new BenchmarkExecutionException("Failed to clear caches", e);
                        }
                    }

                    // Run the iteration
                    CompetitionIteration ci =
                            runIteration(competitor, searchRequests, timeBuckets, docBuckets,
                                    state.competitionIterationSemaphore(competitor.name()));
                    ci.percentiles(request.percentiles());
                    competitionIterations.add(ci);
                    competitionNodeResult.incrementCompletedIterations();
                }

                competitionNodeResult.totalExecutedQueries(settings.multiplier() * searchRequests.size() * iterations);
                state.competitionIterationSemaphore(competitor.name()).stop();
            }

            benchmarkResponse.state(BenchmarkResponse.State.COMPLETE);

        } catch (BenchmarkExecutionException e) {
            benchmarkResponse.state(BenchmarkResponse.State.FAILED);
            benchmarkResponse.errors(e.errorMessages().toArray(new String[e.errorMessages().size()]));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            benchmarkResponse.state(BenchmarkResponse.State.ABORTED);
        } catch (Throwable ex) {
            logger.debug("Unexpected exception during benchmark", ex);
            benchmarkResponse.state(BenchmarkResponse.State.FAILED);
            benchmarkResponse.errors(ex.getMessage());
        } finally {
            synchronized (lock) {
                activeBenchmarks = ImmutableOpenMap.builder(activeBenchmarks).fRemove(request.benchmarkName()).build();
            }
        }

        return benchmarkResponse;
    }

    private List<String> warmUp(List<SearchRequest> searchRequests, StoppableSemaphore semaphore)
            throws InterruptedException {

        final CountDownLatch totalCount = new CountDownLatch(searchRequests.size());
        final CopyOnWriteArrayList<String> errorMessages = new CopyOnWriteArrayList<>();

        for (SearchRequest searchRequest : searchRequests) {
            semaphore.acquire();
            client.search(searchRequest,
                    new BoundsManagingActionListener<SearchResponse>(semaphore, totalCount, errorMessages) { } );
        }
        totalCount.await();
        return errorMessages;
    }

    private CompetitionIteration runIteration(BenchmarkCompetitor competitor, List<SearchRequest> searchRequests,
                                              final long[] timeBuckets, final long[] docBuckets,
                                              StoppableSemaphore semaphore) throws InterruptedException {

        assert timeBuckets.length == competitor.settings().multiplier() * searchRequests.size();
        assert docBuckets.length == competitor.settings().multiplier() * searchRequests.size();

        Arrays.fill(timeBuckets, -1);   // wipe CPU cache     ;)
        Arrays.fill(docBuckets, -1);    // wipe CPU cache     ;)

        int id = 0;
        final CountDownLatch totalCount = new CountDownLatch(timeBuckets.length);
        final CopyOnWriteArrayList<String> errorMessages = new CopyOnWriteArrayList<>();
        final long beforeRun = System.nanoTime();

        for (int i = 0; i < competitor.settings().multiplier(); i++) {
            for (SearchRequest searchRequest : searchRequests) {
                StatisticCollectionActionListener statsListener =
                        new StatisticCollectionActionListener(semaphore, timeBuckets, docBuckets, id++, totalCount, errorMessages);
                semaphore.acquire();
                client.search(searchRequest, statsListener);
            }
        }
        totalCount.await();
        assert id == timeBuckets.length;
        final long afterRun = System.nanoTime();
        if (!errorMessages.isEmpty()) {
            throw new BenchmarkExecutionException("Too many execution failures", errorMessages);
        }

        final long totalTime = TimeUnit.MILLISECONDS.convert(afterRun - beforeRun, TimeUnit.NANOSECONDS);

        CompetitionIterationData iterationData = new CompetitionIterationData(timeBuckets);
        long sumDocs = new CompetitionIterationData(docBuckets).sum();

        // Don't track slowest request if there is only one request as that is redundant
        CompetitionIteration.SlowRequest[] topN = null;
        if ((competitor.settings().numSlowest() > 0) && (searchRequests.size() > 1)) {
            topN = getTopN(timeBuckets, searchRequests, competitor.settings().multiplier(), competitor.settings().numSlowest());
        }

        CompetitionIteration round =
                new CompetitionIteration(topN, totalTime, timeBuckets.length, sumDocs, iterationData);
        return round;
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
            } else if (topNQueue.top().avgTime < max) {
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

    private static abstract class BoundsManagingActionListener<Response> implements ActionListener<Response> {

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
                manage(); // first add the msg then call the count down on the latch otherwise we might iss one error
            }
        }
    }

    private static class StatisticCollectionActionListener extends BoundsManagingActionListener<SearchResponse> {

        private final long[] timeBuckets;
        private final int bucketId;
        private final long[] docBuckets;

        public StatisticCollectionActionListener(StoppableSemaphore semaphore, long[] timeBuckets, long[] docs,
                                                 int bucketId, CountDownLatch totalCount,
                                                 CopyOnWriteArrayList<String> errorMessages) {
            super(semaphore, totalCount, errorMessages);
            this.bucketId = bucketId;
            this.timeBuckets = timeBuckets;
            this.docBuckets = docs;
        }

        @Override
        public void onResponse(SearchResponse searchResponse) {
            super.onResponse(searchResponse);
            timeBuckets[bucketId] = searchResponse.getTookInMillis();
            if (searchResponse.getHits() != null) {
                docBuckets[bucketId] = searchResponse.getHits().getTotalHits();
            }
        }

        @Override
        public void onFailure(Throwable e) {
            try {
                timeBuckets[bucketId] = -1;
                docBuckets[bucketId] = -1;
            } finally {
                super.onFailure(e);
            }

        }
    }

    private final static class StoppableSemaphore {
        private Semaphore semaphore;
        private final int concurrency;
        private volatile boolean stopped = false;
        private volatile boolean paused = false;

        public StoppableSemaphore(int concurrency) {
            semaphore = new Semaphore(concurrency);
            this.concurrency = concurrency;
        }

        public StoppableSemaphore reset(int concurrency) {
            semaphore = new Semaphore(concurrency);
            return this;
        }

        public void acquire() throws InterruptedException {
            if (stopped) {
                throw new InterruptedException("Benchmark Interrupted");
            }
            semaphore.acquire();
        }

        public void release() {
            semaphore.release();
        }

        public synchronized void stop() {
            stopped = true;
            if (paused) {
                resume();
            }
        }

        public synchronized void pause() throws InterruptedException {
            if (!stopped && !paused) {
                semaphore.acquire(concurrency);
                paused = true;
            }
        }

        public synchronized void resume() {
            if (paused) {
                paused = false;
                semaphore.release(concurrency);
            }
        }
    }

    protected String nodeName() {
        if (nodeName == null) {
            nodeName = clusterService.localNode().name();
        }
        return nodeName;
    }

    private final boolean assertBuckets(long[] buckets) {
        for (int i = 0; i < buckets.length; i++) {
            assert buckets[i] >= 0 : "Bucket value was negative: " + buckets[i] + " bucket id: " + i;
        }
        return true;
    }
}
