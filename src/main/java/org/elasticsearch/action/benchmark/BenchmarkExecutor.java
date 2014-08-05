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

import com.google.common.collect.UnmodifiableIterator;
import org.apache.lucene.util.PriorityQueue;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.benchmark.start.*;
import org.elasticsearch.action.benchmark.status.*;
import org.elasticsearch.action.benchmark.competition.*;
import org.elasticsearch.action.benchmark.exception.*;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.*;
import java.util.concurrent.*;

/**
 * Handles execution, listing, pausing, and aborting of benchmarks
 */
public class BenchmarkExecutor {

    protected static final ESLogger logger = Loggers.getLogger(BenchmarkExecutor.class);

    private final Client           client;
    protected final ClusterService clusterService;
    private final Object           lock = new Object();

    private volatile ImmutableOpenMap<String, BenchmarkState> active = ImmutableOpenMap.of();

    public BenchmarkExecutor(Client client, ClusterService clusterService) {
        this.client         = client;
        this.clusterService = clusterService;
    }

    public void abort(String benchmarkId) {

        synchronized (lock) {
            final BenchmarkState state = active.get(benchmarkId);
            if (state == null) {
                throw new BenchmarkMissingException("No such benchmark [" + benchmarkId + "]");
            }

            try {
                state.abortAllCompetitors();
            } catch (Throwable t) {
                logger.error("benchmark [{}]: failed to abort", t, benchmarkId);
            }
        }
    }

    public void pause(String benchmarkId) {

        synchronized (lock) {
            final BenchmarkState state = active.get(benchmarkId);
            if (state == null) {
                throw new BenchmarkMissingException("No such benchmark [" + benchmarkId + "]");
            }

            try {
                state.pauseAllCompetitors();
            } catch (Throwable t) {
                logger.error("benchmark [{}]: failed to pause", t, benchmarkId);
            }
        }
    }

    public void resume(String benchmarkId) {

        synchronized (lock) {
            final BenchmarkState state = active.get(benchmarkId);
            if (state == null) {
                throw new BenchmarkMissingException("No such benchmark [" + benchmarkId + "]");
            }

            try {
                state.resumeAllCompetitors();
            } catch (Throwable t) {
                logger.error("benchmark [{}]: failed to resume", t, benchmarkId);
            }
        }
    }

    public BenchmarkStatusNodeActionResponse status(String benchmarkId) {

        final BenchmarkStatusNodeActionResponse response = new BenchmarkStatusNodeActionResponse(benchmarkId, nodeId());

        final BenchmarkState state = active.get(benchmarkId);
        if (state == null) {
            throw new BenchmarkMissingException("No such benchmark [" + benchmarkId + "]");
        }

        response.response(state.response);
        return response;
    }

    public List<BenchmarkStatusNodeActionResponse> status() {

        final List<BenchmarkStatusNodeActionResponse> responses = new ArrayList<>();
        synchronized (lock) {
            final UnmodifiableIterator<String> iter = active.keysIt();
            while (iter.hasNext()) {
                responses.add(status(iter.next()));
            }
        }

        return responses;
    }

    public void clear(String benchmarkId) {
        synchronized (lock) {
            active = ImmutableOpenMap.builder(active).fRemove(benchmarkId).build();
        }
    }

    public BenchmarkStatusNodeActionResponse create(BenchmarkStartRequest request) {

        synchronized (lock) {
            if (active.containsKey(request.benchmarkId())) {
                throw new BenchmarkIdConflictException(
                        "benchmark [" + request.benchmarkId() + "]: already executing on node [" + nodeName() + "]");
            }

            final BenchmarkStartResponse bsr = new BenchmarkStartResponse(request.benchmarkId(), new HashMap<String, CompetitionResult>());
            bsr.state(BenchmarkStartResponse.State.RUNNING);
            bsr.verbose(request.verbose());

            active = ImmutableOpenMap.builder(active).fPut(request.benchmarkId(), new BenchmarkState(request, bsr)).build();

            final BenchmarkStatusNodeActionResponse response = new BenchmarkStatusNodeActionResponse(request.benchmarkId(), nodeId());
            response.response(bsr);
            return response;
        }
    }

    public BenchmarkStatusNodeActionResponse start(BenchmarkStartRequest request) throws ElasticsearchException {

        final BenchmarkState state = active.get(request.benchmarkId());
        if (state == null) {
            throw new BenchmarkMissingException("No such benchmark [" + request.benchmarkId() + "]");
        }

        final BenchmarkStartResponse benchmarkStartResponse = state.response;

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

                final List<SearchRequest> searchRequests = competitor.settings().searchRequests();

                // Perform warmup if requested
                if (settings.warmup()) {
                    final long start = System.nanoTime();
                    final List<String> errors = warmup(searchRequests, state.competitorSemaphore(competitor.name()));
                    competitionNodeResult.warmUpTime(TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS));
                    if (!errors.isEmpty()) {
                        throw new BenchmarkExecutionException("Failed to execute warmup phase", errors);
                    }
                }

                final int numMeasurements = settings.multiplier() * searchRequests.size();
                final long[] timeBuckets  = new long[numMeasurements];
                final long[] docBuckets   = new long[numMeasurements];

                for (int i = 0; i < settings.iterations(); i++) {
                    /*
                    if (settings.allowCacheClearing() && settings.clearCaches() != null) {
                        try {
                            client.admin().indices().clearCache(settings.clearCaches()).get();
                        } catch (ExecutionException e) {
                            throw new BenchmarkExecutionException("Failed to clear caches", e);
                        }
                    }
                    */
                    // Run the iteration
                    CompetitionIteration ci =
                            iterate(request.benchmarkId(),competitor, searchRequests, timeBuckets,
                                    docBuckets, state.competitorSemaphore(competitor.name()));
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
            synchronized (lock) {
                state.stopAllCompetitors();
            }
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

    protected CompetitionIteration iterate(final String benchmarkId, final BenchmarkCompetitor competitor,
                                           final List<SearchRequest> searchRequests,
                                           final long[] timeBuckets, final long[] docBuckets,
                                           final StoppableSemaphore semaphore) throws InterruptedException {

        assert timeBuckets.length == competitor.settings().multiplier() * searchRequests.size();
        assert docBuckets.length == competitor.settings().multiplier() * searchRequests.size();

        Arrays.fill(timeBuckets, -1);   // wipe CPU cache     ;)
        Arrays.fill(docBuckets, -1);    // wipe CPU cache     ;)

        int id = 0;
        final CountDownLatch totalCount = new CountDownLatch(timeBuckets.length);
        final CopyOnWriteArrayList<String> errorMessages = new CopyOnWriteArrayList<>();
        final long start = System.nanoTime();

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
        final long totalTime = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);

        if (!errorMessages.isEmpty()) {
            throw new BenchmarkExecutionException("Too many execution failures", errorMessages);
        }

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
                manage();
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
}
