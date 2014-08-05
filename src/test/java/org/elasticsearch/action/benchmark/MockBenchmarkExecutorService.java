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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.benchmark.competition.CompetitionIteration;
import org.elasticsearch.action.benchmark.start.BenchmarkStartRequest;
import org.elasticsearch.action.benchmark.status.BenchmarkStatusNodeActionResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;

/**
 * Mock benchmark executor for testing
 */
public final class MockBenchmarkExecutorService extends BenchmarkExecutorService {

    @Inject
    public MockBenchmarkExecutorService(Settings settings, ClusterService clusterService, 
                                        ThreadPool threadPool, Client client, 
                                        TransportService transportService) {
        super(settings, clusterService, threadPool, 
                transportService, new MockBenchmarkExecutor(client, clusterService));
    }

    public MockBenchmarkExecutor executor() {
        return (MockBenchmarkExecutor) executor;
    }

    public void clearMockState() {
        executor().clearMockState();
    }

    /**
     * Hook for testing via a mock executor
     */
    static final class MockBenchmarkExecutor extends BenchmarkExecutor {

        FlowControl control = null;

        public MockBenchmarkExecutor(final Client client, final ClusterService clusterService) {
            super(client, clusterService);
        }

        public void control(final FlowControl control) {
            this.control = control;
        }

        public FlowControl control() {
            return control;
        }
        
        public void clear() {
            control = null;
        }

        static final class FlowControl {

            final CyclicBarrier initializationBarrier;
            final Semaphore     controlSemaphore;
            final String        competition;

            FlowControl(final CyclicBarrier initializationBarrier, final String competition, final Semaphore controlSemaphore) {

                this.initializationBarrier = initializationBarrier;
                this.competition = competition;
                this.controlSemaphore = controlSemaphore;
            }
        }

        public void clearMockState() {
            if (control != null) {
                control.initializationBarrier.reset();
                control.controlSemaphore.release();
                control = null;
            }
        }

        @Override
        public BenchmarkStatusNodeActionResponse start(BenchmarkStartRequest request)
            throws ElasticsearchException {

            if (control != null) {
                try {
                    control.initializationBarrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    logger.error("--> Failed to wait on initialization barrier: {}", 
                            e, e.getMessage());
                }
            }

            return super.start(request);
        }

        protected CompetitionIteration iterate(final String benchmarkId, final BenchmarkCompetitor competitor,
                                               final List<SearchRequest> searchRequests, final long[] timeBuckets, final long[] docBuckets,
                                               final StoppableSemaphore semaphore) 
            throws InterruptedException {

            CompetitionIteration ci = null;

            try {
                if (control != null && control.competition.equals(competitor.name())) {
                    logger.info("--> Acquiring iteration semaphore: [{}] [{}]", 
                            control.controlSemaphore, clusterService.localNode().name());
                    control.controlSemaphore.acquire();
                }
                ci = super.iterate(benchmarkId, competitor, searchRequests, timeBuckets, docBuckets, semaphore);
            }
            finally {
                if (control != null && control.competition.equals(competitor.name())) {
                    control.controlSemaphore.release();
                    logger.info("--> Released iteration semaphore: [{}] [{}]", 
                            control.controlSemaphore, clusterService.localNode().name());
                }
            }

            return ci;
        }
    }
}
