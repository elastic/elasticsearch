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

import org.elasticsearch.action.benchmark.competition.CompetitionIteration;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;

/**
 *
 */
public class MockBenchmarkExecutorService extends BenchmarkExecutorService {

    @Inject
    public MockBenchmarkExecutorService(Settings settings, ClusterService clusterService, ThreadPool threadPool,
                                        Client client, TransportService transportService, BenchmarkUtility utility) {
        super(settings, clusterService, threadPool, transportService, new MockBenchmarkExecutor(client, clusterService), utility);
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

        FlowControl   flow;
        int           currentIteration = 0;
        CyclicBarrier initializationBarrier;

        public MockBenchmarkExecutor(Client client, ClusterService clusterService) {
            super(client, clusterService);
        }

        static final class FlowControl {
            String    competitor;
            int       iteration;
            Semaphore control;

            FlowControl(String competitor, int iteration, Semaphore control) {
                this.competitor = competitor;
                this.iteration  = iteration;
                this.control    = control;
            }

            void acquire(int current) throws InterruptedException {
                if (current == iteration) {
                    control.acquire();
                }
            }

            void release() {
                control.release();
            }
        }

        public void clearMockState() {
            if (flow != null) {
                flow.control.release();
                flow.control = null;
                flow = null;
            }
            currentIteration = 0;
            initializationBarrier = null;
        }

        protected CompetitionIteration iterate(BenchmarkCompetitor competitor, List<SearchRequest> searchRequests,
                                               final long[] timeBuckets, final long[] docBuckets,
                                               StoppableSemaphore semaphore) throws InterruptedException {

            if (currentIteration == 0 && initializationBarrier != null) {
                try {
                    initializationBarrier.await();
                } catch (BrokenBarrierException e) {
                    throw new RuntimeException("Failed to wait for shared initialization", e);
                }
            }

            if (flow != null) {
                if (flow.competitor.equals(competitor.name())) {
                    flow.acquire(currentIteration);
                }
            }

            currentIteration++;
            return super.iterate(competitor, searchRequests, timeBuckets, docBuckets, semaphore);
        }
    }
}
