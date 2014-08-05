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

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.benchmark.competition.CompetitionIteration;
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
 *
 */
public class MockBenchmarkExecutorService extends BenchmarkExecutorService {

    @Inject
    public MockBenchmarkExecutorService(Settings settings, ClusterService clusterService, ThreadPool threadPool,
                                        Client client, TransportService transportService) {
        super(settings, clusterService, threadPool, transportService, new MockBenchmarkExecutor(client, clusterService));
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

        private Map<String, FlowControl> flows = new ConcurrentHashMap<>();

        public MockBenchmarkExecutor(final Client client, final ClusterService clusterService) {
            super(client, clusterService);
        }

        public void addFlowControl(final String benchmarkId, final FlowControl flow) {
            if (flows.containsKey(benchmarkId)) {
                throw new ElasticsearchIllegalStateException("Already have flow control for benchmark: " + benchmarkId);
            }
            flows.put(benchmarkId, flow);
        }

        static final class FlowControl {
            String        competitor;
            int           current;
            int           iteration;
            Semaphore     control;
            CyclicBarrier initialization;

            FlowControl(final String competitor, final int iteration, final Semaphore control, final CyclicBarrier initialization) {
                this.competitor     = competitor;
                this.iteration      = iteration;
                this.control        = control;
                this.initialization = initialization;
            }

            void acquire() throws InterruptedException {
                if (current == iteration) {
                    control.acquire();
                }
            }

            void release() {
                control.release();
            }

            void clear() {
                if (initialization != null) {
                    initialization.reset();
                    initialization = null;
                }
                if (control != null) {
                    control.release();
                    control = null;
                }
            }
        }

        public void clearMockState() {
            for (Map.Entry<String, FlowControl> me : flows.entrySet()) {
                if (me.getValue() != null) {
                    me.getValue().clear();
                }
            }
            flows.clear();
        }

        protected CompetitionIteration iterate(final String benchmarkId, final BenchmarkCompetitor competitor,
                                               final List<SearchRequest> searchRequests,
                                               final long[] timeBuckets, final long[] docBuckets,
                                               final StoppableSemaphore semaphore) throws InterruptedException {

            final FlowControl flow = flows.get(benchmarkId);

            if (flow != null) {
                if (flow.current == 0 && flow.initialization != null) {
                    try {
                        flow.initialization.await();
                        logger.debug("benchmark [{}] passed initialization barrier on node [{}]", benchmarkId, clusterService.localNode().name());
                    } catch (BrokenBarrierException e) {
                        flow.clear();
                        throw new RuntimeException("Failed to wait for shared initialization", e);
                    }
                }

                if (flow.competitor.equals(competitor.name())) {
                    flow.acquire();
                }

                flow.current++;
            }

            return super.iterate(benchmarkId, competitor, searchRequests, timeBuckets, docBuckets, semaphore);
        }
    }
}
