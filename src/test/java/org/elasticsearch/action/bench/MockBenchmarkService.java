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

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;


public class MockBenchmarkService extends BenchmarkService {

    @Inject
    public MockBenchmarkService(Settings settings, ClusterService clusterService, ThreadPool threadPool,
                                Client client, TransportService transportService) {
        super(settings, clusterService, threadPool, client, transportService,
                new MockBenchmarkExecutor(client, clusterService));
    }

    public void pauseSubmissions() throws InterruptedException {
        executor.obtainSubmissionControl();
    }

    public void resumeSubmissions() {
        executor.releaseSubmissionControl();
    }

    private static class MockBenchmarkExecutor extends BenchmarkExecutor {

        public MockBenchmarkExecutor(Client client, ClusterService clusterService) {
            super(client, clusterService);
        }

        @Override
        protected void obtainSubmissionControl() throws InterruptedException {
            submissionControl.acquire();
        }

        @Override
        protected void releaseSubmissionControl() {
            submissionControl.release();
        }
    }
}