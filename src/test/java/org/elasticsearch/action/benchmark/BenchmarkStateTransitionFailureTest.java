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

import org.elasticsearch.action.benchmark.start.BenchmarkStartRequest;
import org.elasticsearch.cluster.metadata.BenchmarkMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

/**
 * Test failures that happen during state transitions
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class BenchmarkStateTransitionFailureTest extends AbstractBenchmarkTest {

    protected synchronized Settings nodeSettings(int ordinal) {
        return ImmutableSettings.builder().put("node.bench",
                ordinal == 0 || randomBoolean()).
                put(BenchmarkModule.BENCHMARK_COORDINATOR_SERVICE_KEY, MockBenchmarkCoordinatorService.class).
                put(BenchmarkModule.BENCHMARK_EXECUTOR_SERVICE_KEY, MockBenchmarkExecutorService.class).
                build();
    }

    @Test
    public void testOnStateUpdateFailure() throws Exception {

        MockBenchmarkCoordinatorService coordinator = mockCoordinatorService();
        coordinator.mockBenchmarkStateManager().forceFailureOnUpdate = true;

        final BenchmarkStartRequest request = BenchmarkTestUtil.randomRequest(client(), indices, numExecutorNodes, competitionSettingsMap);

        try {
            client().startBenchmark(request).actionGet();
            fail("Expected to fail");
        } catch (Throwable t) {
            final BenchmarkMetaData meta = clusterService().state().metaData().custom(BenchmarkMetaData.TYPE);
            if (meta != null) {
                assertFalse("Failed to clear metadata", meta.contains(request.benchmarkId()));  // Should leave no trace in cluster metadata
            }
        }
    }
}
