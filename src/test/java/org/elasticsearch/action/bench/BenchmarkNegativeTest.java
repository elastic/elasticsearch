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

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import org.junit.Test;

import static org.hamcrest.Matchers.*;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.*;

/**
 * Tests for negative situations where we cannot run benchmarks
 */
@ClusterScope(scope = Scope.SUITE, enableRandomBenchNodes = false)
public class BenchmarkNegativeTest extends ElasticsearchIntegrationTest {

    private static final String INDEX_NAME = "test_index";

    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder().put(super.nodeSettings(nodeOrdinal))
                .put("node.bench", false).build();
    }

    @Test(expected = BenchmarkNodeMissingException.class)
    public void testSubmitBenchmarkNegative() {
        client().bench(BenchmarkTestUtil.randomRequest(
                client(), new String[] {INDEX_NAME}, internalCluster().size(), null)).actionGet();
    }

    public void testListBenchmarkNegative() {
        final BenchmarkStatusResponse response =
                client().prepareBenchStatus().execute().actionGet();
        assertThat(response.benchmarkResponses().size(), equalTo(0));
    }

    @Test(expected = BenchmarkNodeMissingException.class)
    public void testAbortBenchmarkNegative() throws Exception {
        client().prepareAbortBench(BenchmarkTestUtil.BENCHMARK_NAME).execute().actionGet();
    }
}
