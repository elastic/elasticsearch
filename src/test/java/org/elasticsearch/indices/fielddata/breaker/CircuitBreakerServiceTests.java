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

package org.elasticsearch.indices.fielddata.breaker;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.util.Arrays;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Integration tests for InternalCircuitBreakerService
 */
@ClusterScope(scope = TEST, randomDynamicTemplates = false)
public class CircuitBreakerServiceTests extends ElasticsearchIntegrationTest {

    private String randomRidiculouslySmallLimit() {
        // 3 different ways to say 100 bytes
        return randomFrom(Arrays.asList("100b", "100"));
         //, (10000. / JvmInfo.jvmInfo().getMem().getHeapMax().bytes()) + "%")); // this is prone to rounding errors and will fail if JVM memory changes!
    }

    @Test
    @TestLogging("org.elasticsearch.indices.fielddata.breaker:TRACE,org.elasticsearch.index.fielddata:TRACE,org.elasticsearch.common.breaker:TRACE")
    public void testMemoryBreaker() {
        assertAcked(prepareCreate("cb-test", 1, settingsBuilder().put(SETTING_NUMBER_OF_REPLICAS, between(0, 1))));
        final Client client = client();

        try {
            // index some different terms so we have some field data for loading
            int docCount = scaledRandomIntBetween(300, 1000);
            for (long id = 0; id < docCount; id++) {
                client.prepareIndex("cb-test", "type", Long.toString(id))
                        .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + id).map()).execute().actionGet();
            }

            // refresh
            refresh();

            // execute a search that loads field data (sorting on the "test" field)
            client.prepareSearch("cb-test").setSource("{\"sort\": \"test\",\"query\":{\"match_all\":{}}}")
                    .execute().actionGet();

            // clear field data cache (thus setting the loaded field data back to 0)
            client.admin().indices().prepareClearCache("cb-test").setFieldDataCache(true).execute().actionGet();

            // Update circuit breaker settings
            Settings settings = settingsBuilder()
                    .put(InternalCircuitBreakerService.CIRCUIT_BREAKER_MAX_BYTES_SETTING, randomRidiculouslySmallLimit())
                    .put(InternalCircuitBreakerService.CIRCUIT_BREAKER_OVERHEAD_SETTING, 1.05)
                    .build();
            client.admin().cluster().prepareUpdateSettings().setTransientSettings(settings).execute().actionGet();

            // execute a search that loads field data (sorting on the "test" field)
            // again, this time it should trip the breaker
            assertFailures(client.prepareSearch("cb-test").setSource("{\"sort\": \"test\",\"query\":{\"match_all\":{}}}"),
                        RestStatus.INTERNAL_SERVER_ERROR,
                    containsString("Data too large, data for field [test] would be larger than limit of [100/100b]"));

            NodesStatsResponse stats = client.admin().cluster().prepareNodesStats().setBreaker(true).get();
            int breaks = 0;
            for (NodeStats stat : stats.getNodes()) {
                FieldDataBreakerStats breakerStats = stat.getBreaker();
                breaks += breakerStats.getTrippedCount();
            }
            assertThat(breaks, greaterThanOrEqualTo(1));
        } finally {
            // Reset settings
            Settings resetSettings = settingsBuilder()
                    .put(InternalCircuitBreakerService.CIRCUIT_BREAKER_MAX_BYTES_SETTING, "-1")
                    .put(InternalCircuitBreakerService.CIRCUIT_BREAKER_OVERHEAD_SETTING, InternalCircuitBreakerService.DEFAULT_OVERHEAD_CONSTANT)
                    .build();
            client.admin().cluster().prepareUpdateSettings().setTransientSettings(resetSettings).execute().actionGet();
        }
    }

    @Test
    @TestLogging("org.elasticsearch.indices.fielddata.breaker:TRACE,org.elasticsearch.index.fielddata:TRACE,org.elasticsearch.common.breaker:TRACE")
    public void testRamAccountingTermsEnum() {
        final Client client = client();

        try {
            // Create an index where the mappings have a field data filter
            assertAcked(prepareCreate("ramtest").setSource("{\"mappings\": {\"type\": {\"properties\": {\"test\": " +
                    "{\"type\": \"string\",\"fielddata\": {\"filter\": {\"regex\": {\"pattern\": \"^value.*\"}}}}}}}}"));

            // Wait 10 seconds for green
            client.admin().cluster().prepareHealth("ramtest").setWaitForGreenStatus().setTimeout("10s").execute().actionGet();

            // index some different terms so we have some field data for loading
            int docCount = scaledRandomIntBetween(300, 1000);
            for (long id = 0; id < docCount; id++) {
                client.prepareIndex("ramtest", "type", Long.toString(id))
                        .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + id).map()).execute().actionGet();
            }

            // refresh
            refresh();

            // execute a search that loads field data (sorting on the "test" field)
            client.prepareSearch("ramtest").setSource("{\"sort\": \"test\",\"query\":{\"match_all\":{}}}")
                    .execute().actionGet();

            // clear field data cache (thus setting the loaded field data back to 0)
            client.admin().indices().prepareClearCache("ramtest").setFieldDataCache(true).execute().actionGet();

            // Update circuit breaker settings
            Settings settings = settingsBuilder()
                    .put(InternalCircuitBreakerService.CIRCUIT_BREAKER_MAX_BYTES_SETTING, randomRidiculouslySmallLimit())
                    .put(InternalCircuitBreakerService.CIRCUIT_BREAKER_OVERHEAD_SETTING, 1.05)
                    .build();
            client.admin().cluster().prepareUpdateSettings().setTransientSettings(settings).execute().actionGet();

            // execute a search that loads field data (sorting on the "test" field)
            // again, this time it should trip the breaker
            assertFailures(client.prepareSearch("ramtest").setSource("{\"sort\": \"test\",\"query\":{\"match_all\":{}}}"),
                        RestStatus.INTERNAL_SERVER_ERROR,
                    containsString("Data too large, data for field [test] would be larger than limit of [100/100b]"));

            NodesStatsResponse stats = client.admin().cluster().prepareNodesStats().setBreaker(true).get();
            int breaks = 0;
            for (NodeStats stat : stats.getNodes()) {
                FieldDataBreakerStats breakerStats = stat.getBreaker();
                breaks += breakerStats.getTrippedCount();
            }
            assertThat(breaks, greaterThanOrEqualTo(1));

        } finally {
            // Reset settings
            Settings resetSettings = settingsBuilder()
                    .put(InternalCircuitBreakerService.CIRCUIT_BREAKER_MAX_BYTES_SETTING, "-1")
                    .put(InternalCircuitBreakerService.CIRCUIT_BREAKER_OVERHEAD_SETTING, InternalCircuitBreakerService.DEFAULT_OVERHEAD_CONSTANT)
                    .build();
            client.admin().cluster().prepareUpdateSettings().setTransientSettings(resetSettings).execute().actionGet();
        }
    }
}
