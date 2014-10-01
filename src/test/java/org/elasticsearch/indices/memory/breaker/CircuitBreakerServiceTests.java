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

package org.elasticsearch.indices.memory.breaker;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.util.Arrays;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.cardinality;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.*;

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
    @TestLogging("org.elasticsearch.indices.memory.breaker:TRACE,org.elasticsearch.index.fielddata:TRACE,org.elasticsearch.common.breaker:TRACE")
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
                    .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING, randomRidiculouslySmallLimit())
                    .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING, 1.05)
                    .build();
            client.admin().cluster().prepareUpdateSettings().setTransientSettings(settings).execute().actionGet();

            // execute a search that loads field data (sorting on the "test" field)
            // again, this time it should trip the breaker
            assertFailures(client.prepareSearch("cb-test").setSource("{\"sort\": \"test\",\"query\":{\"match_all\":{}}}"),
                    RestStatus.INTERNAL_SERVER_ERROR,
                    containsString("Data too large, data for [test] would be larger than limit of [100/100b]"));

            NodesStatsResponse stats = client.admin().cluster().prepareNodesStats().setBreaker(true).get();
            int breaks = 0;
            for (NodeStats stat : stats.getNodes()) {
                CircuitBreakerStats breakerStats = stat.getBreaker().getStats(CircuitBreaker.Name.FIELDDATA);
                breaks += breakerStats.getTrippedCount();
            }
            assertThat(breaks, greaterThanOrEqualTo(1));
        } finally {
            // Reset settings
            Settings resetSettings = settingsBuilder()
                    .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING, "-1")
                    .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                            HierarchyCircuitBreakerService.DEFAULT_FIELDDATA_OVERHEAD_CONSTANT)
                    .build();
            client.admin().cluster().prepareUpdateSettings().setTransientSettings(resetSettings).execute().actionGet();
        }
    }

    @Test
    @TestLogging("org.elasticsearch.indices.memory.breaker:TRACE,org.elasticsearch.index.fielddata:TRACE,org.elasticsearch.common.breaker:TRACE")
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
                    .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING, randomRidiculouslySmallLimit())
                    .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING, 1.05)
                    .build();
            client.admin().cluster().prepareUpdateSettings().setTransientSettings(settings).execute().actionGet();

            // execute a search that loads field data (sorting on the "test" field)
            // again, this time it should trip the breaker
            assertFailures(client.prepareSearch("ramtest").setSource("{\"sort\": \"test\",\"query\":{\"match_all\":{}}}"),
                    RestStatus.INTERNAL_SERVER_ERROR,
                    containsString("Data too large, data for [test] would be larger than limit of [100/100b]"));

            NodesStatsResponse stats = client.admin().cluster().prepareNodesStats().setBreaker(true).get();
            int breaks = 0;
            for (NodeStats stat : stats.getNodes()) {
                CircuitBreakerStats breakerStats = stat.getBreaker().getStats(CircuitBreaker.Name.FIELDDATA);
                breaks += breakerStats.getTrippedCount();
            }
            assertThat(breaks, greaterThanOrEqualTo(1));

        } finally {
            // Reset settings
            Settings resetSettings = settingsBuilder()
                    .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING, "-1")
                    .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                            HierarchyCircuitBreakerService.DEFAULT_FIELDDATA_OVERHEAD_CONSTANT)
                    .build();
            client.admin().cluster().prepareUpdateSettings().setTransientSettings(resetSettings).execute().actionGet();
        }
    }

    /**
     * Test that a breaker correctly redistributes to a different breaker, in
     * this case, the fielddata breaker borrows space from the request breaker
     */
    @Test
    @TestLogging("org.elasticsearch.indices.memory.breaker:TRACE,org.elasticsearch.index.fielddata:TRACE,org.elasticsearch.common.breaker:TRACE")
    public void testParentChecking() {
        assertAcked(prepareCreate("cb-test", 1, settingsBuilder().put(SETTING_NUMBER_OF_REPLICAS, between(0, 1))));
        Client client = client();

        try {
            // index some different terms so we have some field data for loading
            int docCount = scaledRandomIntBetween(300, 1000);
            for (long id = 0; id < docCount; id++) {
                client.prepareIndex("cb-test", "type", Long.toString(id))
                        .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + id).map()).execute().actionGet();
            }
            refresh();

            // We need the request limit beforehand, just from a single node because the limit should always be the same
            long beforeReqLimit = client.admin().cluster().prepareNodesStats().setBreaker(true).get()
                    .getNodes()[0].getBreaker().getStats(CircuitBreaker.Name.REQUEST).getLimit();

            Settings resetSettings = settingsBuilder()
                    .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING, "10b")
                    .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING, 1.0)
                    .build();
            client.admin().cluster().prepareUpdateSettings().setTransientSettings(resetSettings).execute().actionGet();

            // Perform a search to load field data for the "test" field
            try {
                client.prepareSearch("cb-test").setQuery(matchAllQuery()).addSort("test", SortOrder.DESC).get();
            } catch (Exception e) {
                String errMsg = "[FIELDDATA] Data too large, data for [test] would be larger than limit of [10/10b]";
                assertThat("Exception: " + ExceptionsHelper.unwrapCause(e) + " should contain a CircuitBreakingException",
                        ExceptionsHelper.unwrapCause(e).getMessage().contains(errMsg), equalTo(true));
            }

            assertFailures(client.prepareSearch("cb-test").setSource("{\"sort\": \"test\",\"query\":{\"match_all\":{}}}"),
                    RestStatus.INTERNAL_SERVER_ERROR,
                    containsString("Data too large, data for [test] would be larger than limit of [10/10b]"));

            // Adjust settings so the parent breaker will fail, but the fielddata breaker doesn't
            resetSettings = settingsBuilder()
                    .put(HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING, "15b")
                    .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING, "90%")
                    .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING, 1.0)
                    .build();
            client.admin().cluster().prepareUpdateSettings().setTransientSettings(resetSettings).execute().actionGet();

            // Perform a search to load field data for the "test" field
            try {
                client.prepareSearch("cb-test").setQuery(matchAllQuery()).addSort("test", SortOrder.DESC).get();
            } catch (Exception e) {
                String errMsg = "[PARENT] Data too large, data for [test] would be larger than limit of [15/15b]";
                assertThat("Exception: " + ExceptionsHelper.unwrapCause(e) + " should contain a CircuitBreakingException",
                        ExceptionsHelper.unwrapCause(e).getMessage().contains(errMsg), equalTo(true));
            }

        } finally {
            Settings resetSettings = settingsBuilder()
                    .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING, "-1")
                    .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING, HierarchyCircuitBreakerService.DEFAULT_REQUEST_BREAKER_LIMIT)
                    .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                            HierarchyCircuitBreakerService.DEFAULT_FIELDDATA_OVERHEAD_CONSTANT)
                    .build();
            client.admin().cluster().prepareUpdateSettings().setTransientSettings(resetSettings).execute().actionGet();
        }
    }

    @Test
    @TestLogging("org.elasticsearch.indices.memory.breaker:TRACE,org.elasticsearch.index.fielddata:TRACE,org.elasticsearch.common.breaker:TRACE")
    public void testRequestBreaker() {
        assertAcked(prepareCreate("cb-test", 1, settingsBuilder().put(SETTING_NUMBER_OF_REPLICAS, between(0, 1))));
        Client client = client();

        try {
            // Make request breaker limited to a small amount
            Settings resetSettings = settingsBuilder()
                    .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING, "10b")
                    .build();
            client.admin().cluster().prepareUpdateSettings().setTransientSettings(resetSettings).execute().actionGet();

            // index some different terms so we have some field data for loading
            int docCount = scaledRandomIntBetween(300, 1000);
            for (long id = 0; id < docCount; id++) {
                client.prepareIndex("cb-test", "type", Long.toString(id))
                        .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", id).map()).execute().actionGet();
            }
            refresh();

            // A cardinality aggregation uses BigArrays and thus the REQUEST breaker
            try {
                client.prepareSearch("cb-test").setQuery(matchAllQuery()).addAggregation(cardinality("card").field("test")).get();
                fail("aggregation should have tripped the breaker");
            } catch (Exception e) {
                String errMsg = "CircuitBreakingException[[REQUEST] Data too large, data for [<reused_arrays>] would be larger than limit of [10/10b]]";
                assertThat("Exception: " + ExceptionsHelper.unwrapCause(e) + " should contain a CircuitBreakingException",
                        ExceptionsHelper.unwrapCause(e).getMessage().contains(errMsg), equalTo(true));
            }
        } finally {
            Settings resetSettings = settingsBuilder()
                    .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING,
                            HierarchyCircuitBreakerService.DEFAULT_REQUEST_BREAKER_LIMIT)
                    .build();
            client.admin().cluster().prepareUpdateSettings().setTransientSettings(resetSettings).execute().actionGet();
        }
    }

}
