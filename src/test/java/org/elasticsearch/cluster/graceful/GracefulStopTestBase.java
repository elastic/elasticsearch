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

package org.elasticsearch.cluster.graceful;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.deallocator.Deallocators;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.GracefulStop;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.concurrent.TimeUnit;

public class GracefulStopTestBase extends ElasticsearchIntegrationTest {

    static String mappingSource;

    GracefulStop gracefulStop;
    Deallocators deallocators;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void prepareClass() throws Exception {
        mappingSource = XContentFactory.jsonBuilder().startObject().startObject("properties")
                .startObject("_id")
                .field("type", "integer")
                .endObject()
                .startObject("name")
                .field("type", "string")
                .endObject()
                .endObject()
                .endObject().string();
    }

    @Before
    public void prepare() {
        DiscoveryNode takeDownNode = clusterService().state().nodes().dataNodes().values().iterator().next().value;
        gracefulStop = ((InternalTestCluster) cluster()).getInstance(GracefulStop.class, takeDownNode.name());
        deallocators = ((InternalTestCluster) cluster()).getInstance(Deallocators.class, takeDownNode.name());
    }

    @After
    public void cleanUp() {
        // reset to default
        setSettings(false, true, "primaries", "2h");
        gracefulStop = null;
        deallocators = null;
    }

    protected void setSettings(boolean force, boolean reallocate, String minAvailability, String timeOut) {
        client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(ImmutableSettings.builder()
                        .put("cluster.graceful_stop.force", force)
                        .put("cluster.graceful_stop.reallocate", reallocate)
                        .put("cluster.graceful_stop.min_availability", minAvailability)
                        .put("cluster.graceful_stop.timeout", timeOut)).execute().actionGet();
    }

    /**
     * asserting the cluster.routing.allocation.enable setting got reset to null
     */
    private static final Predicate ALLOCATION_SETTINGS_GOT_RESET = new Predicate() {
        @Override
        public boolean apply(Object input) {
            String enableSetting = internalCluster().clusterService().state().metaData()
                    .settings().get(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE);
            return enableSetting == null || EnableAllocationDecider.Allocation.ALL.name().equalsIgnoreCase(enableSetting);
        }
    };

    protected void assertAllocationSettingsGotReset() throws Exception {
        assertTrue("'cluster.routing.allocation.enable' did not get reset", awaitBusy(ALLOCATION_SETTINGS_GOT_RESET, 5, TimeUnit.SECONDS));
    }

    protected void createIndex(int shards, int replicas, int records) {
        client().admin().indices()
                .prepareCreate("t2")
                .addMapping("default", mappingSource)
                .setSettings(ImmutableSettings.builder().put("number_of_shards", shards).put("number_of_replicas", replicas))
                .execute().actionGet();
        for (int i = 0; i < records; i++) {
            client().prepareIndex("t2", "default")
                    .setId(String.valueOf(randomInt()))
                    .setSource(ImmutableMap.<String, Object>of("name", randomAsciiOfLength(10))).execute().actionGet();
        }
        refresh();
    }
}
