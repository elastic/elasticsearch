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

package org.elasticsearch.cluster;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.percolate.PercolateSourceBuilder;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.util.HashMap;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.Matchers.*;

/**
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class NoMasterNodeTests extends ElasticsearchIntegrationTest {

    @Test
    @TestLogging("action:TRACE,cluster.service:TRACE")
    public void testNoMasterActions() throws Exception {
        // note, sometimes, we want to check with the fact that an index gets created, sometimes not...
        boolean autoCreateIndex = randomBoolean();
        logger.info("auto_create_index set to {}", autoCreateIndex);

        Settings settings = settingsBuilder()
                .put("discovery.type", "zen")
                .put("action.auto_create_index", autoCreateIndex)
                .put("discovery.zen.minimum_master_nodes", 2)
                .put("discovery.zen.ping_timeout", "200ms")
                .put("discovery.initial_state_timeout", "500ms")
                .build();

        TimeValue timeout = TimeValue.timeValueMillis(200);

        internalCluster().startNode(settings);
        // start a second node, create an index, and then shut it down so we have no master block
        internalCluster().startNode(settings);
        createIndex("test");
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();
        internalCluster().stopRandomDataNode();
        assertBusy(new Runnable() {
            @Override
            public void run() {
                ClusterState state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                assertTrue(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK));
            }
        });

        try {
            client().prepareGet("test", "type1", "1").execute().actionGet();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException | MasterNotDiscoveredException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        try {
            client().prepareMultiGet().add("test", "type1", "1").execute().actionGet();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException | MasterNotDiscoveredException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        try {
            PercolateSourceBuilder percolateSource = new PercolateSourceBuilder();
            percolateSource.percolateDocument().setDoc(new HashMap());
            client().preparePercolate()
                    .setIndices("test").setDocumentType("type1")
                    .setSource(percolateSource).execute().actionGet();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException | MasterNotDiscoveredException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        long now = System.currentTimeMillis();
        try {
            client().prepareUpdate("test", "type1", "1").setScript("test script", ScriptService.ScriptType.INLINE).setTimeout(timeout).execute().actionGet();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException | MasterNotDiscoveredException e) {
            assertThat(System.currentTimeMillis() - now, greaterThan(timeout.millis() - 50));
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        try {
            client().admin().indices().prepareAnalyze("test", "this is a test").execute().actionGet();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException | MasterNotDiscoveredException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        try {
            client().prepareCount("test").execute().actionGet();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException | MasterNotDiscoveredException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        now = System.currentTimeMillis();
        try {
            client().prepareIndex("test", "type1", "1").setSource(XContentFactory.jsonBuilder().startObject().endObject()).setTimeout(timeout).execute().actionGet();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException | MasterNotDiscoveredException e) {
            assertThat(System.currentTimeMillis() - now, greaterThan(timeout.millis() - 50));
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        now = System.currentTimeMillis();
        try {
            BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
            bulkRequestBuilder.add(client().prepareIndex("test", "type1", "1").setSource(XContentFactory.jsonBuilder().startObject().endObject()));
            bulkRequestBuilder.add(client().prepareIndex("test", "type1", "2").setSource(XContentFactory.jsonBuilder().startObject().endObject()));
            bulkRequestBuilder.setTimeout(timeout);
            bulkRequestBuilder.get();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException | MasterNotDiscoveredException e) {
            if (autoCreateIndex) {
                // if its auto create index, the timeout will be based on the create index API
                assertThat(System.currentTimeMillis() - now, greaterThan(timeout.millis() - 50));
            } else {
                // TODO note, today we don't retry on global block for bulk operations-Dtests.seed=80C397728140167
                assertThat(System.currentTimeMillis() - now, lessThan(50l));
            }
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        internalCluster().startNode(settings);
        client().admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
    }
}
