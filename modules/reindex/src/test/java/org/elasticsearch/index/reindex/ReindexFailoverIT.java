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

package org.elasticsearch.index.reindex;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ReindexFailoverIT extends ReindexTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final HashSet<Class<? extends Plugin>> classes = new HashSet<>(super.nodePlugins());
        classes.add(MockTransportService.TestPlugin.class);
        return classes;
    }

    public void testReindexFailover() throws Throwable {
        int scrollTimeout = randomIntBetween(5, 15);

        logger.info("--> start 4 nodes, 1 master, 3 data");

        final Settings sharedSettings = Settings.builder()
            .put("cluster.join.timeout", "10s")  // still long to induce failures but not too long so test won't time out
            .build();

        internalCluster().startMasterOnlyNodes(1, sharedSettings);
        internalCluster().startDataOnlyNodes(3, sharedSettings);

        logger.info("--> wait for all nodes to join the cluster");
        ensureStableCluster(4);

        ClusterStateResponse clusterState = client().admin().cluster().prepareState().setNodes(true).get();
        Map<String, String> nodeIdToName = new HashMap<>();
        for (ObjectObjectCursor<String, DiscoveryNode> node : clusterState.getState().nodes().getDataNodes()) {
            nodeIdToName.put(node.key, node.value.getName());
        }

        // Create Index
        Settings.Builder indexSettings = Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 1);
        client().admin().indices().prepareCreate("source").setSettings(indexSettings).get();
        ensureGreen("source");

        List<IndexRequestBuilder> docs = new ArrayList<>();
        int docCount = between(1500, 5000);
        for (int i = 0; i < docCount; i++) {
            docs.add(client().prepareIndex("source", "_doc", Integer.toString(i)).setSource("foo", "a"));
        }

        indexRandom(true, docs);
        assertHitCount(client().prepareSearch("source").setSize(0).get(), docCount);
        ensureGreen("source");

        // Copy all the docs
        ReindexRequestBuilder copy = reindex().source("source").destination("dest").refresh(true);
        ReindexRequest reindexRequest = copy.request();
        reindexRequest.setScroll(TimeValue.timeValueSeconds(scrollTimeout));
        StartReindexJobAction.Request request = new StartReindexJobAction.Request(reindexRequest, false);

        copy.source().setSize(100);
        StartReindexJobAction.Response response = client().execute(StartReindexJobAction.INSTANCE, request).get();
        TaskId taskId = new TaskId(response.getTaskId());

        String nodeId = taskId.getNodeId();
        String nodeName = nodeIdToName.get(nodeId);

        logger.info("--> restarting node: " + nodeName);
        internalCluster().restartNode(nodeName, new InternalTestCluster.RestartCallback());

        ensureYellow("dest");

        assertBusy(() -> {
            try {
                assertEquals(docCount, client().prepareSearch("dest").get().getHits().getTotalHits().value);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });

        for (int i = 0; i < docCount; i++) {
            int docId = i;
            assertBusy(() -> {
                try {
                    GetResponse getResponse = client().prepareGet("dest", "_doc", Integer.toString(docId)).get();
                    assertTrue("Doc with id [" + docId + "] is missing", getResponse.isExists());
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
        }

        assertBusy(MockSearchService::assertNoInFlightContext, 10 + scrollTimeout, TimeUnit.SECONDS);

        // TODO: Add mechanism to wait for reindex task to complete
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }
}
