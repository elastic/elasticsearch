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
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ReindexFailoverIT extends ReindexTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put(SearchService.KEEPALIVE_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1)).build();
    }

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
            docs.add(client().prepareIndex("source").setId(Integer.toString(i)).setSource("foo", "a"));
        }

        indexRandom(true, docs);
        assertHitCount(client().prepareSearch("source").setSize(0).get(), docCount);
        ensureGreen("source");

        // Copy all the docs
        ReindexRequestBuilder copy = reindex().source("source").destination("dest").refresh(true);
        ReindexRequest reindexRequest = copy.request();
        reindexRequest.setScroll(TimeValue.timeValueSeconds(scrollTimeout));
        reindexRequest.setCheckpointInterval(TimeValue.timeValueMillis(100));
        StartReindexTaskAction.Request request = new StartReindexTaskAction.Request(reindexRequest, false);

        copy.source().setSize(10);
        copy.setRequestsPerSecond(1000);
        StartReindexTaskAction.Response response = client().execute(StartReindexTaskAction.INSTANCE, request).get();
        TaskId taskId = new TaskId(response.getEphemeralTaskId());

        String nodeId = taskId.getNodeId();
        String nodeName = nodeIdToName.get(nodeId);

        assertBusy(() -> {
            try {
                assertThat(((BulkByScrollTask.Status) client().admin().cluster()
                    .prepareGetTask(taskId).get().getTask().getTask().getStatus()).getCreated(),
                    greaterThanOrEqualTo(10L));

            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        client().admin().indices().prepareRefresh("dest").get();
        assertThat(client().prepareSearch("dest").get().getHits().getTotalHits().value, greaterThanOrEqualTo(10L));

        logger.info("--> restarting node: " + nodeName);
        ensureGreen(ReindexIndexClient.REINDEX_ALIAS);
        internalCluster().restartNode(nodeName, new InternalTestCluster.RestartCallback());
        client().admin().indices().prepareRefresh("dest").get();
        long hitsAfterRestart = client().prepareSearch("dest").get().getHits().getTotalHits().value;

        // todo: once we can rethrottle, make it slow initially and speed it up here and verify that hitsAfterRestart < docsCount

        ensureYellow("dest");

        assertBusy(() -> {
            try {
                assertEquals(docCount, client().prepareSearch("dest").get().getHits().getTotalHits().value);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }, 30, TimeUnit.SECONDS);

        for (int i = 0; i < docCount; i++) {
            int docId = i;
            assertBusy(() -> {
                try {
                    GetResponse getResponse = client().prepareGet("dest", Integer.toString(docId)).get();
                    assertTrue("Doc with id [" + docId + "] is missing", getResponse.isExists());
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
        }

        assertBusy(() -> {
            BitSet seqNos = new BitSet();
            client().prepareSearch("dest").setSize(5000).seqNoAndPrimaryTerm(true).get().getHits()
                .forEach(hit -> seqNos.set(Math.toIntExact(hit.getSeqNo())));
            assertEquals(docCount, seqNos.cardinality());

            // first 9 should not be replayed.
            for (int i = 0; i < 9 ; ++i) {
                assertTrue("index: " + i, seqNos.get(i));
            }

            if (hitsAfterRestart < docCount) {
                // at least one overlapped seqNo.
                assertThat(seqNos.length(), greaterThan(docCount));
            }
            // The first 9 should not be replayed, we restart from at least seqNo 9.
            assertThat("docCount: " + docCount + " hitsAfterRestart " + hitsAfterRestart, seqNos.length()-1,
                lessThan(Math.toIntExact(docCount + hitsAfterRestart - 9)));

        });

        assertBusy(MockSearchService::assertNoInFlightContext, 10 + scrollTimeout, TimeUnit.SECONDS);

        @SuppressWarnings("unchecked")
        Map<String, Object> dotReindexResponse =
            (Map<String, Object>) client().prepareGet(".reindex", response.getPersistentTaskId()).get().getSource().get("response");
        assertThat((int) dotReindexResponse.get("created") + (int) dotReindexResponse.get("updated"),
            greaterThanOrEqualTo(docCount));

        // TODO: Add mechanism to wait for reindex task to complete
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }
}
