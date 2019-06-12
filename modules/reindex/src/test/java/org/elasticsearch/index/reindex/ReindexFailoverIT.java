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

import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class ReindexFailoverIT extends ReindexTestCase {

    @TestLogging("_root:DEBUG")
    public void testReindexFailover() throws Throwable {
        logger.info("--> start 4 nodes, 1 master, 3 data");

        final Settings sharedSettings = Settings.builder()
            .put("cluster.join.timeout", "10s")  // still long to induce failures but not too long so test won't time out
            .build();

        internalCluster().startMasterOnlyNodes(1, sharedSettings);
        List<String> dataNodes = internalCluster().startDataOnlyNodes(3, sharedSettings);

        logger.info("--> wait for all nodes to join the cluster");
        ensureStableCluster(4);

        // Create Index
        Settings.Builder indexSettings = Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0);
        client().admin().indices().prepareCreate("source").setSettings(indexSettings).get();
        ensureGreen("source");

        List<IndexRequestBuilder> docs = new ArrayList<>();
        int max = between(1500, 5000);
        for (int i = 0; i < max; i++) {
            docs.add(client().prepareIndex("source", "test", Integer.toString(i)).setSource("foo", "a"));
        }

        indexRandom(true, docs);
        assertHitCount(client().prepareSearch("source").setSize(0).get(), max);

        // Copy all the docs
        ReindexRequestBuilder copy = reindex().source("source").destination("dest", "type").refresh(true);
        StartReindexJobAction.Request request = new StartReindexJobAction.Request(copy.request());
        // Use a small batch size so we have to use more than one batch
        copy.source().setSize(5);
        StartReindexJobAction.Response reindexResponse = client().execute(StartReindexJobAction.INSTANCE, request).get();
        String taskId = reindexResponse.getTaskId();

        GetTaskResponse getTaskResponse = client().admin().cluster().prepareGetTask(taskId).get();
        String nodeId = getTaskResponse.getTask().getTask().getTaskId().getNodeId();

//        // interrupt communication between master and other nodes in cluster
//        String master = internalCluster().getMasterName();
//        Set<String> otherNodes = new HashSet<>(Arrays.asList(internalCluster().getNodeNames()));
//        otherNodes.remove(master);
//
//        NetworkDisruption partition = new NetworkDisruption(
//            new NetworkDisruption.TwoPartitions(Collections.singleton(master), otherNodes),
//            new NetworkDisruption.NetworkDisconnect());
//        internalCluster().setDisruptionScheme(partition);
//
//        logger.info("--> disrupting network");
//        partition.startDisrupting();
//
//        logger.info("--> waiting for new master to be elected");
//        ensureStableCluster(3, dataNodes);
//
//        partition.stopDisrupting();
//        logger.info("--> waiting to heal");
//        ensureStableCluster(4);
//
//        ensureGreen("myindex");
//        refresh();
//        assertThat(client().prepareSearch("myindex").get().getHits().getTotalHits().value, equalTo(10L));
    }


}
