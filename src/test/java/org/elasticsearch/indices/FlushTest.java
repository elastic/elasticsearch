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
package org.elasticsearch.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;

public class FlushTest extends ElasticsearchIntegrationTest {

    @Test
    public void testWaitIfOngoing() throws InterruptedException {
        createIndex("test");
        ensureGreen("test");
        final int numIters = scaledRandomIntBetween(10, 30);
        for (int i = 0; i < numIters; i++) {
            for (int j = 0; j < 10; j++) {
                client().prepareIndex("test", "test").setSource("{}").get();
            }
            final CountDownLatch latch = new CountDownLatch(10);
            final CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();
            for (int j = 0; j < 10; j++) {
                client().admin().indices().prepareFlush("test").setWaitIfOngoing(true).execute(new ActionListener<FlushResponse>() {
                    @Override
                    public void onResponse(FlushResponse flushResponse) {
                        try {
                            // dont' use assertAllSuccesssful it uses a randomized context that belongs to a different thread
                            assertThat("Unexpected ShardFailures: " + Arrays.toString(flushResponse.getShardFailures()), flushResponse.getFailedShards(), equalTo(0));
                            latch.countDown();
                        } catch (Throwable ex) {
                            onFailure(ex);
                        }

                    }

                    @Override
                    public void onFailure(Throwable e) {
                        errors.add(e);
                        latch.countDown();
                    }
                });
            }
            latch.await();
            assertThat(errors, emptyIterable());
        }
    }

    public void testSyncedFlush() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("test").setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1).get();
        ensureGreen();
        ClusterStateResponse state = client().admin().cluster().prepareState().get();
        String nodeId = state.getState().getRoutingTable().index("test").shard(0).getShards().get(0).currentNodeId();
        String nodeName = state.getState().getNodes().get(nodeId).name();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        indicesService.indexServiceSafe("test").shardInjectorSafe(0).getInstance(SyncedFlushService.class).attemptSyncedFlush(new ShardId("test", 0));
    }
}
