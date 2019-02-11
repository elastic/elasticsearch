/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class CloseFollowerIndexIT extends CcrIntegTestCase {

    public void testCloseAndReopenFollowerIndex() throws Exception {
        final String leaderIndexSettings = getIndexSettings(1, 1, singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderYellow("index1");

        PutFollowAction.Request followRequest = new PutFollowAction.Request();
        followRequest.setRemoteCluster("leader_cluster");
        followRequest.setLeaderIndex("index1");
        followRequest.setFollowerIndex("index2");
        followRequest.getParameters().setMaxRetryDelay(TimeValue.timeValueMillis(10));
        followRequest.getParameters().setReadPollTimeout(TimeValue.timeValueMillis(10));
        followRequest.getParameters().setMaxReadRequestSize(new ByteSizeValue(1));
        followRequest.getParameters().setMaxOutstandingReadRequests(128);
        followRequest.waitForActiveShards(ActiveShardCount.DEFAULT);

        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();
        ensureFollowerGreen("index2");

        AtomicBoolean isRunning = new AtomicBoolean(true);
        int numThreads = 4;
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                while (isRunning.get()) {
                    leaderClient().prepareIndex("index1", "doc").setSource("{}", XContentType.JSON).get();
                }
            });
            threads[i].start();
        }

        atLeastDocsIndexed(followerClient(), "index2", 32);
        AcknowledgedResponse response = followerClient().admin().indices().close(new CloseIndexRequest("index2")).get();
        assertThat(response.isAcknowledged(), is(true));

        ClusterState clusterState = followerClient().admin().cluster().prepareState().get().getState();
        List<ClusterBlock> blocks = new ArrayList<>(clusterState.getBlocks().indices().get("index2"));
        assertThat(blocks.size(), equalTo(1));
        assertThat(blocks.get(0).id(), equalTo(MetaDataIndexStateService.INDEX_CLOSED_BLOCK_ID));

        isRunning.set(false);
        for (Thread thread : threads) {
            thread.join();
        }
        assertAcked(followerClient().admin().indices().open(new OpenIndexRequest("index2")).get());

        refresh(leaderClient(), "index1");
        SearchRequest leaderSearchRequest = new SearchRequest("index1");
        leaderSearchRequest.source().trackTotalHits(true);
        long leaderIndexDocs = leaderClient().search(leaderSearchRequest).actionGet().getHits().getTotalHits();
        assertBusy(() -> {
            refresh(followerClient(), "index2");
            SearchRequest followerSearchRequest = new SearchRequest("index2");
            followerSearchRequest.source().trackTotalHits(true);
            long followerIndexDocs = followerClient().search(followerSearchRequest).actionGet().getHits().getTotalHits();
            assertThat(followerIndexDocs, equalTo(leaderIndexDocs));
        });
    }
}
