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
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.junit.After;
import org.junit.Before;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class CloseFollowerIndexIT extends CcrIntegTestCase {

    private Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

    @Before
    public void wrapUncaughtExceptionHandler() {
        uncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
                if (t.getThreadGroup().getName().contains(getTestClass().getSimpleName())) {
                    for (StackTraceElement element : e.getStackTrace()) {
                        if (element.getClassName().equals(ReadOnlyEngine.class.getName())) {
                            if (element.getMethodName().equals("assertMaxSeqNoEqualsToGlobalCheckpoint")) {
                                return;
                            }
                        }
                    }
                }
                uncaughtExceptionHandler.uncaughtException(t, e);
            });
            return null;
        });
    }

    @After
    public void restoreUncaughtExceptionHandler() {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
            return null;
        });
    }

    public void testCloseAndReopenFollowerIndex() throws Exception {
        final String leaderIndexSettings = getIndexSettings(1, 1);
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
                    leaderClient().prepareIndex("index1").setSource("{}", XContentType.JSON).get();
                }
            });
            threads[i].start();
        }

        atLeastDocsIndexed(followerClient(), "index2", 32);

        CloseIndexRequest closeIndexRequest = new CloseIndexRequest("index2").masterNodeTimeout(TimeValue.MAX_VALUE);
        closeIndexRequest.waitForActiveShards(ActiveShardCount.NONE);
        AcknowledgedResponse response = followerClient().admin().indices().close(closeIndexRequest).get();
        assertThat(response.isAcknowledged(), is(true));

        ClusterState clusterState = followerClient().admin().cluster().prepareState().get().getState();
        assertThat(clusterState.metadata().index("index2").getState(), is(IndexMetadata.State.CLOSE));
        assertThat(clusterState.getBlocks().hasIndexBlock("index2", MetadataIndexStateService.INDEX_CLOSED_BLOCK), is(true));
        assertThat(followerClient().admin().cluster().prepareHealth("index2").get().getStatus(), equalTo(ClusterHealthStatus.RED));

        isRunning.set(false);
        for (Thread thread : threads) {
            thread.join();
        }

        assertAcked(followerClient().admin().indices().open(new OpenIndexRequest("index2").masterNodeTimeout(TimeValue.MAX_VALUE)).get());

        clusterState = followerClient().admin().cluster().prepareState().get().getState();
        assertThat(clusterState.metadata().index("index2").getState(), is(IndexMetadata.State.OPEN));
        assertThat(clusterState.getBlocks().hasIndexBlockWithId("index2", MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID), is(false));
        ensureFollowerGreen("index2");

        refresh(leaderClient(), "index1");
        SearchRequest leaderSearchRequest = new SearchRequest("index1");
        leaderSearchRequest.source().trackTotalHits(true);
        long leaderIndexDocs = leaderClient().search(leaderSearchRequest).actionGet().getHits().getTotalHits().value;
        assertBusy(() -> {
            refresh(followerClient(), "index2");
            SearchRequest followerSearchRequest = new SearchRequest("index2");
            followerSearchRequest.source().trackTotalHits(true);
            long followerIndexDocs = followerClient().search(followerSearchRequest).actionGet().getHits().getTotalHits().value;
            assertThat(followerIndexDocs, equalTo(leaderIndexDocs));
        }, 30L, TimeUnit.SECONDS);
    }
}
