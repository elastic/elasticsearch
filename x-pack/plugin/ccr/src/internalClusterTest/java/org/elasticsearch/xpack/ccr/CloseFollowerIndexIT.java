/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.shard.CloseFollowerIndexErrorSuppressionHelper;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.xcontent.XContentType;
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
        CloseFollowerIndexErrorSuppressionHelper.setSuppressCreateEngineErrors(true);
        uncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
                if (t.getThreadGroup().getName().contains(getTestClass().getSimpleName())
                    && t.getName().equals("elasticsearch-error-rethrower")) {
                    for (StackTraceElement element : e.getStackTrace()) {
                        if (element.getClassName().equals(ReadOnlyEngine.class.getName())) {
                            if (element.getMethodName().equals("assertMaxSeqNoEqualsToGlobalCheckpoint")) {
                                logger.error("HACK: suppressing uncaught exception thrown from assertMaxSeqNoEqualsToGlobalCheckpoint", e);
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
        CloseFollowerIndexErrorSuppressionHelper.setSuppressCreateEngineErrors(false);
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
            return null;
        });
    }

    public void testCloseAndReopenFollowerIndex() throws Exception {
        final String leaderIndexSettings = getIndexSettings(1, 1);
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderYellow("index1");

        PutFollowAction.Request followRequest = new PutFollowAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        followRequest.setRemoteCluster("leader_cluster");
        followRequest.setLeaderIndex("index1");
        followRequest.setFollowerIndex("index2");
        followRequest.getParameters().setMaxRetryDelay(TimeValue.timeValueMillis(10));
        followRequest.getParameters().setReadPollTimeout(TimeValue.timeValueMillis(10));
        followRequest.getParameters().setMaxReadRequestSize(ByteSizeValue.ofBytes(1));
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

        ClusterState clusterState = followerClient().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        assertThat(clusterState.metadata().getProject().index("index2").getState(), is(IndexMetadata.State.CLOSE));
        assertThat(
            clusterState.getBlocks().hasIndexBlock(Metadata.DEFAULT_PROJECT_ID, "index2", MetadataIndexStateService.INDEX_CLOSED_BLOCK),
            is(true)
        );

        isRunning.set(false);
        for (Thread thread : threads) {
            thread.join();
        }

        assertAcked(followerClient().admin().indices().open(new OpenIndexRequest("index2").masterNodeTimeout(TimeValue.MAX_VALUE)).get());

        clusterState = followerClient().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        assertThat(clusterState.metadata().getProject().index("index2").getState(), is(IndexMetadata.State.OPEN));
        assertThat(
            clusterState.getBlocks()
                .hasIndexBlockWithId(Metadata.DEFAULT_PROJECT_ID, "index2", MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID),
            is(false)
        );
        ensureFollowerGreen("index2");

        refresh(leaderClient(), "index1");
        long leaderIndexDocs = SearchResponseUtils.getTotalHitsValue(leaderClient().prepareSearch("index1").setTrackTotalHits(true));
        assertBusy(() -> {
            refresh(followerClient(), "index2");
            long followerIndexDocs = SearchResponseUtils.getTotalHitsValue(
                followerClient().prepareSearch("index2").setTrackTotalHits(true)
            );
            assertThat(followerIndexDocs, equalTo(leaderIndexDocs));
        }, 30L, TimeUnit.SECONDS);
    }
}
