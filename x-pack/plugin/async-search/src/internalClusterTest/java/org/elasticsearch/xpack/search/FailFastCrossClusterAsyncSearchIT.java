/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.query.ThrowingQueryBuilder;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.AsyncStatusResponse;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for cross-cluster search where a remote cluster marked with skip_unavailable=false
 * fails, the other searches are cancelled in order to fail-fast the overall search query.
 */
public class FailFastCrossClusterAsyncSearchIT extends AbstractCrossClusterAsyncSearchTestCase {

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER, false);
    }

    public void testClusterDetailsAfterCCSCancelledDueToFailFast() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        boolean skipUnavailable = (Boolean) testClusterInfo.get("remote.skip_unavailable");

        assertFalse("This test should be set up for remotes to always have skip_unavailable=false", skipUnavailable);

        SearchListenerPlugin.blockQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(true);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);

        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder.Builder(randomLong()).setExceptionForIndex(
            remoteIndex,
            new IllegalStateException("index corrupted")
        ).setSleepForIndex(localIndex, 500).build();
        request.getSearchRequest().source(new SearchSourceBuilder().query(queryBuilder).size(10));

        AsyncSearchResponse response = submitAsyncSearch(request);
        assertNotNull(response.getSearchResponse());
        assertTrue(response.isRunning());
        {
            SearchResponse.Clusters clusters = response.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertTrue("search cluster results should be marked as partial", clusters.hasPartialResults());

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));
        }

        SearchListenerPlugin.waitSearchStarted();
        SearchListenerPlugin.allowQueryPhase();
        waitForSearchTasksToFinish();

        // there is a slight race condition in this test due to how the ThrowingQueryBuilder works - the
        // local cluster is sleeping a short while to allow the remote cluster to fail first. Most of the
        // time this results in a CANCELLED status, but if the local cluster does finish before the remote
        // cluster fails, it will have SUCCESS status instead. The asserts below are set up to handle this
        // so the test should never fail due to this race condition.
        SearchResponse.Cluster.Status localClusterExpectedStatus = SearchResponse.Cluster.Status.CANCELLED;

        {
            AsyncSearchResponse finishedResponse = getAsyncSearch(response.getId());
            assertTrue(finishedResponse.isPartial());

            SearchResponse.Clusters clusters = finishedResponse.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(2));

            if (clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL) == 1) {
                localClusterExpectedStatus = SearchResponse.Cluster.Status.SUCCESSFUL;
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
            } else {
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(0));
            }
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(1));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(localClusterExpectedStatus));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.FAILED));
            assertNull(remoteClusterSearchInfo.getTotalShards());
            assertNull(remoteClusterSearchInfo.getSuccessfulShards());
            assertNull(remoteClusterSearchInfo.getSkippedShards());
            assertNull(remoteClusterSearchInfo.getFailedShards());
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(1));
            assertNull(remoteClusterSearchInfo.getTook());
            assertFalse(remoteClusterSearchInfo.isTimedOut());
            ShardSearchFailure remoteShardSearchFailure = remoteClusterSearchInfo.getFailures().get(0);
            assertTrue("should have 'index corrupted' in reason", remoteShardSearchFailure.reason().contains("index corrupted"));
        }

        // check that the async_search/status response includes the same cluster details
        {
            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            assertTrue(statusResponse.isPartial());

            SearchResponse.Clusters clusters = statusResponse.getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(1));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(localClusterExpectedStatus));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.FAILED));
            assertNull(remoteClusterSearchInfo.getTotalShards());
            assertNull(remoteClusterSearchInfo.getFailedShards());
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(1));
            assertNull(remoteClusterSearchInfo.getTook());
            assertFalse(remoteClusterSearchInfo.isTimedOut());
            ShardSearchFailure remoteShardSearchFailure = remoteClusterSearchInfo.getFailures().get(0);
            assertTrue("should have 'index corrupted' in reason", remoteShardSearchFailure.reason().contains("index corrupted"));
        }
    }
}
