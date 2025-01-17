/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.LegacyReaderContext;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.SlowRunningQueryBuilder;
import org.elasticsearch.search.query.ThrowingQueryBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.async.AsyncResultsIndexPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.async.GetAsyncStatusRequest;
import org.elasticsearch.xpack.core.async.TransportDeleteAsyncResultAction;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.AsyncStatusResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.GetAsyncStatusAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.not;

public class CrossClusterAsyncSearchIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER = "cluster_a";
    private static final long EARLIEST_TIMESTAMP = 1691348810000L;
    private static final long LATEST_TIMESTAMP = 1691348820000L;

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER, randomBoolean());
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugs = Arrays.asList(
            SearchListenerPlugin.class,
            AsyncSearch.class,
            AsyncResultsIndexPlugin.class,
            LocalStateCompositeXPackPlugin.class,
            TestQueryBuilderPlugin.class
        );
        return Stream.concat(super.nodePlugins(clusterAlias).stream(), plugs.stream()).collect(Collectors.toList());
    }

    public static class TestQueryBuilderPlugin extends Plugin implements SearchPlugin {
        public TestQueryBuilderPlugin() {}

        @Override
        public List<QuerySpec<?>> getQueries() {
            QuerySpec<SlowRunningQueryBuilder> slowRunningSpec = new QuerySpec<>(
                SlowRunningQueryBuilder.NAME,
                SlowRunningQueryBuilder::new,
                p -> {
                    throw new IllegalStateException("not implemented");
                }
            );
            QuerySpec<ThrowingQueryBuilder> throwingSpec = new QuerySpec<>(ThrowingQueryBuilder.NAME, ThrowingQueryBuilder::new, p -> {
                throw new IllegalStateException("not implemented");
            });

            return List.of(slowRunningSpec, throwingSpec);
        }
    }

    public void testClusterDetailsAfterSuccessfulCCS() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        SearchListenerPlugin.blockQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        request.getSearchRequest().source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, 256));
        }
        boolean dfs = randomBoolean();
        if (dfs) {
            request.getSearchRequest().searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }

        final String responseId;
        final AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            responseId = response.getId();
            assertNotNull(response.getSearchResponse());
            assertTrue(response.isRunning());
            SearchResponse.Clusters clusters = response.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertTrue("search cluster results should be marked as partial", clusters.hasPartialResults());

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));
        } finally {
            response.decRef();
        }

        SearchListenerPlugin.waitSearchStarted();
        SearchListenerPlugin.allowQueryPhase();

        waitForSearchTasksToFinish();
        final AsyncSearchResponse finishedResponse = getAsyncSearch(responseId);
        try {
            assertFalse(finishedResponse.isPartial());

            SearchResponse.Clusters clusters = finishedResponse.getSearchResponse().getClusters();
            assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
        } finally {
            finishedResponse.decRef();
        }

        // check that the async_search/status response includes the same cluster details
        {
            AsyncStatusResponse statusResponse = getAsyncStatus(responseId);
            assertFalse(statusResponse.isPartial());

            SearchResponse.Clusters clusters = statusResponse.getClusters();
            assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
        }
    }

    // CCS with a search where the timestamp of the query cannot match so should be SUCCESSFUL with all shards skipped
    // during can-match
    public void testCCSClusterDetailsWhereAllShardsSkippedInCanMatch() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        SearchListenerPlugin.blockQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, 256));
        }
        boolean dfs = randomBoolean();
        if (dfs) {
            request.getSearchRequest().searchType(SearchType.DFS_QUERY_THEN_FETCH);
        } else {
            request.getSearchRequest().searchType(SearchType.QUERY_THEN_FETCH);
        }
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder("@timestamp").from(100).to(2000);
        request.getSearchRequest().source(new SearchSourceBuilder().query(rangeQueryBuilder).size(10));

        boolean minimizeRoundtrips = TransportSearchAction.shouldMinimizeRoundtrips(request.getSearchRequest());

        final String responseId;
        final AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            responseId = response.getId();
            assertNotNull(response.getSearchResponse());
            SearchResponse.Clusters clusters = response.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            if (dfs) {
                assertTrue("search cluster results should be marked as partial", clusters.hasPartialResults());
            } else {
                assertFalse(
                    "search cluster results should not be marked as partial as all shards are skipped",
                    clusters.hasPartialResults()
                );
            }
            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            if (dfs) {
                assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));
            } else {
                assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            }

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
        } finally {
            response.decRef();
        }
        if (dfs) {
            SearchListenerPlugin.waitSearchStarted();
        }
        SearchListenerPlugin.allowQueryPhase();

        waitForSearchTasksToFinish();
        final AsyncSearchResponse finishedResponse = getAsyncSearch(responseId);
        try {
            assertNotNull(finishedResponse);
            assertFalse(finishedResponse.isPartial());

            SearchResponse.Clusters clusters = finishedResponse.getSearchResponse().getClusters();
            assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);

            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
            if (dfs) {
                // no skipped shards locally when DFS_QUERY_THEN_FETCH is used
                assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            } else {
                assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(localNumShards));
            }
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThanOrEqualTo(0L));

            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards));
            if (minimizeRoundtrips) {
                assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(remoteNumShards));
            } else {
                assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(remoteNumShards));
            }
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThanOrEqualTo(0L));
        } finally {
            finishedResponse.decRef();
        }
        {
            AsyncStatusResponse statusResponse = getAsyncStatus(responseId);
            assertNotNull(statusResponse);
            assertFalse(statusResponse.isPartial());

            SearchResponse.Clusters clusters = statusResponse.getClusters();
            assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);

            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
            if (dfs) {
                // no skipped shards locally when DFS_QUERY_THEN_FETCH is used
                assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            } else {
                assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(localNumShards));
            }
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThanOrEqualTo(0L));

            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards));
            if (minimizeRoundtrips) {
                assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(remoteNumShards));
            } else {
                assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(remoteNumShards));
            }
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThanOrEqualTo(0L));
        }
    }

    public void testClusterDetailsAfterCCSWithFailuresOnAllShards() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");
        boolean skipUnavailable = (Boolean) testClusterInfo.get("remote.skip_unavailable");

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, 256));
        }
        request.setKeepOnCompletion(true);
        boolean dfs = randomBoolean();
        if (dfs) {
            request.getSearchRequest().searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }
        // shardId -1 means to throw the Exception on all shards, so should result in complete search failure
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("index corrupted"), -1);
        request.getSearchRequest().source(new SearchSourceBuilder().query(queryBuilder).size(10));

        boolean minimizeRoundtrips = TransportSearchAction.shouldMinimizeRoundtrips(request.getSearchRequest());

        final AsyncSearchResponse response = submitAsyncSearch(request);
        final String responseId;
        try {
            assertNotNull(response.getSearchResponse());
            waitForSearchTasksToFinish();
            responseId = response.getId();
        } finally {
            response.decRef();
        }
        final AsyncSearchResponse finishedResponse = getAsyncSearch(responseId);
        try {
            assertTrue(finishedResponse.isPartial());

            SearchResponse.Clusters clusters = finishedResponse.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
            if (skipUnavailable) {
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(1));
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(1));
            } else {
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(2));
            }

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.FAILED));
            assertAllShardsFailed(minimizeRoundtrips, localClusterSearchInfo, localNumShards);

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            SearchResponse.Cluster.Status expectedStatus = skipUnavailable
                ? SearchResponse.Cluster.Status.SKIPPED
                : SearchResponse.Cluster.Status.FAILED;
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(expectedStatus));
            assertAllShardsFailed(minimizeRoundtrips, remoteClusterSearchInfo, remoteNumShards);
        } finally {
            finishedResponse.decRef();
        }
        // check that the async_search/status response includes the same cluster details
        {
            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            assertTrue(statusResponse.isPartial());

            SearchResponse.Clusters clusters = statusResponse.getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
            if (skipUnavailable) {
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(1));
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(1));
            } else {
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(2));
            }

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.FAILED));
            assertAllShardsFailed(minimizeRoundtrips, localClusterSearchInfo, localNumShards);

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            SearchResponse.Cluster.Status expectedStatus = skipUnavailable
                ? SearchResponse.Cluster.Status.SKIPPED
                : SearchResponse.Cluster.Status.FAILED;
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(expectedStatus));
            assertAllShardsFailed(minimizeRoundtrips, remoteClusterSearchInfo, remoteNumShards);
        }
    }

    public void testClusterDetailsAfterCCSWithFailuresOnOneShardOnly() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        SearchListenerPlugin.blockQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, 256));
        }
        boolean dfs = randomBoolean();
        if (dfs) {
            request.getSearchRequest().searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }
        // shardId 0 means to throw the Exception only on shard 0; all others should work
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("index corrupted"), 0);
        request.getSearchRequest().source(new SearchSourceBuilder().query(queryBuilder).size(10));

        final AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            assertNotNull(response.getSearchResponse());
            assertTrue(response.isRunning());
            SearchResponse.Clusters clusters = response.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertTrue("search cluster results should be marked as partial", clusters.hasPartialResults());

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));
        } finally {
            response.decRef();
        }

        SearchListenerPlugin.waitSearchStarted();
        SearchListenerPlugin.allowQueryPhase();

        waitForSearchTasksToFinish();

        final AsyncSearchResponse finishedResponse = getAsyncSearch(response.getId());
        try {
            assertTrue(finishedResponse.isPartial());

            SearchResponse.Clusters clusters = finishedResponse.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards - 1));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(1));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(1));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));
            ShardSearchFailure localShardSearchFailure = localClusterSearchInfo.getFailures().get(0);
            assertTrue(
                "should have 'index corrupted' in reason but was: " + localShardSearchFailure.reason(),
                localShardSearchFailure.reason().contains("index corrupted")
            );

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards - 1));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(1));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(1));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
            ShardSearchFailure remoteShardSearchFailure = remoteClusterSearchInfo.getFailures().get(0);
            assertTrue(
                "should have 'index corrupted' in reason but was: " + remoteShardSearchFailure.reason(),
                remoteShardSearchFailure.reason().contains("index corrupted")
            );
        } finally {
            finishedResponse.decRef();
        }
        // check that the async_search/status response includes the same cluster details
        {
            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            assertTrue(statusResponse.isPartial());

            SearchResponse.Clusters clusters = statusResponse.getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards - 1));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(1));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(1));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));
            ShardSearchFailure localShardSearchFailure = localClusterSearchInfo.getFailures().get(0);
            assertTrue(
                "should have 'index corrupted' in reason but was: " + localShardSearchFailure.reason(),
                localShardSearchFailure.reason().contains("index corrupted")
            );

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards - 1));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(1));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(1));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
            ShardSearchFailure remoteShardSearchFailure = remoteClusterSearchInfo.getFailures().get(0);
            assertTrue(
                "should have 'index corrupted' in reason but was: " + remoteShardSearchFailure.reason(),
                remoteShardSearchFailure.reason().contains("index corrupted")
            );
        }
    }

    public void testClusterDetailsAfterCCSWithFailuresOnOneClusterOnly() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");
        boolean skipUnavailable = (Boolean) testClusterInfo.get("remote.skip_unavailable");

        SearchListenerPlugin.blockQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, 256));
        }
        boolean dfs = randomBoolean();
        if (dfs) {
            request.getSearchRequest().searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }

        // throw Exception of all shards of remoteIndex, but against localIndex
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(
            randomLong(),
            new IllegalStateException("index corrupted"),
            remoteIndex
        );
        request.getSearchRequest().source(new SearchSourceBuilder().query(queryBuilder).size(10));

        boolean minimizeRoundtrips = TransportSearchAction.shouldMinimizeRoundtrips(request.getSearchRequest());

        final AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            assertNotNull(response.getSearchResponse());
            assertTrue(response.isRunning());
            SearchResponse.Clusters clusters = response.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertTrue("search cluster results should be marked as partial", clusters.hasPartialResults());

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));
        } finally {
            response.decRef();
        }

        SearchListenerPlugin.waitSearchStarted();
        SearchListenerPlugin.allowQueryPhase();

        waitForSearchTasksToFinish();

        final AsyncSearchResponse finishedResponse = getAsyncSearch(response.getId());
        try {
            assertTrue(finishedResponse.isPartial());

            SearchResponse.Clusters clusters = finishedResponse.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(1));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
            if (skipUnavailable) {
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(1));
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));
            } else {
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(1));
            }

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            SearchResponse.Cluster.Status expectedStatus = skipUnavailable
                ? SearchResponse.Cluster.Status.SKIPPED
                : SearchResponse.Cluster.Status.FAILED;
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(expectedStatus));
            if (minimizeRoundtrips) {
                assertNull(remoteClusterSearchInfo.getTotalShards());
                assertNull(remoteClusterSearchInfo.getSuccessfulShards());
                assertNull(remoteClusterSearchInfo.getSkippedShards());
                assertNull(remoteClusterSearchInfo.getFailedShards());
                assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(1));
            } else {
                assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
                assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(0));
                assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
                assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(remoteNumShards));
                assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(remoteNumShards));
            }
            assertNull(remoteClusterSearchInfo.getTook());
            assertFalse(remoteClusterSearchInfo.isTimedOut());
            ShardSearchFailure remoteShardSearchFailure = remoteClusterSearchInfo.getFailures().get(0);
            assertTrue(
                "should have 'index corrupted' in reason but was: " + remoteShardSearchFailure.reason(),
                remoteShardSearchFailure.reason().contains("index corrupted")
            );
        } finally {
            finishedResponse.decRef();
        }
        // check that the async_search/status response includes the same cluster details
        {
            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            assertTrue(statusResponse.isPartial());

            SearchResponse.Clusters clusters = statusResponse.getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(1));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
            if (skipUnavailable) {
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(1));
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));
            } else {
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(1));
            }

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            SearchResponse.Cluster.Status expectedStatus = skipUnavailable
                ? SearchResponse.Cluster.Status.SKIPPED
                : SearchResponse.Cluster.Status.FAILED;
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(expectedStatus));
            if (minimizeRoundtrips) {
                assertNull(remoteClusterSearchInfo.getTotalShards());
                assertNull(remoteClusterSearchInfo.getFailedShards());
                assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(1));
            } else {
                assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
                assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(remoteNumShards));
                assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(remoteNumShards));
            }
            assertNull(remoteClusterSearchInfo.getTook());
            assertFalse(remoteClusterSearchInfo.isTimedOut());
            ShardSearchFailure remoteShardSearchFailure = remoteClusterSearchInfo.getFailures().get(0);
            assertTrue(
                "should have 'index corrupted' in reason but was: " + remoteShardSearchFailure.reason(),
                remoteShardSearchFailure.reason().contains("index corrupted")
            );
        }
    }

    // tests bug fix https://github.com/elastic/elasticsearch/issues/100350
    public void testClusterDetailsAfterCCSWhereRemoteClusterHasNoShardsToSearch() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");

        SearchListenerPlugin.blockQueryPhase();

        // query against a missing index on the remote cluster
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + "no_such_index*");
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        request.getSearchRequest().source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, 256));
        }
        boolean dfs = randomBoolean();
        if (dfs) {
            request.getSearchRequest().searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }

        final AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            assertNotNull(response.getSearchResponse());
            assertTrue(response.isRunning());

            boolean minimizeRoundtrips = TransportSearchAction.shouldMinimizeRoundtrips(request.getSearchRequest());

            assertNotNull(response.getSearchResponse());
            assertTrue(response.isRunning());

            SearchResponse.Clusters clusters = response.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertTrue("search cluster results should be marked as partial", clusters.hasPartialResults());

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));
        } finally {
            response.decRef();
        }

        SearchListenerPlugin.waitSearchStarted();
        SearchListenerPlugin.allowQueryPhase();

        waitForSearchTasksToFinish();

        final AsyncSearchResponse finishedResponse = getAsyncSearch(response.getId());
        try {
            assertFalse(finishedResponse.isPartial());

            SearchResponse.Clusters clusters = finishedResponse.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(0));  // will be zero since index does not index
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));

            assertNotNull(remoteClusterSearchInfo.getTook());
            assertFalse(remoteClusterSearchInfo.isTimedOut());
        } finally {
            finishedResponse.decRef();
        }
        // check that the async_search/status response includes the same cluster details
        {
            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            assertFalse(statusResponse.isPartial());

            SearchResponse.Clusters clusters = statusResponse.getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(0));  // will be zero since index does not index
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));

            assertNotNull(remoteClusterSearchInfo.getTook());
            assertFalse(remoteClusterSearchInfo.isTimedOut());
        }
    }

    public void testCCSWithSearchTimeout() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        TimeValue searchTimeout = new TimeValue(100, TimeUnit.MILLISECONDS);
        // query builder that will sleep for the specified amount of time in the query phase
        SlowRunningQueryBuilder slowRunningQueryBuilder = new SlowRunningQueryBuilder(searchTimeout.millis() * 5);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(slowRunningQueryBuilder).timeout(searchTimeout);

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.getSearchRequest().source(sourceBuilder);
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, 256));
        }
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.getSearchRequest().allowPartialSearchResults(true);
        request.setKeepOnCompletion(true);
        boolean dfs = randomBoolean();
        if (dfs) {
            request.getSearchRequest().searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }

        final String responseId;
        final AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            assertNotNull(response.getSearchResponse());
            waitForSearchTasksToFinish();
            responseId = response.getId();
        } finally {
            response.decRef();
        }

        final AsyncSearchResponse finishedResponse = getAsyncSearch(responseId);
        try {
            assertTrue(finishedResponse.getSearchResponse().isTimedOut());
            assertTrue(finishedResponse.isPartial());

            SearchResponse.Clusters clusters = finishedResponse.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            // PARTIAL expected since timedOut=true
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThanOrEqualTo(0L));
            assertTrue(localClusterSearchInfo.isTimedOut());

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            // PARTIAL expected since timedOut=true
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThanOrEqualTo(0L));
            assertTrue(remoteClusterSearchInfo.isTimedOut());
        } finally {
            finishedResponse.decRef();
        }
        // check that the async_search/status response includes the same cluster details
        {
            AsyncStatusResponse statusResponse = getAsyncStatus(responseId);
            assertTrue(statusResponse.isPartial());

            SearchResponse.Clusters clusters = statusResponse.getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            // PARTIAL expected since timedOut=true
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThanOrEqualTo(0L));
            assertTrue(localClusterSearchInfo.isTimedOut());

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            // PARTIAL expected since timedOut=true
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThanOrEqualTo(0L));
            assertTrue(remoteClusterSearchInfo.isTimedOut());
        }
    }

    public void testRemoteClusterOnlyCCSSuccessfulResult() throws Exception {
        // for remote-only queries, we can't use the SearchListenerPlugin since that listens for search
        // stage on the local cluster, so we only test final state of the search response
        SearchListenerPlugin.negate();

        Map<String, Object> testClusterInfo = setupTwoClusters();
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        // search only the remote cluster
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, 256));
        }
        boolean dfs = randomBoolean();
        if (dfs) {
            request.getSearchRequest().searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }
        request.getSearchRequest().source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));

        final String responseId;
        final AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            assertNotNull(response.getSearchResponse());
            responseId = response.getId();
        } finally {
            response.decRef();
        }

        waitForSearchTasksToFinish();

        final AsyncSearchResponse finishedResponse = getAsyncSearch(responseId);
        try {
            assertFalse(finishedResponse.isPartial());

            SearchResponse.Clusters clusters = finishedResponse.getSearchResponse().getClusters();
            assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
            assertThat(clusters.getTotal(), equalTo(1));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(1));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));

            assertNull(clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
        } finally {
            finishedResponse.decRef();
        }

        // check that the async_search/status response includes the same cluster details
        {
            AsyncStatusResponse statusResponse = getAsyncStatus(responseId);
            assertFalse(statusResponse.isPartial());

            SearchResponse.Clusters clusters = statusResponse.getClusters();
            assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
            assertThat(clusters.getTotal(), equalTo(1));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(1));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));

            assertNull(clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
        }
    }

    public void testRemoteClusterOnlyCCSWithFailuresOnOneShardOnly() throws Exception {
        // for remote-only queries, we can't use the SearchListenerPlugin since that listens for search
        // stage on the local cluster, so we only test final state of the search response
        SearchListenerPlugin.negate();

        Map<String, Object> testClusterInfo = setupTwoClusters();
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, 256));
        }
        boolean dfs = randomBoolean();
        if (dfs) {
            request.getSearchRequest().searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }
        // shardId 0 means to throw the Exception only on shard 0; all others should work
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("index corrupted"), 0);
        request.getSearchRequest().source(new SearchSourceBuilder().query(queryBuilder).size(10));

        final AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            assertNotNull(response.getSearchResponse());
        } finally {
            response.decRef();
        }
        waitForSearchTasksToFinish();

        final AsyncSearchResponse finishedResponse = getAsyncSearch(response.getId());
        try {
            assertTrue(finishedResponse.isPartial());

            SearchResponse.Clusters clusters = finishedResponse.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(1));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(1));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));

            assertNull(clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards - 1));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(1));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(1));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
            ShardSearchFailure remoteShardSearchFailure = remoteClusterSearchInfo.getFailures().get(0);
            assertTrue(
                "should have 'index corrupted' in reason but was: " + remoteShardSearchFailure.reason(),
                remoteShardSearchFailure.reason().contains("index corrupted")
            );
        } finally {
            finishedResponse.decRef();
        }
        // check that the async_search/status response includes the same cluster details
        {
            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            assertTrue(statusResponse.isPartial());

            SearchResponse.Clusters clusters = statusResponse.getClusters();
            assertThat(clusters.getTotal(), equalTo(1));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(1));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));

            assertNull(clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards - 1));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(1));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(1));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
            ShardSearchFailure remoteShardSearchFailure = remoteClusterSearchInfo.getFailures().get(0);
            assertTrue(
                "should have 'index corrupted' in reason but was: " + remoteShardSearchFailure.reason(),
                remoteShardSearchFailure.reason().contains("index corrupted")
            );
        }
    }

    public void testRemoteClusterOnlyCCSWithFailuresOnAllShards() throws Exception {
        // for remote-only queries, we can't use the SearchListenerPlugin since that listens for search
        // stage on the local cluster, so we only test final state of the search response
        SearchListenerPlugin.negate();

        Map<String, Object> testClusterInfo = setupTwoClusters();
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");
        boolean skipUnavailable = (Boolean) testClusterInfo.get("remote.skip_unavailable");

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, 256));
        }
        boolean dfs = randomBoolean();
        if (dfs) {
            request.getSearchRequest().searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }

        // shardId -1 means to throw the Exception on all shards, so should result in complete search failure
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("index corrupted"), -1);
        request.getSearchRequest().source(new SearchSourceBuilder().query(queryBuilder).size(10));

        boolean minimizeRoundtrips = TransportSearchAction.shouldMinimizeRoundtrips(request.getSearchRequest());

        final AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            assertNotNull(response.getSearchResponse());
        } finally {
            response.decRef();
        }

        waitForSearchTasksToFinish();
        final AsyncSearchResponse finishedResponse = getAsyncSearch(response.getId());
        try {
            assertTrue(finishedResponse.isPartial());

            SearchResponse.Clusters clusters = finishedResponse.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(1));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
            if (skipUnavailable) {
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(1));
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));
            } else {
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(1));
            }

            assertNull(clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            SearchResponse.Cluster.Status expectedStatus = skipUnavailable
                ? SearchResponse.Cluster.Status.SKIPPED
                : SearchResponse.Cluster.Status.FAILED;
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(expectedStatus));
            assertAllShardsFailed(minimizeRoundtrips, remoteClusterSearchInfo, remoteNumShards);
        } finally {
            finishedResponse.decRef();
        }
        // check that the async_search/status response includes the same cluster details
        {
            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            assertTrue(statusResponse.isPartial());

            SearchResponse.Clusters clusters = statusResponse.getClusters();
            assertThat(clusters.getTotal(), equalTo(1));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
            if (skipUnavailable) {
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(1));
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));
            } else {
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(1));
            }
            assertNull(clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            SearchResponse.Cluster.Status expectedStatus = skipUnavailable
                ? SearchResponse.Cluster.Status.SKIPPED
                : SearchResponse.Cluster.Status.FAILED;
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(expectedStatus));
            assertAllShardsFailed(minimizeRoundtrips, remoteClusterSearchInfo, remoteNumShards);
        }
    }

    public void testCancelViaTasksAPI() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        SearchListenerPlugin.blockQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, 256));
        }
        request.getSearchRequest().allowPartialSearchResults(false);
        request.getSearchRequest().source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));

        final AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            assertNotNull(response.getSearchResponse());
        } finally {
            response.decRef();
            assertTrue(response.isRunning());
        }

        SearchListenerPlugin.waitSearchStarted();

        ActionFuture<ListTasksResponse> cancelFuture;
        try {
            ListTasksResponse listTasksResponse = client(LOCAL_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .setActions(TransportSearchAction.TYPE.name())
                .get();
            List<TaskInfo> tasks = listTasksResponse.getTasks();
            assertThat(tasks.size(), equalTo(1));
            final TaskInfo rootTask = tasks.get(0);

            AtomicReference<List<TaskInfo>> remoteClusterSearchTasks = new AtomicReference<>();
            assertBusy(() -> {
                List<TaskInfo> remoteSearchTasks = client(REMOTE_CLUSTER).admin()
                    .cluster()
                    .prepareListTasks()
                    .get()
                    .getTasks()
                    .stream()
                    .filter(t -> t.action().contains(TransportSearchAction.TYPE.name()))
                    .collect(Collectors.toList());
                assertThat(remoteSearchTasks.size(), greaterThan(0));
                remoteClusterSearchTasks.set(remoteSearchTasks);
            });

            for (TaskInfo taskInfo : remoteClusterSearchTasks.get()) {
                assertFalse("taskInfo on remote cluster should not be cancelled yet: " + taskInfo, taskInfo.cancelled());
            }

            final CancelTasksRequest cancelRequest = new CancelTasksRequest().setTargetTaskId(rootTask.taskId());
            cancelRequest.setWaitForCompletion(randomBoolean());
            cancelFuture = client().admin().cluster().cancelTasks(cancelRequest);
            assertBusy(() -> {
                final Iterable<TransportService> transportServices = cluster(REMOTE_CLUSTER).getInstances(TransportService.class);
                for (TransportService transportService : transportServices) {
                    Collection<CancellableTask> cancellableTasks = transportService.getTaskManager().getCancellableTasks().values();
                    for (CancellableTask cancellableTask : cancellableTasks) {
                        if (cancellableTask.getAction().contains(TransportSearchAction.TYPE.name())) {
                            assertTrue(cancellableTask.getDescription(), cancellableTask.isCancelled());
                        }
                    }
                }
            });

            List<TaskInfo> remoteSearchTasksAfterCancellation = client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .get()
                .getTasks()
                .stream()
                .filter(t -> t.action().contains(TransportSearchAction.TYPE.name()))
                .toList();
            for (TaskInfo taskInfo : remoteSearchTasksAfterCancellation) {
                assertTrue(taskInfo.description(), taskInfo.cancelled());
            }

            // check async search status before allowing query to continue but after cancellation
            final AsyncSearchResponse searchResponseAfterCancellation = getAsyncSearch(response.getId());
            try {
                assertTrue(searchResponseAfterCancellation.isPartial());
                assertTrue(searchResponseAfterCancellation.isRunning());
                assertFalse(searchResponseAfterCancellation.getSearchResponse().isTimedOut());
                assertThat(searchResponseAfterCancellation.getSearchResponse().getClusters().getTotal(), equalTo(2));

                AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
                assertTrue(statusResponse.isPartial());
                assertTrue(statusResponse.isRunning());
                assertThat(statusResponse.getClusters().getTotal(), equalTo(2));
                assertNull(statusResponse.getCompletionStatus());
            } finally {
                searchResponseAfterCancellation.decRef();
            }

        } finally {
            SearchListenerPlugin.allowQueryPhase();
        }

        assertBusy(() -> assertTrue(cancelFuture.isDone()));

        waitForSearchTasksToFinish();

        AsyncStatusResponse statusResponseAfterCompletion = getAsyncStatus(response.getId());
        assertTrue(statusResponseAfterCompletion.isPartial());
        assertFalse(statusResponseAfterCompletion.isRunning());
        assertThat(statusResponseAfterCompletion.getClusters().getTotal(), equalTo(2));
        assertThat(statusResponseAfterCompletion.getCompletionStatus(), equalTo(RestStatus.BAD_REQUEST));

        final AsyncSearchResponse searchResponseAfterCompletion = getAsyncSearch(response.getId());
        try {
            assertTrue(searchResponseAfterCompletion.isPartial());
            assertFalse(searchResponseAfterCompletion.isRunning());
            assertFalse(searchResponseAfterCompletion.getSearchResponse().isTimedOut());
            assertThat(searchResponseAfterCompletion.getSearchResponse().getClusters().getTotal(), equalTo(2));
            Throwable cause = ExceptionsHelper.unwrap(searchResponseAfterCompletion.getFailure(), TaskCancelledException.class);
            assertNotNull("TaskCancelledException should be in the causal chain", cause);
            String json = Strings.toString(
                ChunkedToXContent.wrapAsToXContent(searchResponseAfterCompletion)
                    .toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)
            );
            assertThat(json, matchesRegex(".*task (was)?\s*cancelled.*"));
        } finally {
            searchResponseAfterCompletion.decRef();
        }
    }

    public void testCancelViaAsyncSearchDelete() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        SearchListenerPlugin.blockQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, 256));
        }
        request.getSearchRequest().allowPartialSearchResults(false);
        request.getSearchRequest().source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));

        final AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            assertNotNull(response.getSearchResponse());
            assertTrue(response.isRunning());
        } finally {
            response.decRef();
        }

        SearchListenerPlugin.waitSearchStarted();

        try {
            ListTasksResponse listTasksResponse = client(LOCAL_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .setActions(TransportSearchAction.TYPE.name())
                .get();
            List<TaskInfo> tasks = listTasksResponse.getTasks();
            assertThat(tasks.size(), equalTo(1));

            AtomicReference<List<TaskInfo>> remoteClusterSearchTasks = new AtomicReference<>();
            assertBusy(() -> {
                List<TaskInfo> remoteSearchTasks = client(REMOTE_CLUSTER).admin()
                    .cluster()
                    .prepareListTasks()
                    .get()
                    .getTasks()
                    .stream()
                    .filter(t -> t.action().contains(TransportSearchAction.TYPE.name()))
                    .collect(Collectors.toList());
                assertThat(remoteSearchTasks.size(), greaterThan(0));
                remoteClusterSearchTasks.set(remoteSearchTasks);
            });

            for (TaskInfo taskInfo : remoteClusterSearchTasks.get()) {
                assertFalse("taskInfo on remote cluster should not be cancelled yet: " + taskInfo, taskInfo.cancelled());
            }

            AcknowledgedResponse ack = deleteAsyncSearch(response.getId());
            assertTrue(ack.isAcknowledged());

            assertBusy(() -> {
                final Iterable<TransportService> transportServices = cluster(REMOTE_CLUSTER).getInstances(TransportService.class);
                for (TransportService transportService : transportServices) {
                    Collection<CancellableTask> cancellableTasks = transportService.getTaskManager().getCancellableTasks().values();
                    for (CancellableTask cancellableTask : cancellableTasks) {
                        if (cancellableTask.getAction().contains(TransportSearchAction.TYPE.name())) {
                            assertTrue(cancellableTask.getDescription(), cancellableTask.isCancelled());
                        }
                    }
                }
            });

            List<TaskInfo> remoteSearchTasksAfterCancellation = client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .get()
                .getTasks()
                .stream()
                .filter(t -> t.action().contains(TransportSearchAction.TYPE.name()))
                .toList();
            for (TaskInfo taskInfo : remoteSearchTasksAfterCancellation) {
                assertTrue(taskInfo.description(), taskInfo.cancelled());
            }

            ExecutionException e = expectThrows(ExecutionException.class, () -> getAsyncSearch(response.getId()));
            assertNotNull(e);
            assertNotNull(e.getCause());
            Throwable t = ExceptionsHelper.unwrap(e, ResourceNotFoundException.class);
            assertNotNull("after deletion, getAsyncSearch should throw an Exception with ResourceNotFoundException in the causal chain", t);

            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            assertTrue(statusResponse.isPartial());
            assertTrue(statusResponse.isRunning());
            assertThat(statusResponse.getClusters().getTotal(), equalTo(2));
            assertNull(statusResponse.getCompletionStatus());
        } finally {
            SearchListenerPlugin.allowQueryPhase();
        }

        waitForSearchTasksToFinish();

        assertBusy(() -> expectThrows(ExecutionException.class, () -> getAsyncStatus(response.getId())));
    }

    public void testCancellationViaTimeoutWithAllowPartialResultsSetToFalse() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        SearchListenerPlugin.blockQueryPhase();

        TimeValue searchTimeout = new TimeValue(100, TimeUnit.MILLISECONDS);
        // query builder that will sleep for the specified amount of time in the query phase
        SlowRunningQueryBuilder slowRunningQueryBuilder = new SlowRunningQueryBuilder(searchTimeout.millis() * 5);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(slowRunningQueryBuilder).timeout(searchTimeout);

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.getSearchRequest().source(sourceBuilder);
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, 256));
        }
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.getSearchRequest().allowPartialSearchResults(false);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);

        final AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            assertNotNull(response.getSearchResponse());
            assertTrue(response.isRunning());
        } finally {
            response.decRef();
        }

        SearchListenerPlugin.waitSearchStarted();

        // ensure tasks are present on both clusters and not cancelled
        try {
            ListTasksResponse listTasksResponse = client(LOCAL_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .setActions(TransportSearchAction.TYPE.name())
                .get();
            List<TaskInfo> tasks = listTasksResponse.getTasks();
            assertThat(tasks.size(), equalTo(1));

            AtomicReference<List<TaskInfo>> remoteClusterSearchTasks = new AtomicReference<>();
            assertBusy(() -> {
                List<TaskInfo> remoteSearchTasks = client(REMOTE_CLUSTER).admin()
                    .cluster()
                    .prepareListTasks()
                    .get()
                    .getTasks()
                    .stream()
                    .filter(t -> t.action().contains(TransportSearchAction.TYPE.name()))
                    .collect(Collectors.toList());
                assertThat(remoteSearchTasks.size(), greaterThan(0));
                remoteClusterSearchTasks.set(remoteSearchTasks);
            });

            for (TaskInfo taskInfo : remoteClusterSearchTasks.get()) {
                assertFalse("taskInfo on remote cluster should not be cancelled yet: " + taskInfo, taskInfo.cancelled());
            }

        } finally {
            SearchListenerPlugin.allowQueryPhase();
        }

        // query phase has begun, so wait for query failure (due to timeout)
        SearchListenerPlugin.waitQueryFailure();

        // wait for search tasks to complete and be unregistered
        assertBusy(() -> {
            ListTasksResponse listTasksResponse = client(LOCAL_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .setActions(TransportSearchAction.TYPE.name())
                .get();
            List<TaskInfo> tasks = listTasksResponse.getTasks();
            assertThat(tasks.size(), equalTo(0));

            ListTasksResponse remoteTasksResponse = client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .setActions(TransportSearchAction.TYPE.name())
                .get();
            List<TaskInfo> remoteTasks = remoteTasksResponse.getTasks();
            assertThat(remoteTasks.size(), equalTo(0));
        });

        AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
        assertFalse(statusResponse.isRunning());
        assertTrue(statusResponse.isPartial());

        assertEquals(0, statusResponse.getSuccessfulShards());
        assertEquals(0, statusResponse.getSkippedShards());
        assertThat(statusResponse.getFailedShards(), greaterThanOrEqualTo(1));

        waitForSearchTasksToFinish();
    }

    private void waitForSearchTasksToFinish() throws Exception {
        assertBusy(() -> {
            ListTasksResponse listTasksResponse = client(LOCAL_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .setActions(TransportSearchAction.TYPE.name())
                .get();
            List<TaskInfo> tasks = listTasksResponse.getTasks();
            assertThat(tasks.size(), equalTo(0));

            ListTasksResponse remoteTasksResponse = client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .setActions(TransportSearchAction.TYPE.name())
                .get();
            List<TaskInfo> remoteTasks = remoteTasksResponse.getTasks();
            assertThat(remoteTasks.size(), equalTo(0));
        });

        assertBusy(() -> {
            final Iterable<TransportService> transportServices = cluster(REMOTE_CLUSTER).getInstances(TransportService.class);
            for (TransportService transportService : transportServices) {
                assertThat(transportService.getTaskManager().getBannedTaskIds(), Matchers.empty());
            }
        });
    }

    private static void assertAllShardsFailed(boolean minimizeRoundtrips, SearchResponse.Cluster cluster, int numShards) {
        if (minimizeRoundtrips) {
            assertNull(cluster.getTotalShards());
            assertNull(cluster.getSuccessfulShards());
            assertNull(cluster.getSkippedShards());
            assertNull(cluster.getFailedShards());
            assertThat(cluster.getFailures().size(), equalTo(1));
        } else {
            assertThat(cluster.getTotalShards(), equalTo(numShards));
            assertThat(cluster.getSuccessfulShards(), equalTo(0));
            assertThat(cluster.getSkippedShards(), equalTo(0));
            assertThat(cluster.getFailedShards(), equalTo(numShards));
            assertThat(cluster.getFailures().size(), equalTo(numShards));
        }
        assertNull(cluster.getTook());
        assertFalse(cluster.isTimedOut());
        ShardSearchFailure shardSearchFailure = cluster.getFailures().get(0);
        assertTrue(
            "should have 'index corrupted' in reason but was: " + shardSearchFailure.reason(),
            shardSearchFailure.reason().contains("index corrupted")
        );
    }

    protected AsyncSearchResponse submitAsyncSearch(SubmitAsyncSearchRequest request) throws ExecutionException, InterruptedException {
        return client(LOCAL_CLUSTER).execute(SubmitAsyncSearchAction.INSTANCE, request).get();
    }

    protected AsyncSearchResponse getAsyncSearch(String id) throws ExecutionException, InterruptedException {
        return client(LOCAL_CLUSTER).execute(GetAsyncSearchAction.INSTANCE, new GetAsyncResultRequest(id)).get();
    }

    protected AsyncStatusResponse getAsyncStatus(String id) throws ExecutionException, InterruptedException {
        return client(LOCAL_CLUSTER).execute(GetAsyncStatusAction.INSTANCE, new GetAsyncStatusRequest(id)).get();
    }

    protected AcknowledgedResponse deleteAsyncSearch(String id) throws ExecutionException, InterruptedException {
        return client().execute(TransportDeleteAsyncResultAction.TYPE, new DeleteAsyncResultRequest(id)).get();
    }

    private Map<String, Object> setupTwoClusters() {
        String localIndex = "demo";
        int numShardsLocal = randomIntBetween(2, 12);
        Settings localSettings = indexSettings(numShardsLocal, randomIntBetween(0, 1)).build();
        assertAcked(
            client(LOCAL_CLUSTER).admin()
                .indices()
                .prepareCreate(localIndex)
                .setSettings(localSettings)
                .setMapping("@timestamp", "type=date", "f", "type=text")
        );
        indexDocs(client(LOCAL_CLUSTER), localIndex);

        String remoteIndex = "prod";
        int numShardsRemote = randomIntBetween(2, 12);
        final InternalTestCluster remoteCluster = cluster(REMOTE_CLUSTER);
        remoteCluster.ensureAtLeastNumDataNodes(randomIntBetween(1, 3));
        final Settings.Builder remoteSettings = Settings.builder();
        remoteSettings.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShardsRemote);
        remoteSettings.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 1));

        assertAcked(
            client(REMOTE_CLUSTER).admin()
                .indices()
                .prepareCreate(remoteIndex)
                .setSettings(Settings.builder().put(remoteSettings.build()))
                .setMapping("@timestamp", "type=date", "f", "type=text")
        );
        assertFalse(
            client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareHealth(TEST_REQUEST_TIMEOUT, remoteIndex)
                .setWaitForYellowStatus()
                .setTimeout(TimeValue.timeValueSeconds(10))
                .get()
                .isTimedOut()
        );
        indexDocs(client(REMOTE_CLUSTER), remoteIndex);

        String skipUnavailableKey = Strings.format("cluster.remote.%s.skip_unavailable", REMOTE_CLUSTER);
        Setting<?> skipUnavailableSetting = cluster(REMOTE_CLUSTER).clusterService().getClusterSettings().get(skipUnavailableKey);
        boolean skipUnavailable = (boolean) cluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).clusterService()
            .getClusterSettings()
            .get(skipUnavailableSetting);

        Map<String, Object> clusterInfo = new HashMap<>();
        clusterInfo.put("local.num_shards", numShardsLocal);
        clusterInfo.put("local.index", localIndex);
        clusterInfo.put("remote.num_shards", numShardsRemote);
        clusterInfo.put("remote.index", remoteIndex);
        clusterInfo.put("remote.skip_unavailable", skipUnavailable);
        return clusterInfo;
    }

    private int indexDocs(Client client, String index) {
        int numDocs = between(500, 1200);
        for (int i = 0; i < numDocs; i++) {
            long ts = EARLIEST_TIMESTAMP + i;
            if (i == numDocs - 1) {
                ts = LATEST_TIMESTAMP;
            }
            client.prepareIndex(index).setSource("f", "v", "@timestamp", ts).get();
        }
        client.admin().indices().prepareRefresh(index).get();
        return numDocs;
    }

    @Before
    public void resetSearchListenerPlugin() throws Exception {
        SearchListenerPlugin.reset();
    }

    public static class SearchListenerPlugin extends Plugin {
        private static final AtomicReference<CountDownLatch> startedLatch = new AtomicReference<>();
        private static final AtomicReference<CountDownLatch> queryLatch = new AtomicReference<>();
        private static final AtomicReference<CountDownLatch> failedQueryLatch = new AtomicReference<>();

        /**
         * For tests that cannot use SearchListenerPlugin, ensure all latches are unset to
         * avoid test problems around searches of the .async-search index
         */
        static void negate() {
            if (startedLatch.get() != null) {
                startedLatch.get().countDown();
            }
            if (queryLatch.get() != null) {
                queryLatch.get().countDown();
            }
            if (failedQueryLatch.get() != null) {
                failedQueryLatch.get().countDown();
            }
        }

        static void reset() {
            startedLatch.set(new CountDownLatch(1));
            failedQueryLatch.set(new CountDownLatch(1));
        }

        static void blockQueryPhase() {
            queryLatch.set(new CountDownLatch(1));
        }

        static void allowQueryPhase() {
            final CountDownLatch latch = queryLatch.get();
            if (latch != null) {
                latch.countDown();
            }
        }

        static void waitSearchStarted() throws InterruptedException {
            assertTrue(startedLatch.get().await(60, TimeUnit.SECONDS));
        }

        static void waitQueryFailure() throws Exception {
            assertTrue(failedQueryLatch.get().await(60, TimeUnit.SECONDS));
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onNewReaderContext(ReaderContext readerContext) {
                    assertThat(readerContext, not(instanceOf(LegacyReaderContext.class)));
                }

                @Override
                public void onPreQueryPhase(SearchContext searchContext) {
                    startedLatch.get().countDown();
                    final CountDownLatch latch = queryLatch.get();
                    if (latch != null) {
                        try {
                            assertTrue(latch.await(60, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                    }
                }

                @Override
                public void onFailedQueryPhase(SearchContext searchContext) {
                    // only count failed queries that have a timeout set (to be sure we are listening for our test query)
                    if (searchContext.timeout().millis() > -1) {
                        if (failedQueryLatch.get().getCount() > 0) {
                            failedQueryLatch.get().countDown();
                        }
                    }
                }
            });
            super.onIndexModule(indexModule);
        }
    }
}
