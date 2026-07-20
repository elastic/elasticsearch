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
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
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
import org.elasticsearch.index.store.DirectoryMetrics;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreMetrics;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
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
import org.elasticsearch.test.junit.annotations.TestLogging;
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
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@TestLogging(
    reason = "testing debug log output to identify race condition",
    value = "org.elasticsearch.xpack.search.MutableSearchResponse:DEBUG,org.elasticsearch.xpack.search.AsyncSearchTask:DEBUG"
)
public class CrossClusterAsyncSearchIT extends AbstractMultiClustersTestCase {

    public static final String LOCAL_CLUSTER = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
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

        SearchListenerPlugin.blockLocalQueryPhase();
        SearchListenerPlugin.blockRemoteQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        request.getSearchRequest().source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, Math.max(localNumShards, remoteNumShards) + 1));
        }
        boolean dfs = randomBoolean();
        if (dfs) {
            request.getSearchRequest().searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }

        final String responseId;
        final AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            responseId = response.getId();
            assertInitialClusterDetailsState(response, SearchResponse.Cluster.Status.RUNNING);
        } finally {
            response.decRef();
        }

        SearchListenerPlugin.waitLocalSearchStarted();
        SearchListenerPlugin.waitRemoteSearchStarted();
        SearchListenerPlugin.allowLocalQueryPhase();
        SearchListenerPlugin.allowRemoteQueryPhase();

        waitForSearchTasksToFinish();
        final AsyncSearchResponse finishedResponse = getAsyncSearch(responseId);
        try {
            assertFalse(finishedResponse.isPartial());

            assertNotNull(finishedResponse.getSearchResponse());
            assertClusterDetailsSuccessful(finishedResponse.getSearchResponse().getClusters(), localNumShards, remoteNumShards, true);
        } finally {
            finishedResponse.decRef();
        }

        // check that the async_search/status response includes the same cluster details
        AsyncStatusResponse statusResponse = getAsyncStatus(responseId);
        assertFalse(statusResponse.isPartial());

        assertClusterDetailsSuccessful(statusResponse.getClusters(), localNumShards, remoteNumShards, true);
    }

    // CCS with a search where the timestamp of the query cannot match so should be SUCCESSFUL with all shards skipped
    // during can-match
    public void testCCSClusterDetailsWhereAllShardsSkippedInCanMatch() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        SearchListenerPlugin.blockLocalQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, Math.max(localNumShards, remoteNumShards) + 1));
        }
        boolean dfs = randomBoolean();
        if (dfs) {
            request.getSearchRequest().searchType(SearchType.DFS_QUERY_THEN_FETCH);
        } else {
            request.getSearchRequest().searchType(SearchType.QUERY_THEN_FETCH);
        }
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder("@timestamp").from(100).to(2000);
        request.getSearchRequest().source(new SearchSourceBuilder().query(rangeQueryBuilder).size(10));

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
            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(LOCAL_CLUSTER);
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
            SearchListenerPlugin.waitLocalSearchStarted();
        }
        SearchListenerPlugin.allowLocalQueryPhase();

        waitForSearchTasksToFinish();
        final AsyncSearchResponse finishedResponse = getAsyncSearch(responseId);
        try {
            assertNotNull(finishedResponse);
            assertFalse(finishedResponse.isPartial());
            assertNotNull(finishedResponse.getSearchResponse());
            assertClusterDetailsAllShardsSkipped(finishedResponse.getSearchResponse().getClusters(), dfs, localNumShards, remoteNumShards);
        } finally {
            finishedResponse.decRef();
        }

        // check that the async_search/status response includes the same cluster details
        AsyncStatusResponse statusResponse = getAsyncStatus(responseId);
        assertNotNull(statusResponse);
        assertFalse(statusResponse.isPartial());

        assertClusterDetailsAllShardsSkipped(statusResponse.getClusters(), dfs, localNumShards, remoteNumShards);
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
            request.setBatchedReduceSize(randomIntBetween(2, Math.max(localNumShards, remoteNumShards) + 1));
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
            assertNotNull(finishedResponse.getSearchResponse());
            assertClusterDetailsAllShardsFailed(
                finishedResponse.getSearchResponse().getClusters(),
                skipUnavailable,
                minimizeRoundtrips,
                localNumShards,
                remoteNumShards,
                true
            );
        } finally {
            finishedResponse.decRef();
        }
        // check that the async_search/status response includes the same cluster details
        AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
        assertTrue(statusResponse.isPartial());

        assertClusterDetailsAllShardsFailed(
            statusResponse.getClusters(),
            skipUnavailable,
            minimizeRoundtrips,
            localNumShards,
            remoteNumShards,
            true
        );
    }

    public void testClusterDetailsAfterCCSWithFailuresOnOneShardOnly() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        SearchListenerPlugin.blockLocalQueryPhase();
        SearchListenerPlugin.blockRemoteQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, Math.max(localNumShards, remoteNumShards) + 1));
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
            assertInitialClusterDetailsState(response, SearchResponse.Cluster.Status.RUNNING);
        } finally {
            response.decRef();
        }

        SearchListenerPlugin.waitLocalSearchStarted();
        SearchListenerPlugin.waitRemoteSearchStarted();
        SearchListenerPlugin.allowLocalQueryPhase();
        SearchListenerPlugin.allowRemoteQueryPhase();

        waitForSearchTasksToFinish();

        final AsyncSearchResponse finishedResponse = getAsyncSearch(response.getId());
        try {
            assertTrue(finishedResponse.isPartial());
            assertNotNull(finishedResponse.getSearchResponse());
            assertClusterDetailsFailureOnOneShardOnly(
                finishedResponse.getSearchResponse().getClusters(),
                localNumShards,
                remoteNumShards,
                true
            );
        } finally {
            finishedResponse.decRef();
        }
        // check that the async_search/status response includes the same cluster details
        {
            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            assertTrue(statusResponse.isPartial());

            assertClusterDetailsFailureOnOneShardOnly(statusResponse.getClusters(), localNumShards, remoteNumShards, true);
        }
    }

    /**
     * This tests the specific case where allow_partial_search_results=false and ccs_minimize_roundtrips=false. Previously, this
     * combination of settings would result in the cluster status getting stuck as {@link SearchResponse.Cluster.Status#RUNNING}
     */
    public void testClusterDetailsAfterCCSWithFailuresOnOneShardOnly_AllowPartialResultsFalse_MinimizeRoundtripsFalse() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");
        boolean skipUnavailable = (Boolean) testClusterInfo.get("remote.skip_unavailable");

        SearchListenerPlugin.blockLocalQueryPhase();
        SearchListenerPlugin.blockRemoteQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(false);
        request.getSearchRequest().allowPartialSearchResults(false);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, Math.max(localNumShards, remoteNumShards) + 1));
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
            assertInitialClusterDetailsState(response, SearchResponse.Cluster.Status.RUNNING);
        } finally {
            response.decRef();
        }

        SearchListenerPlugin.waitLocalSearchStarted();
        SearchListenerPlugin.waitRemoteSearchStarted();
        SearchListenerPlugin.allowLocalQueryPhase();
        SearchListenerPlugin.allowRemoteQueryPhase();

        waitForSearchTasksToFinish();

        final AsyncSearchResponse finishedResponse = getAsyncSearch(response.getId());
        try {
            assertTrue(finishedResponse.isPartial());
            assertNotNull(finishedResponse.getSearchResponse());
            assertClusterDetailsFailureOnOneShardOnlyAllowPartialResultsFalseMinimizeRoundtripsFalse(
                finishedResponse.getSearchResponse().getClusters(),
                skipUnavailable,
                dfs,
                localNumShards,
                remoteNumShards,
                true
            );
        } finally {
            finishedResponse.decRef();
        }
        // check that the async_search/status response includes the same cluster details
        {
            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            assertTrue(statusResponse.isPartial());

            assertClusterDetailsFailureOnOneShardOnlyAllowPartialResultsFalseMinimizeRoundtripsFalse(
                statusResponse.getClusters(),
                skipUnavailable,
                dfs,
                localNumShards,
                remoteNumShards,
                true
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

        SearchListenerPlugin.blockLocalQueryPhase();
        SearchListenerPlugin.blockRemoteQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, Math.max(localNumShards, remoteNumShards) + 1));
        }
        boolean dfs = randomBoolean();
        if (dfs) {
            request.getSearchRequest().searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }

        // throw Exception for all shards of remoteIndex, but not against localIndex
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(
            randomLong(),
            new IllegalStateException("index corrupted"),
            remoteIndex
        );
        request.getSearchRequest().source(new SearchSourceBuilder().query(queryBuilder).size(10));

        boolean minimizeRoundtrips = TransportSearchAction.shouldMinimizeRoundtrips(request.getSearchRequest());

        final AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            assertInitialClusterDetailsState(response, SearchResponse.Cluster.Status.RUNNING);
        } finally {
            response.decRef();
        }

        SearchListenerPlugin.waitLocalSearchStarted();
        SearchListenerPlugin.waitRemoteSearchStarted();
        SearchListenerPlugin.allowLocalQueryPhase();
        SearchListenerPlugin.allowRemoteQueryPhase();

        waitForSearchTasksToFinish();

        final AsyncSearchResponse finishedResponse = getAsyncSearch(response.getId());
        try {
            assertTrue(finishedResponse.isPartial());
            assertNotNull(finishedResponse.getSearchResponse());
            assertClusterDetailsFailuresOnOneCluster(
                finishedResponse.getSearchResponse().getClusters(),
                skipUnavailable,
                minimizeRoundtrips,
                localNumShards,
                remoteNumShards
            );
        } finally {
            finishedResponse.decRef();
        }
        // check that the async_search/status response includes the same cluster details
        {
            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            assertTrue(statusResponse.isPartial());
            assertClusterDetailsFailuresOnOneCluster(
                statusResponse.getClusters(),
                skipUnavailable,
                minimizeRoundtrips,
                localNumShards,
                remoteNumShards
            );
        }
    }

    /**
     * This tests the specific case where allow_partial_search_results=false and ccs_minimize_roundtrips=false. Previously, this
     * combination of settings would result in the cluster status getting stuck as {@link SearchResponse.Cluster.Status#RUNNING} on the
     * local cluster
     */
    public void testClusterDetailsAfterCCSWithFailuresOnOneClusterOnly_AllowPartialResultsFalse_MinimizeRoundtripsFalse() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");
        boolean skipUnavailable = (Boolean) testClusterInfo.get("remote.skip_unavailable");

        SearchListenerPlugin.blockLocalQueryPhase();
        SearchListenerPlugin.blockRemoteQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        boolean minimizeRoundtrips = false;
        request.setCcsMinimizeRoundtrips(minimizeRoundtrips);
        request.getSearchRequest().allowPartialSearchResults(false);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, Math.max(localNumShards, remoteNumShards) + 1));
        }
        boolean dfs = randomBoolean();
        if (dfs) {
            request.getSearchRequest().searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }

        // throw Exception for all shards of remoteIndex, but not against localIndex
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(
            randomLong(),
            new IllegalStateException("index corrupted"),
            remoteIndex
        );
        request.getSearchRequest().source(new SearchSourceBuilder().query(queryBuilder).size(10));

        final AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            assertInitialClusterDetailsState(response, SearchResponse.Cluster.Status.RUNNING);
        } finally {
            response.decRef();
        }

        SearchListenerPlugin.waitLocalSearchStarted();
        SearchListenerPlugin.waitRemoteSearchStarted();
        SearchListenerPlugin.allowLocalQueryPhase();
        SearchListenerPlugin.allowRemoteQueryPhase();

        waitForSearchTasksToFinish();

        final AsyncSearchResponse finishedResponse = getAsyncSearch(response.getId());
        try {
            assertTrue(finishedResponse.isPartial());
            assertNotNull(finishedResponse.getSearchResponse());
            assertClustersDetailsFailuresOnOneClusterOnlyAllowPartialResultsFalseMinimizeRoundtripsFalse(
                finishedResponse.getSearchResponse().getClusters(),
                skipUnavailable,
                dfs,
                localNumShards,
                remoteNumShards
            );
        } finally {
            finishedResponse.decRef();
        }
        // check that the async_search/status response includes the same cluster details
        AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
        assertTrue(statusResponse.isPartial());

        assertClustersDetailsFailuresOnOneClusterOnlyAllowPartialResultsFalseMinimizeRoundtripsFalse(
            statusResponse.getClusters(),
            skipUnavailable,
            dfs,
            localNumShards,
            remoteNumShards
        );
    }

    // tests bug fix https://github.com/elastic/elasticsearch/issues/100350
    public void testClusterDetailsAfterCCSWhereRemoteClusterHasNoShardsToSearch() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");

        SearchListenerPlugin.blockLocalQueryPhase();

        // query against a missing index on the remote cluster
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + "no_such_index*");
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        request.getSearchRequest().source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, localNumShards + 1));
        }
        boolean dfs = randomBoolean();
        if (dfs) {
            request.getSearchRequest().searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }

        final AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            assertInitialClusterDetailsState(response, SearchResponse.Cluster.Status.SUCCESSFUL);
        } finally {
            response.decRef();
        }

        SearchListenerPlugin.waitLocalSearchStarted();
        SearchListenerPlugin.allowLocalQueryPhase();

        waitForSearchTasksToFinish();

        final AsyncSearchResponse finishedResponse = getAsyncSearch(response.getId());
        try {
            assertFalse(finishedResponse.isPartial());
            assertNotNull(finishedResponse.getSearchResponse());
            assertClusterDetailsSuccessful(finishedResponse.getSearchResponse().getClusters(), localNumShards, 0, true);
        } finally {
            finishedResponse.decRef();
        }
        // check that the async_search/status response includes the same cluster details
        AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
        assertFalse(statusResponse.isPartial());
        assertClusterDetailsSuccessful(statusResponse.getClusters(), localNumShards, 0, true);
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
            request.setBatchedReduceSize(randomIntBetween(2, Math.max(localNumShards, remoteNumShards) + 1));
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
            responseId = response.getId();
        } finally {
            response.decRef();
        }

        waitForSearchTasksToFinish();

        final AsyncSearchResponse finishedResponse = getAsyncSearch(responseId);
        try {
            assertTrue(finishedResponse.getSearchResponse().isTimedOut());
            assertTrue(finishedResponse.isPartial());

            assertClusterDetailsSearchTimeout(finishedResponse.getSearchResponse().getClusters(), localNumShards, remoteNumShards);
        } finally {
            finishedResponse.decRef();
        }
        // check that the async_search/status response includes the same cluster details
        AsyncStatusResponse statusResponse = getAsyncStatus(responseId);
        assertTrue(statusResponse.isPartial());
        assertClusterDetailsSearchTimeout(statusResponse.getClusters(), localNumShards, remoteNumShards);
    }

    public void testGetResultIntermediateResultsFalseOnRunningSearchDoesNotIncludeIntermediateResults() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        SearchListenerPlugin.blockLocalQueryPhase();
        SearchListenerPlugin.blockRemoteQueryPhase();

        AggregationBuilder agg = AggregationBuilders.max("max").field("@timestamp");
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(true);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        request.getSearchRequest().source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10).aggregation(agg));

        final String responseId;
        final AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            responseId = response.getId();
            assertInitialClusterDetailsState(response, SearchResponse.Cluster.Status.RUNNING);
        } finally {
            response.decRef();
        }

        final AsyncSearchResponse runningResponse = getAsyncSearch(responseId, true);
        try {
            // Test that a response object with no intermediate results will not be fleshed out with hits and aggs
            assertTrue(runningResponse.isRunning());
            assertTrue(runningResponse.isPartial());
            assertThat(runningResponse.getSearchResponse().getHits().getHits().length, equalTo(0));
            assertThat(runningResponse.getSearchResponse().getAggregations(), notNullValue());
            assertThat(runningResponse.getSearchResponse().getAggregations().asList(), hasSize(0));
        } finally {
            runningResponse.decRef();
        }

        SearchListenerPlugin.allowLocalQueryPhase();
        final AsyncSearchResponse localQueryDoneWithIntermediateResponse = getAsyncSearch(responseId, true);
        try {
            // Test that a response object for an intermediate response will be fleshed out with the intermediate response if requested
            assertTrue(localQueryDoneWithIntermediateResponse.isRunning());
            assertTrue(localQueryDoneWithIntermediateResponse.isPartial());
            assertThat(localQueryDoneWithIntermediateResponse.getSearchResponse().getHits().getHits().length, equalTo(10));
            assertThat(localQueryDoneWithIntermediateResponse.getSearchResponse().getAggregations(), notNullValue());
            assertThat(localQueryDoneWithIntermediateResponse.getSearchResponse().getAggregations().asList(), hasSize(1));
        } finally {
            localQueryDoneWithIntermediateResponse.decRef();
        }

        final AsyncSearchResponse localQueryDoneWithNoIntermediateResponse = getAsyncSearch(responseId, false);
        try {
            // Test that a response object is not fleshed out with hits and aggs when intermediate results are disabled
            assertTrue(localQueryDoneWithNoIntermediateResponse.isRunning());
            assertTrue(localQueryDoneWithNoIntermediateResponse.isPartial());
            assertThat(localQueryDoneWithNoIntermediateResponse.getSearchResponse().getHits().getHits().length, equalTo(0));
            assertThat(localQueryDoneWithNoIntermediateResponse.getSearchResponse().getAggregations(), nullValue());
        } finally {
            localQueryDoneWithNoIntermediateResponse.decRef();
        }

        SearchListenerPlugin.allowRemoteQueryPhase();
        waitForSearchTasksToFinish();
        final AsyncSearchResponse finishedResponse = getAsyncSearch(responseId, randomBoolean());
        try {
            assertFalse(finishedResponse.isPartial());
            assertFalse(finishedResponse.isRunning());
            assertThat(finishedResponse.getSearchResponse().getHits().getHits().length, equalTo(10));
            assertThat(finishedResponse.getSearchResponse().getAggregations(), notNullValue());
            assertThat(finishedResponse.getSearchResponse().getAggregations().asList(), hasSize(1));

            assertClusterDetailsSuccessful(finishedResponse.getSearchResponse().getClusters(), localNumShards, remoteNumShards, true);
        } finally {
            finishedResponse.decRef();
        }

        // check that the async_search/status response includes the same cluster details
        AsyncStatusResponse statusResponse = getAsyncStatus(responseId);
        assertFalse(statusResponse.isPartial());

        assertClusterDetailsSuccessful(statusResponse.getClusters(), localNumShards, remoteNumShards, true);
    }

    public void testGetResultIntermediateResultsFalseOnRunningSearchDoesNotIncludeIntermediateResultsCcsMrtFalse() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        SearchListenerPlugin.blockLocalQueryPhase();
        SearchListenerPlugin.blockRemoteQueryPhase();

        // To ensure that we get reduced partial results, we'll allow both local and remote query phase to complete but block both local
        // and remote fetch phases. In this state, the search is still running and setting return_intermediate_results to true or false
        // should have different outcomes asserted below.
        SearchListenerPlugin.blockLocalFetchPhase();
        SearchListenerPlugin.blockRemoteFetchPhase();
        SearchListenerPlugin.blockLocalQueryPhaseCompletion(localNumShards);

        AggregationBuilder agg = AggregationBuilders.max("max").field("@timestamp");
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(false);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        request.setBatchedReduceSize(2);
        request.getSearchRequest().source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10).aggregation(agg));

        final String responseId;
        final AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            responseId = response.getId();
            assertInitialClusterDetailsState(response, SearchResponse.Cluster.Status.RUNNING);
        } finally {
            response.decRef();
        }

        final AsyncSearchResponse runningResponse = getAsyncSearch(responseId, true);
        try {
            // Test that a response object with no intermediate results will not be fleshed out with aggs
            assertTrue(runningResponse.isRunning());
            assertTrue(runningResponse.isPartial());
            assertThat(runningResponse.getSearchResponse().getAggregations(), nullValue());
        } finally {
            runningResponse.decRef();
        }

        SearchListenerPlugin.allowLocalQueryPhase();
        SearchListenerPlugin.allowRemoteQueryPhase();
        SearchListenerPlugin.waitForLocalQueryPhaseCompletion();
        final AsyncSearchResponse localQueryDoneNoPartialResponse = getAsyncSearch(responseId, false);
        try {
            // Test that a response object is not fleshed out with aggs when intermediate results are disabled
            assertTrue(localQueryDoneNoPartialResponse.isRunning());
            assertTrue(localQueryDoneNoPartialResponse.isPartial());
            assertThat(localQueryDoneNoPartialResponse.getSearchResponse().getAggregations(), nullValue());
        } finally {
            localQueryDoneNoPartialResponse.decRef();
        }

        final AsyncSearchResponse localQueryDoneWithIntermediateResponse = getAsyncSearch(responseId, true);
        try {
            // Test that a response object for a partial response will be fleshed out with the intermediate response if requested
            assertTrue(localQueryDoneWithIntermediateResponse.isRunning());
            assertTrue(localQueryDoneWithIntermediateResponse.isPartial());
            assertThat(localQueryDoneWithIntermediateResponse.getSearchResponse().getAggregations(), notNullValue());
            assertThat(localQueryDoneWithIntermediateResponse.getSearchResponse().getAggregations().asList(), hasSize(1));
        } finally {
            localQueryDoneWithIntermediateResponse.decRef();
        }

        SearchListenerPlugin.allowLocalFetchPhase();
        SearchListenerPlugin.allowRemoteFetchPhase();
        waitForSearchTasksToFinish();
        final AsyncSearchResponse finishedResponse = getAsyncSearch(responseId, randomBoolean());
        try {
            assertFalse(finishedResponse.isPartial());
            assertFalse(finishedResponse.isRunning());
            assertThat(finishedResponse.getSearchResponse().getAggregations(), notNullValue());
            assertThat(finishedResponse.getSearchResponse().getAggregations().asList(), hasSize(1));
            assertClusterDetailsSuccessful(finishedResponse.getSearchResponse().getClusters(), localNumShards, remoteNumShards, true);
        } finally {
            finishedResponse.decRef();
        }

        // check that the async_search/status response includes the same cluster details
        AsyncStatusResponse statusResponse = getAsyncStatus(responseId);
        assertFalse(statusResponse.isPartial());
        assertClusterDetailsSuccessful(statusResponse.getClusters(), localNumShards, remoteNumShards, true);
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
            request.setBatchedReduceSize(randomIntBetween(2, remoteNumShards + 1));
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
            assertNotNull(finishedResponse.getSearchResponse());
            assertClusterDetailsSuccessful(finishedResponse.getSearchResponse().getClusters(), 0, remoteNumShards, false);
        } finally {
            finishedResponse.decRef();
        }

        // check that the async_search/status response includes the same cluster details
        AsyncStatusResponse statusResponse = getAsyncStatus(responseId);
        assertFalse(statusResponse.isPartial());
        assertClusterDetailsSuccessful(statusResponse.getClusters(), 0, remoteNumShards, false);
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
            request.setBatchedReduceSize(randomIntBetween(2, remoteNumShards + 1));
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
            assertNotNull(finishedResponse.getSearchResponse());
            assertClusterDetailsFailureOnOneShardOnly(finishedResponse.getSearchResponse().getClusters(), 0, remoteNumShards, false);
        } finally {
            finishedResponse.decRef();
        }
        // check that the async_search/status response includes the same cluster details
        AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
        assertTrue(statusResponse.isPartial());
        assertClusterDetailsFailureOnOneShardOnly(statusResponse.getClusters(), 0, remoteNumShards, false);
    }

    /**
     * This tests the specific case where allow_partial_search_results=false and ccs_minimize_roundtrips=false. Previously, this
     * combination of settings would result in the cluster status getting stuck as {@link SearchResponse.Cluster.Status#RUNNING} on the
     * local cluster
     */
    public void testRemoteClusterOnlyCCSWithFailuresOnOneShardOnly_AllowPartialResultsFalse_MinimizeRoundtripsFalse() throws Exception {
        // for remote-only queries, we can't use the SearchListenerPlugin since that listens for search
        // stage on the local cluster, so we only test final state of the search response
        SearchListenerPlugin.negate();

        Map<String, Object> testClusterInfo = setupTwoClusters();
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");
        boolean skipUnavailable = (Boolean) testClusterInfo.get("remote.skip_unavailable");

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(false);
        request.getSearchRequest().allowPartialSearchResults(false);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, remoteNumShards + 1));
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
            assertNotNull(finishedResponse.getSearchResponse());
            assertClusterDetailsFailureOnOneShardOnlyAllowPartialResultsFalseMinimizeRoundtripsFalse(
                finishedResponse.getSearchResponse().getClusters(),
                skipUnavailable,
                dfs,
                0,
                remoteNumShards,
                false
            );
        } finally {
            finishedResponse.decRef();
        }
        // check that the async_search/status response includes the same cluster details
        AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
        assertTrue(statusResponse.isPartial());
        assertClusterDetailsFailureOnOneShardOnlyAllowPartialResultsFalseMinimizeRoundtripsFalse(
            statusResponse.getClusters(),
            skipUnavailable,
            dfs,
            0,
            remoteNumShards,
            false
        );
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
            request.setBatchedReduceSize(randomIntBetween(2, remoteNumShards + 1));
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
        int localNumShards = 0;
        boolean localIncluded = false;
        try {
            assertTrue(finishedResponse.isPartial());
            assertNotNull(finishedResponse.getSearchResponse());
            assertClusterDetailsAllShardsFailed(
                finishedResponse.getSearchResponse().getClusters(),
                skipUnavailable,
                minimizeRoundtrips,
                localNumShards,
                remoteNumShards,
                localIncluded
            );

        } finally {
            finishedResponse.decRef();
        }
        // check that the async_search/status response includes the same cluster details
        AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
        assertTrue(statusResponse.isPartial());
        assertClusterDetailsAllShardsFailed(
            statusResponse.getClusters(),
            skipUnavailable,
            minimizeRoundtrips,
            localNumShards,
            remoteNumShards,
            localIncluded
        );
    }

    /**
     * This test verifies that get async search triggers an automatic task cancellation when trying to retrieve
     * results for an expired async search
     */
    public void testCancelViaExpirationOnGetAsyncSearchWithMinimizeRoundtrips() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        SearchListenerPlugin.blockLocalQueryPhase();
        SearchListenerPlugin.blockRemoteQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(true);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepAlive(new TimeValue(1, TimeUnit.SECONDS));
        request.setKeepOnCompletion(true);

        final AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            assertNotNull(response.getSearchResponse());
        } finally {
            response.decRef();
            assertTrue(response.isRunning());
        }

        SearchListenerPlugin.waitLocalSearchStarted();
        SearchListenerPlugin.waitRemoteSearchStarted();

        ListTasksResponse listTasksResponse = client(LOCAL_CLUSTER).admin()
            .cluster()
            .prepareListTasks()
            .setActions(TransportSearchAction.TYPE.name())
            .get();
        List<TaskInfo> localSearchTasks = listTasksResponse.getTasks();
        assertThat(localSearchTasks.size(), equalTo(1));
        TaskInfo localSearchTask = localSearchTasks.getFirst();
        assertFalse("taskInfo on local cluster should not be cancelled yet: " + localSearchTask, localSearchTask.cancelled());

        AtomicReference<TaskInfo> remoteClusterSearchTask = new AtomicReference<>();
        assertBusy(() -> {
            List<TaskInfo> remoteSearchTasks = client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .setActions(TransportSearchAction.TYPE.name())
                .get()
                .getTasks();
            assertThat(remoteSearchTasks.size(), equalTo(1));
            remoteClusterSearchTask.set(remoteSearchTasks.getFirst());
        });
        assertFalse(
            "taskInfo on remote cluster should not be cancelled yet: " + remoteClusterSearchTask.get(),
            remoteClusterSearchTask.get().cancelled()
        );

        // wait until the async search has expired (takes one second - keep alive can't be set lower than 1s)
        // the get async search that returns 404 will also cancel the task as it is expired
        assertBusy(() -> {
            expectThrows(ResourceNotFoundException.class, () -> {
                AsyncSearchResponse asyncSearchResponse = getAsyncSearch(response.getId());
                asyncSearchResponse.decRef();
            });
        });

        try {
            assertBusy(() -> {
                // check that the tasks are cancelled
                GetTaskResponse getLocalTaskResponse = client(LOCAL_CLUSTER).admin()
                    .cluster()
                    .getTask(new GetTaskRequest().setTaskId(localSearchTask.taskId()))
                    .get();
                assertTrue(getLocalTaskResponse.getTask().getTask().cancelled());
                GetTaskResponse getRemoteTaskResponse = client(REMOTE_CLUSTER).admin()
                    .cluster()
                    .getTask(new GetTaskRequest().setTaskId(remoteClusterSearchTask.get().taskId()))
                    .get();
                assertTrue(getRemoteTaskResponse.getTask().getTask().cancelled());
            });
        } finally {
            SearchListenerPlugin.allowRemoteQueryPhase();
            SearchListenerPlugin.allowLocalQueryPhase();
            waitForSearchTasksToFinish();
        }
    }

    public void testCancelViaExpirationOnRemoteResultsWithMinimizeRoundtrips() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        SearchListenerPlugin.blockLocalQueryPhase();
        SearchListenerPlugin.blockRemoteQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(true);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepAlive(new TimeValue(1, TimeUnit.SECONDS));
        request.setKeepOnCompletion(true);

        final AsyncSearchResponse response = submitAsyncSearch(request);
        final long asyncSearchExpirationTimeMillis = response.getExpirationTime();
        try {
            assertNotNull(response.getSearchResponse());
        } finally {
            response.decRef();
            assertTrue(response.isRunning());
        }

        SearchListenerPlugin.waitLocalSearchStarted();
        SearchListenerPlugin.waitRemoteSearchStarted();

        ListTasksResponse listTasksResponse = client(LOCAL_CLUSTER).admin()
            .cluster()
            .prepareListTasks()
            .setActions(TransportSearchAction.TYPE.name())
            .get();
        List<TaskInfo> localSearchTasks = listTasksResponse.getTasks();
        assertThat(localSearchTasks.size(), equalTo(1));
        TaskInfo localSearchTask = localSearchTasks.getFirst();
        assertFalse("taskInfo on local cluster should not be cancelled yet: " + localSearchTask, localSearchTask.cancelled());

        AtomicReference<TaskInfo> remoteClusterSearchTask = new AtomicReference<>();
        assertBusy(() -> {
            List<TaskInfo> remoteSearchTasks = client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .setActions(TransportSearchAction.TYPE.name())
                .get()
                .getTasks();
            assertThat(remoteSearchTasks.size(), equalTo(1));
            remoteClusterSearchTask.set(remoteSearchTasks.getFirst());
        });
        assertFalse(
            "taskInfo on remote cluster should not be cancelled yet: " + remoteClusterSearchTask.get(),
            remoteClusterSearchTask.get().cancelled()
        );

        // wait until the async search has expired (takes one second - keep alive can't be set lower than 1s)
        // don't call get async search as that triggers cancellation of the task - we want to verify that we can cancel it
        // as we get results from a remote cluster
        assertBusy(() -> assertThat(System.currentTimeMillis(), greaterThanOrEqualTo(asyncSearchExpirationTimeMillis)));

        {
            // check that the tasks are cancelled
            GetTaskResponse getLocalTaskResponse = client(LOCAL_CLUSTER).admin()
                .cluster()
                .getTask(new GetTaskRequest().setTaskId(localSearchTask.taskId()))
                .get();
            assertFalse(getLocalTaskResponse.getTask().getTask().cancelled());
            GetTaskResponse getRemoteTaskResponse = client(REMOTE_CLUSTER).admin()
                .cluster()
                .getTask(new GetTaskRequest().setTaskId(remoteClusterSearchTask.get().taskId()))
                .get();
            assertFalse(getRemoteTaskResponse.getTask().getTask().cancelled());
        }

        // unblock the remote query phase, but not the local one: we want to test that getting results from a remote cluster triggers
        // cancellation given the async search has expired
        SearchListenerPlugin.allowRemoteQueryPhase();

        try {
            assertBusy(() -> {
                // check that the tasks are cancelled - they get cancelled because we check for cancellation in
                // AsyncSearchTask#onClusterResponseMinimizeRoundtrips
                GetTaskResponse getLocalTaskResponse = client(LOCAL_CLUSTER).admin()
                    .cluster()
                    .getTask(new GetTaskRequest().setTaskId(localSearchTask.taskId()))
                    .get();
                assertTrue(getLocalTaskResponse.getTask().getTask().cancelled());
                expectThrows(
                    ResourceNotFoundException.class,
                    () -> client(REMOTE_CLUSTER).admin()
                        .cluster()
                        .getTask(new GetTaskRequest().setTaskId(remoteClusterSearchTask.get().taskId()))
                        .actionGet()
                );
            });
        } finally {
            SearchListenerPlugin.allowLocalQueryPhase();
            waitForSearchTasksToFinish();
        }
    }

    public void testCancelViaTasksAPI() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        SearchListenerPlugin.blockLocalQueryPhase();
        SearchListenerPlugin.blockRemoteQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, Math.max(localNumShards, remoteNumShards) + 1));
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

        SearchListenerPlugin.waitLocalSearchStarted();
        SearchListenerPlugin.waitRemoteSearchStarted();

        ActionFuture<ListTasksResponse> cancelFuture;
        try {
            ListTasksResponse listTasksResponse = client(LOCAL_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .setActions(TransportSearchAction.TYPE.name())
                .get();
            List<TaskInfo> tasks = listTasksResponse.getTasks();
            assertThat(tasks.size(), equalTo(1));
            final TaskInfo rootTask = tasks.getFirst();

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
            SearchListenerPlugin.allowLocalQueryPhase();
            SearchListenerPlugin.allowRemoteQueryPhase();
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
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        SearchListenerPlugin.blockLocalQueryPhase();
        SearchListenerPlugin.blockRemoteQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, Math.max(localNumShards, remoteNumShards) + 1));
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

        SearchListenerPlugin.waitLocalSearchStarted();
        SearchListenerPlugin.waitRemoteSearchStarted();

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

            expectThrows(ResourceNotFoundException.class, () -> getAsyncSearch(response.getId()));
            expectThrows(ResourceNotFoundException.class, () -> getAsyncStatus(response.getId()));
        } finally {
            SearchListenerPlugin.allowLocalQueryPhase();
            SearchListenerPlugin.allowRemoteQueryPhase();
        }

        waitForSearchTasksToFinish();

        assertBusy(() -> expectThrows(ResourceNotFoundException.class, () -> getAsyncStatus(response.getId())));
    }

    public void testCancellationViaTimeoutWithAllowPartialResultsSetToFalse() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        SearchListenerPlugin.blockLocalQueryPhase();
        SearchListenerPlugin.blockRemoteQueryPhase();

        TimeValue searchTimeout = new TimeValue(100, TimeUnit.MILLISECONDS);
        // query builder that will sleep for the specified amount of time in the query phase
        SlowRunningQueryBuilder slowRunningQueryBuilder = new SlowRunningQueryBuilder(searchTimeout.millis() * 5);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(slowRunningQueryBuilder).timeout(searchTimeout);

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.getSearchRequest().source(sourceBuilder);
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, Math.max(localNumShards, remoteNumShards) + 1));
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

        SearchListenerPlugin.waitLocalSearchStarted();
        SearchListenerPlugin.waitRemoteSearchStarted();

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
            SearchListenerPlugin.allowLocalQueryPhase();
            SearchListenerPlugin.allowRemoteQueryPhase();
        }

        // query phase has begun, so wait for query failure (due to timeout)
        SearchListenerPlugin.waitQueryFailure();

        // wait for search tasks to complete and be unregistered
        waitForSearchTasksToFinish();

        AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
        assertFalse(statusResponse.isRunning());
        assertTrue(statusResponse.isPartial());

        assertEquals(0, statusResponse.getSuccessfulShards());
        assertEquals(0, statusResponse.getSkippedShards());
        assertThat(statusResponse.getFailedShards(), greaterThanOrEqualTo(1));
    }

    public void testDirectoryMetricsLocalAndRemoteMinimizeRoundtripsTrue() {
        assertAsyncCcsDirectoryMetrics(true, false);
    }

    public void testDirectoryMetricsLocalAndRemoteMinimizeRoundtripsFalse() {
        assertAsyncCcsDirectoryMetrics(false, false);
    }

    public void testDirectoryMetricsRemoteOnlyMinimizeRoundtripsTrue() {
        assertAsyncCcsDirectoryMetrics(true, true);
    }

    public void testDirectoryMetricsRemoteOnlyMinimizeRoundtripsFalse() {
        assertAsyncCcsDirectoryMetrics(false, true);
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
                assertThat(transportService.getTaskManager().getBannedTaskIds(), empty());
            }
        });
    }

    private static void assertInitialClusterDetailsState(AsyncSearchResponse response, SearchResponse.Cluster.Status remoteStatus) {
        assertNotNull(response.getSearchResponse());
        assertTrue(response.isRunning());
        SearchResponse.Clusters clusters = response.getSearchResponse().getClusters();
        assertThat(clusters.getTotal(), equalTo(2));
        assertTrue("search cluster results should be marked as partial", clusters.hasPartialResults());

        SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(LOCAL_CLUSTER);
        assertNotNull(localClusterSearchInfo);
        assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));

        SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
        assertNotNull(remoteClusterSearchInfo);
        assertThat(remoteClusterSearchInfo.getStatus(), equalTo(remoteStatus));
    }

    private static void assertClusterDetailsSuccessful(
        SearchResponse.Clusters clusters,
        int localNumShards,
        int remoteNumShards,
        boolean localIncluded
    ) {
        int numClusters = localIncluded ? 2 : 1;
        assertNotNull(clusters);
        assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
        assertThat(clusters.getTotal(), equalTo(numClusters));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(numClusters));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));

        if (localIncluded) {
            assertOneClusterDetailsAllShardsSuccessful(clusters.getCluster(LOCAL_CLUSTER), localNumShards);
        } else {
            assertNull(clusters.getCluster(LOCAL_CLUSTER));
        }

        assertOneClusterDetailsAllShardsSuccessful(clusters.getCluster(REMOTE_CLUSTER), remoteNumShards);
    }

    private static void assertOneClusterDetailsAllShardsSuccessful(SearchResponse.Cluster cluster, int numShards) {
        assertNotNull(cluster);
        assertThat(cluster.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
        assertThat(cluster.getTotalShards(), equalTo(numShards));
        assertThat(cluster.getSuccessfulShards(), equalTo(numShards));
        assertThat(cluster.getSkippedShards(), equalTo(0));
        assertThat(cluster.getFailedShards(), equalTo(0));
        assertThat(cluster.getFailures().size(), equalTo(0));
        // It's possible for searches with no shards to complete immediately, in which case took will be 0
        assertThat(cluster.getTook().millis(), greaterThanOrEqualTo(0L));
        assertThat(cluster.isTimedOut(), equalTo(false));
    }

    private static void assertClusterDetailsAllShardsSkipped(
        SearchResponse.Clusters clusters,
        boolean dfs,
        int localNumShards,
        int remoteNumShards
    ) {
        assertNotNull(clusters);
        assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
        assertThat(clusters.getTotal(), equalTo(2));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(2));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));

        SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(LOCAL_CLUSTER);
        assertNotNull(localClusterSearchInfo);

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

        SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
        assertNotNull(remoteClusterSearchInfo);
        assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
        assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
        assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards));
        assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(remoteNumShards));
        assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
        assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));
        assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThanOrEqualTo(0L));
    }

    private static void assertClusterDetailsFailureOnOneShardOnly(
        SearchResponse.Clusters clusters,
        int localNumShards,
        int remoteNumShards,
        boolean localIncluded
    ) {
        assertNotNull(clusters);
        int numClusters = localIncluded ? 2 : 1;
        assertThat(clusters.getTotal(), equalTo(numClusters));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(0));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(numClusters));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));

        if (localIncluded) {
            assertOneClusterDetailsOneShardFailed(clusters.getCluster(LOCAL_CLUSTER), localNumShards);
        } else {
            assertNull(clusters.getCluster(LOCAL_CLUSTER));
        }

        assertOneClusterDetailsOneShardFailed(clusters.getCluster(REMOTE_CLUSTER), remoteNumShards);
    }

    private static void assertOneClusterDetailsOneShardFailed(SearchResponse.Cluster localClusterSearchInfo, int localNumShards) {
        assertNotNull(localClusterSearchInfo);
        assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
        assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
        assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards - 1));
        assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
        assertThat(localClusterSearchInfo.getFailedShards(), equalTo(1));
        assertThat(localClusterSearchInfo.getFailures().size(), equalTo(1));
        assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));
        ShardSearchFailure localShardSearchFailure = localClusterSearchInfo.getFailures().getFirst();
        assertTrue(
            "should have 'index corrupted' in reason but was: " + localShardSearchFailure.reason(),
            localShardSearchFailure.reason().contains("index corrupted")
        );
    }

    private static void assertClusterDetailsFailureOnOneShardOnlyAllowPartialResultsFalseMinimizeRoundtripsFalse(
        SearchResponse.Clusters clusters,
        boolean remoteSkipUnavailable,
        boolean dfs,
        int localNumShards,
        int remoteNumShards,
        boolean localIncluded
    ) {
        int successfulLocalShards;
        int successfulRemoteShards;
        if (dfs) {
            // For a DFS search, the exception thrown by ThrowingQueryBuilder is thrown during the DfsPhase, before the DfsQueryPhase.
            // When allow_partial_search_results=false, the search is cancelled on the first shard failure, so no shards ever see a
            // successful query result
            successfulLocalShards = 0;
            successfulRemoteShards = 0;
        } else {
            successfulLocalShards = localNumShards - 1;
            successfulRemoteShards = remoteNumShards - 1;
        }

        int numClusters = localIncluded ? 2 : 1;
        assertThat(clusters, notNullValue());
        assertThat(clusters.getTotal(), equalTo(numClusters));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(0));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
        if (dfs) {
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
            if (remoteSkipUnavailable) {
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(1));
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(numClusters - 1));
            } else {
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(numClusters));
            }
        } else {
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(numClusters));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));
        }

        if (localIncluded == false) {
            assertNull(clusters.getCluster(LOCAL_CLUSTER));
        } else {
            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(LOCAL_CLUSTER);
            assertNotNull(localClusterSearchInfo);
            if (dfs) {
                assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.FAILED));
                assertThat(localClusterSearchInfo.getTook(), nullValue());
            } else {
                assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
                assertThat(localClusterSearchInfo.getTook().millis(), greaterThanOrEqualTo(0L));
            }
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            // Failures due to the search task being cancelled after the first failure may be included in the results, reducing the number
            // of successful shards and increasing the number of failed shards and failures
            assertThat(localClusterSearchInfo.getSuccessfulShards(), lessThanOrEqualTo(successfulLocalShards));
            assertThat(localClusterSearchInfo.getFailedShards(), greaterThanOrEqualTo(1));
            var sumOfLocalShards = localClusterSearchInfo.getSuccessfulShards() + localClusterSearchInfo.getFailedShards();
            assertThat(sumOfLocalShards, equalTo(localClusterSearchInfo.getTotalShards()));
            assertThat(localClusterSearchInfo.getFailures().size(), greaterThanOrEqualTo(1));
            ShardSearchFailure localShardSearchFailure = localClusterSearchInfo.getFailures().getFirst();
            // When allow_partial_search_results=false, the exception may be a TaskCancelledException
            assertThat(
                "should have 'index corrupted' or 'partial results are not allowed' in reason but was: " + localShardSearchFailure.reason(),
                localShardSearchFailure.reason(),
                anyOf(
                    containsString("index corrupted"),
                    containsString("Fatal failure during search: partial results are not allowed and at least one shard has failed")
                )
            );
        }

        SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
        assertNotNull(remoteClusterSearchInfo);
        if (dfs) {
            if (remoteSkipUnavailable) {
                assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SKIPPED));
            } else {
                assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.FAILED));
            }
            assertThat(remoteClusterSearchInfo.getTook(), nullValue());
        } else {
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
        }

        assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
        assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
        // Failures due to the search task being cancelled after the first failure may be included in the results, reducing the number
        // of successful shards and increasing the number of failed shards and failures
        assertThat(remoteClusterSearchInfo.getSuccessfulShards(), lessThanOrEqualTo(successfulRemoteShards));
        assertThat(remoteClusterSearchInfo.getFailedShards(), greaterThanOrEqualTo(1));
        var sumOfRemoteShards = remoteClusterSearchInfo.getSuccessfulShards() + remoteClusterSearchInfo.getFailedShards();
        assertThat(sumOfRemoteShards, equalTo(remoteClusterSearchInfo.getTotalShards()));
        assertThat(remoteClusterSearchInfo.getFailures().size(), greaterThanOrEqualTo(1));
        ShardSearchFailure remoteShardSearchFailure = remoteClusterSearchInfo.getFailures().getFirst();
        // When allow_partial_search_results=false, the exception may be a TaskCancelledException
        assertThat(
            "should have 'index corrupted' or 'partial results are not allowed' in reason but was: " + remoteShardSearchFailure.reason(),
            remoteShardSearchFailure.reason(),
            anyOf(
                containsString("index corrupted"),
                containsString("Fatal failure during search: partial results are not allowed and at least one shard has failed")
            )
        );
    }

    private static void assertClusterDetailsAllShardsFailed(
        SearchResponse.Clusters clusters,
        boolean remoteSkipUnavailable,
        boolean minimizeRoundtrips,
        int localNumShards,
        int remoteNumShards,
        boolean localIncluded
    ) {
        int numClusters;
        int baseFailures;
        if (localIncluded) {
            numClusters = 2;
            baseFailures = 1;
        } else {
            numClusters = 1;
            baseFailures = 0;
        }
        assertNotNull(clusters);
        assertThat(clusters.getTotal(), equalTo(numClusters));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(0));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
        if (remoteSkipUnavailable) {
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(1));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(baseFailures));
        } else {
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(baseFailures + 1));
        }

        if (localIncluded) {
            assertOneClusterDetailsAllShardsFailed(
                clusters.getCluster(LOCAL_CLUSTER),
                SearchResponse.Cluster.Status.FAILED,
                minimizeRoundtrips,
                localNumShards
            );
        } else {
            assertNull(clusters.getCluster(LOCAL_CLUSTER));
        }

        SearchResponse.Cluster.Status expectedStatus = remoteSkipUnavailable
            ? SearchResponse.Cluster.Status.SKIPPED
            : SearchResponse.Cluster.Status.FAILED;
        assertOneClusterDetailsAllShardsFailed(clusters.getCluster(REMOTE_CLUSTER), expectedStatus, minimizeRoundtrips, remoteNumShards);
    }

    private static void assertOneClusterDetailsAllShardsFailed(
        SearchResponse.Cluster cluster,
        SearchResponse.Cluster.Status expectedStatus,
        boolean minimizeRoundtrips,
        int numShards
    ) {
        assertNotNull(cluster);
        assertThat(cluster.getStatus(), equalTo(expectedStatus));
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
        ShardSearchFailure shardSearchFailure = cluster.getFailures().getFirst();
        assertTrue(
            "should have 'index corrupted' in reason but was: " + shardSearchFailure.reason(),
            shardSearchFailure.reason().contains("index corrupted")
        );
    }

    private static void assertClusterDetailsFailuresOnOneCluster(
        SearchResponse.Clusters clusters,
        boolean remoteSkipUnavailable,
        boolean minimizeRoundtrips,
        int localNumShards,
        int remoteNumShards
    ) {
        assertNotNull(clusters);
        assertThat(clusters.getTotal(), equalTo(2));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(1));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
        if (remoteSkipUnavailable) {
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(1));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));
        } else {
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(1));
        }

        assertOneClusterDetailsAllShardsSuccessful(clusters.getCluster(LOCAL_CLUSTER), localNumShards);

        SearchResponse.Cluster.Status expectedStatus = remoteSkipUnavailable
            ? SearchResponse.Cluster.Status.SKIPPED
            : SearchResponse.Cluster.Status.FAILED;
        assertOneClusterDetailsAllShardsFailed(clusters.getCluster(REMOTE_CLUSTER), expectedStatus, minimizeRoundtrips, remoteNumShards);
    }

    private static void assertClustersDetailsFailuresOnOneClusterOnlyAllowPartialResultsFalseMinimizeRoundtripsFalse(
        SearchResponse.Clusters clusters,
        boolean remoteSkipUnavailable,
        boolean dfs,
        int localNumShards,
        int remoteNumShards
    ) {
        assertNotNull(clusters);
        assertThat(clusters.getTotal(), equalTo(2));
        SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(LOCAL_CLUSTER);
        assertNotNull(localClusterSearchInfo);
        boolean localFailure = localClusterSearchInfo.getFailedShards() > 0;
        // If the internal cancel happens before the local cluster gets results for all shards, the status will be PARTIAL instead of
        // SUCCESSFUL, localClusterSearchInfo.getFailedShards() will be non-zero, and there may be ShardSearchFailure in the response
        if (localFailure) {
            localClusterSearchInfo.getFailures().forEach(failure -> {
                assertThat(failure.getCause(), instanceOf(TaskCancelledException.class));
                assertThat(
                    failure.reason(),
                    containsString("Fatal failure during search: partial results are not allowed and at least one shard has failed")
                );
            });
        }

        int skipped = 0;
        int successful = 0;
        int failed = 0;
        int partial = 0;

        // Determine status count from the local cluster
        // When the search uses DFS_QUERY_THEN_FETCH, the failure on the remote cluster happens before the DfsPhase completes, which
        // causes the search to be cancelled, so it never progresses to the DfsQueryPhase on the local cluster, causing the state to be
        // FAILED even though no failures may have actually occurred on the local cluster.
        // When not using DFS_QUERY_THEN_FETCH, there may still be failures on the local cluster due to the search being cancelled
        if (dfs) {
            failed++;
        } else {
            if (localFailure) {
                partial++;
            } else {
                successful++;
            }
        }

        // Determine status count from the remote cluster
        if (remoteSkipUnavailable) {
            skipped++;
        } else {
            failed++;
        }

        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(partial));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(skipped));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(successful));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(failed));

        if (dfs) {
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.FAILED));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(localNumShards));
            // There may be TaskCancellationException in the failures list if the search is cancelled before the DfsPhase finishes
            assertThat(localClusterSearchInfo.getFailures().size(), greaterThanOrEqualTo(0));
            assertThat(localClusterSearchInfo.getTook(), nullValue());
        } else {
            if (localFailure) {
                assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
                assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
                assertThat(localClusterSearchInfo.getSuccessfulShards(), lessThan(localNumShards));
                assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
                assertThat(localClusterSearchInfo.getFailedShards(), greaterThan(0));
                var sumOfLocalShards = localClusterSearchInfo.getSuccessfulShards() + localClusterSearchInfo.getFailedShards();
                assertThat(sumOfLocalShards, equalTo(localClusterSearchInfo.getTotalShards()));
                // Depending on the cause of the failure on the local cluster, there may or may not be TaskCancellationException in the
                // failures list
                assertThat(localClusterSearchInfo.getFailures().size(), greaterThanOrEqualTo(0));
            } else {
                assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
                assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
                assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
                assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
                assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
                assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            }
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));
        }

        SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
        assertNotNull(remoteClusterSearchInfo);
        SearchResponse.Cluster.Status expectedStatus = remoteSkipUnavailable
            ? SearchResponse.Cluster.Status.SKIPPED
            : SearchResponse.Cluster.Status.FAILED;
        assertThat(remoteClusterSearchInfo.getStatus(), equalTo(expectedStatus));
        assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
        assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(0));
        assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
        assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(remoteNumShards));
        assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(remoteNumShards));
        assertNull(remoteClusterSearchInfo.getTook());
        assertFalse(remoteClusterSearchInfo.isTimedOut());
        ShardSearchFailure remoteShardSearchFailure = remoteClusterSearchInfo.getFailures().getFirst();
        // When allow_partial_search_results=false, the exception may be a TaskCancelledException
        assertThat(
            "should have 'index corrupted' or 'partial results are not allowed' in reason but was: " + remoteShardSearchFailure.reason(),
            remoteShardSearchFailure.reason(),
            anyOf(
                containsString("index corrupted"),
                containsString("Fatal failure during search: partial results are not allowed and at least one shard has failed")
            )
        );
    }

    private static void assertClusterDetailsSearchTimeout(SearchResponse.Clusters clusters, int localNumShards, int remoteNumShards) {
        assertNotNull(clusters);
        assertThat(clusters.getTotal(), equalTo(2));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(0));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(2));
        assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));

        assertOneClusterDetailsTimedOut(clusters.getCluster(LOCAL_CLUSTER), localNumShards);

        assertOneClusterDetailsTimedOut(clusters.getCluster(REMOTE_CLUSTER), remoteNumShards);
    }

    private static void assertOneClusterDetailsTimedOut(SearchResponse.Cluster cluster, int numShards) {
        assertNotNull(cluster);
        // PARTIAL expected since timedOut=true
        assertThat(cluster.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
        assertThat(cluster.getTotalShards(), equalTo(numShards));
        assertThat(cluster.getSuccessfulShards(), equalTo(numShards));
        assertThat(cluster.getSkippedShards(), equalTo(0));
        assertThat(cluster.getFailedShards(), equalTo(0));
        assertThat(cluster.getFailures().size(), equalTo(0));
        assertThat(cluster.getTook().millis(), greaterThanOrEqualTo(0L));
        assertTrue(cluster.isTimedOut());
    }

    private void assertAsyncCcsDirectoryMetrics(boolean minimizeRoundtrips, boolean remoteOnly) {
        assumeTrue("directory metrics must be enabled", Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled());
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        SubmitAsyncSearchRequest request = remoteOnly
            ? new SubmitAsyncSearchRequest(REMOTE_CLUSTER + ":" + remoteIndex)
            : new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(minimizeRoundtrips);
        request.setKeepOnCompletion(true);
        request.setWaitForCompletionTimeout(TimeValue.timeValueSeconds(60));
        request.getSearchRequest().allowPartialSearchResults(false);
        request.getSearchRequest().source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));

        int expectedSuccessfulClusters = remoteOnly ? 1 : 2;
        String responseId = null;
        try {
            final AsyncSearchResponse response = submitAsyncSearch(request);
            try {
                responseId = response.getId();
                assertFalse(response.isRunning());
                assertNotNull(response.getSearchResponse());
                assertThat(
                    response.getSearchResponse().getClusters().getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL),
                    equalTo(expectedSuccessfulClusters)
                );
                assertThat(storeBytesRead(response.getSearchResponse().getDirectoryMetrics()), greaterThan(0L));
            } finally {
                response.decRef();
            }
        } finally {
            if (responseId != null) {
                deleteAsyncSearch(responseId);
            }
        }
    }

    private static long storeBytesRead(DirectoryMetrics metrics) {
        String value = metrics.entries().get(StoreMetrics.BYTES_READ_METRIC_KEY);
        return value == null ? 0L : Long.parseLong(value);
    }

    protected AsyncSearchResponse submitAsyncSearch(SubmitAsyncSearchRequest request) {
        return client(LOCAL_CLUSTER).execute(SubmitAsyncSearchAction.INSTANCE, request).actionGet();
    }

    protected AsyncSearchResponse getAsyncSearch(String id) {
        return client(LOCAL_CLUSTER).execute(GetAsyncSearchAction.INSTANCE, new GetAsyncResultRequest(id)).actionGet();
    }

    protected AsyncSearchResponse getAsyncSearch(String id, boolean returnIntermediateResponse) {
        return client(LOCAL_CLUSTER).execute(
            GetAsyncSearchAction.INSTANCE,
            new GetAsyncResultRequest(id).setReturnIntermediateResults(returnIntermediateResponse)
                .setWaitForCompletionTimeout(TimeValue.timeValueSeconds(1))
        ).actionGet();
    }

    protected AsyncStatusResponse getAsyncStatus(String id) {
        return client(LOCAL_CLUSTER).execute(GetAsyncStatusAction.INSTANCE, new GetAsyncStatusRequest(id)).actionGet();
    }

    protected AcknowledgedResponse deleteAsyncSearch(String id) {
        return client().execute(TransportDeleteAsyncResultAction.TYPE, new DeleteAsyncResultRequest(id)).actionGet();
    }

    private Map<String, Object> setupTwoClusters() {
        String localIndex = "local";
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

        String remoteIndex = "remote";
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
        // Wait for yellow and no initializing shards so all assigned shards are STARTED before tests run searches.
        ClusterHealthResponse healthResponse = client(REMOTE_CLUSTER).admin()
            .cluster()
            .prepareHealth(TEST_REQUEST_TIMEOUT, remoteIndex)
            .setWaitForYellowStatus()
            .setWaitForNoInitializingShards(true)
            .setTimeout(TimeValue.timeValueSeconds(10))
            .get();
        assertFalse("remote index health check timed out", healthResponse.isTimedOut());
        assertEquals(
            "remote index should have no initializing shards, had: " + healthResponse.getInitializingShards(),
            0,
            healthResponse.getInitializingShards()
        );
        indexDocs(client(REMOTE_CLUSTER), remoteIndex);

        String skipUnavailableKey = Strings.format("cluster.remote.%s.skip_unavailable", REMOTE_CLUSTER);
        Setting<?> skipUnavailableSetting = cluster(REMOTE_CLUSTER).clusterService().getClusterSettings().get(skipUnavailableKey);
        boolean skipUnavailable = (boolean) cluster(LOCAL_CLUSTER).clusterService().getClusterSettings().get(skipUnavailableSetting);

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
        private static final AtomicReference<CountDownLatch> startedLocalLatch = new AtomicReference<>();
        private static final AtomicReference<CountDownLatch> startedRemoteLatch = new AtomicReference<>();
        private static final AtomicReference<CountDownLatch> localQueryLatch = new AtomicReference<>();
        private static final AtomicReference<CountDownLatch> remoteQueryLatch = new AtomicReference<>();
        private static final AtomicReference<CountDownLatch> failedQueryLatch = new AtomicReference<>();
        private static final AtomicReference<CountDownLatch> localQueryPhaseCompleteLatch = new AtomicReference<>();
        private static final AtomicReference<CountDownLatch> remoteQueryPhaseCompleteLatch = new AtomicReference<>();
        private static final AtomicReference<CountDownLatch> localFetchLatch = new AtomicReference<>();
        private static final AtomicReference<CountDownLatch> remoteFetchLatch = new AtomicReference<>();

        /**
         * For tests that cannot use SearchListenerPlugin, ensure all latches are unset to
         * avoid test problems around searches of the .async-search index
         */
        static void negate() {
            if (startedLocalLatch.get() != null) {
                startedLocalLatch.get().countDown();
            }
            if (startedRemoteLatch.get() != null) {
                startedRemoteLatch.get().countDown();
            }
            if (localQueryLatch.get() != null) {
                localQueryLatch.get().countDown();
            }
            if (remoteQueryLatch.get() != null) {
                remoteQueryLatch.get().countDown();
            }
            if (failedQueryLatch.get() != null) {
                failedQueryLatch.get().countDown();
            }
            if (localFetchLatch.get() != null) {
                localFetchLatch.get().countDown();
            }
            if (remoteFetchLatch.get() != null) {
                remoteFetchLatch.get().countDown();
            }
        }

        static void reset() {
            startedLocalLatch.set(new CountDownLatch(1));
            startedRemoteLatch.set(new CountDownLatch(1));
            failedQueryLatch.set(new CountDownLatch(1));
        }

        static void blockRemoteQueryPhase() {
            remoteQueryLatch.set(new CountDownLatch(1));
        }

        static void allowRemoteQueryPhase() {
            final CountDownLatch latch = remoteQueryLatch.get();
            if (latch != null) {
                latch.countDown();
            }
        }

        static void blockLocalQueryPhase() {
            localQueryLatch.set(new CountDownLatch(1));
        }

        static void allowLocalQueryPhase() {
            final CountDownLatch latch = localQueryLatch.get();
            if (latch != null) {
                latch.countDown();
            }
        }

        static void blockLocalQueryPhaseCompletion(int numCompletions) {
            localQueryPhaseCompleteLatch.set(new CountDownLatch(numCompletions));
        }

        static void waitForLocalQueryPhaseCompletion() throws InterruptedException {
            final CountDownLatch latch = localQueryPhaseCompleteLatch.get();
            if (latch != null) {
                latch.await(60, TimeUnit.SECONDS);
            }
        }

        static void blockRemoteQueryPhaseCompletion(int numCompletions) {
            remoteQueryPhaseCompleteLatch.set(new CountDownLatch(numCompletions));
        }

        static void waitForRemoteQueryPhaseCompletion() throws InterruptedException {
            final CountDownLatch latch = remoteQueryPhaseCompleteLatch.get();
            if (latch != null) {
                latch.await(60, TimeUnit.SECONDS);
            }
        }

        static void waitRemoteSearchStarted() throws InterruptedException {
            assertTrue(startedRemoteLatch.get().await(60, TimeUnit.SECONDS));
        }

        static void waitLocalSearchStarted() throws InterruptedException {
            assertTrue(startedLocalLatch.get().await(60, TimeUnit.SECONDS));
        }

        static void waitQueryFailure() throws Exception {
            assertTrue(failedQueryLatch.get().await(60, TimeUnit.SECONDS));
        }

        static void blockLocalFetchPhase() {
            localFetchLatch.set(new CountDownLatch(1));
        }

        static void allowLocalFetchPhase() {
            final CountDownLatch latch = localFetchLatch.get();
            if (latch != null) {
                latch.countDown();
            }
        }

        static void blockRemoteFetchPhase() {
            remoteFetchLatch.set(new CountDownLatch(1));
        }

        static void allowRemoteFetchPhase() {
            final CountDownLatch latch = remoteFetchLatch.get();
            if (latch != null) {
                latch.countDown();
            }
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onNewReaderContext(ReaderContext readerContext) {
                    assertThat(readerContext, not(instanceOf(LegacyReaderContext.class)));
                }

                @Override
                public void onPreDfsPhase(SearchContext searchContext) {
                    // If the query is using DFS_QUERY_THEN_FETCH, block before the DFS phase, because otherwise any exceptions thrown
                    // by the test during queries will cause the search to finish before getting to the query phase
                    onPreQueryPhase(searchContext);
                }

                @Override
                public void onPreQueryPhase(SearchContext searchContext) {
                    final CountDownLatch latch;
                    if (searchContext.indexShard().shardId().getIndexName().equals("remote")) {
                        startedRemoteLatch.get().countDown();
                        latch = remoteQueryLatch.get();
                    } else if (searchContext.indexShard().shardId().getIndexName().equals("local")) {
                        startedLocalLatch.get().countDown();
                        latch = localQueryLatch.get();
                    } else {
                        throw new AssertionError("unexpected index name: " + searchContext.indexShard().shardId().getIndexName());
                    }
                    if (latch != null) {
                        try {
                            assertTrue(latch.await(60, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                    }
                }

                @Override
                public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
                    if (localQueryPhaseCompleteLatch.get() != null && searchContext.indexShard().shardId().getIndexName().equals("local")) {
                        localQueryPhaseCompleteLatch.get().countDown();
                    } else if (remoteQueryPhaseCompleteLatch.get() != null
                        && searchContext.indexShard().shardId().getIndexName().equals("remote")) {
                            remoteQueryPhaseCompleteLatch.get().countDown();
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

                @Override
                public void onPreFetchPhase(SearchContext searchContext) {
                    final CountDownLatch latch;
                    if (searchContext.indexShard().shardId().getIndexName().equals("remote")) {
                        latch = remoteFetchLatch.get();
                    } else if (searchContext.indexShard().shardId().getIndexName().equals("local")) {
                        latch = localFetchLatch.get();
                    } else {
                        throw new AssertionError("unexpected index name: " + searchContext.indexShard().shardId().getIndexName());
                    }
                    if (latch != null) {
                        try {
                            assertTrue(latch.await(60, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                    }
                }
            });
            super.onIndexModule(indexModule);
        }
    }
}
