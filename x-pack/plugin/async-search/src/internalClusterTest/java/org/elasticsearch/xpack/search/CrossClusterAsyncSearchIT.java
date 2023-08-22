/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
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
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.LegacyReaderContext;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;
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
import org.elasticsearch.xpack.core.async.DeleteAsyncResultAction;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.async.GetAsyncStatusRequest;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.AsyncStatusResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.GetAsyncStatusAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.ArrayList;
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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

@LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/98272")
public class CrossClusterAsyncSearchIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER = "cluster_a";

    @Override
    protected Collection<String> remoteClusterAlias() {
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
        request.setCcsMinimizeRoundtrips(true);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        request.getSearchRequest().source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(1000));

        AsyncSearchResponse response = submitAsyncSearch(request);
        assertNotNull(response.getSearchResponse());
        assertTrue(response.isRunning());

        {
            SearchResponse.Clusters clusters = response.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertTrue("search cluster results should be marked as partial", clusters.hasPartialResults());

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).get();
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
            assertNotNull(remoteClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));
        }

        SearchListenerPlugin.waitSearchStarted();
        SearchListenerPlugin.allowQueryPhase();

        assertBusy(() -> {
            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            assertFalse(statusResponse.isRunning());
            assertNotNull(statusResponse.getCompletionStatus());
        });

        {
            AsyncSearchResponse finishedResponse = getAsyncSearch(response.getId());

            SearchResponse.Clusters clusters = finishedResponse.getSearchResponse().getClusters();
            assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getSuccessful(), equalTo(2));
            assertThat(clusters.getSkipped(), equalTo(0));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).get();
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
        }

        // check that the async_search/status response includes the same cluster details
        {
            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());

            SearchResponse.Clusters clusters = statusResponse.getClusters();
            assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getSuccessful(), equalTo(2));
            assertThat(clusters.getSkipped(), equalTo(0));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).get();
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
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

    public void testClusterDetailsAfterCCSWithFailuresOnAllShards() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        boolean skipUnavailable = (Boolean) testClusterInfo.get("remote.skip_unavailable");

        SearchListenerPlugin.blockQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(true);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        // shardId -1 means to throw the Exception on all shards, so should result in complete search failure
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("index corrupted"), -1);
        request.getSearchRequest().source(new SearchSourceBuilder().query(queryBuilder).size(10));

        AsyncSearchResponse response = submitAsyncSearch(request);
        assertNotNull(response.getSearchResponse());
        assertTrue(response.isRunning());

        {
            SearchResponse.Clusters clusters = response.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertTrue("search cluster results should be marked as partial", clusters.hasPartialResults());

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).get();
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
            assertNotNull(remoteClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));
        }

        SearchListenerPlugin.waitSearchStarted();
        SearchListenerPlugin.allowQueryPhase();

        assertBusy(() -> {
            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            assertFalse(statusResponse.isRunning());
            assertNotNull(statusResponse.getCompletionStatus());
        });

        {
            AsyncSearchResponse finishedResponse = getAsyncSearch(response.getId());

            SearchResponse.Clusters clusters = finishedResponse.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getSuccessful(), equalTo(0));
            assertThat(clusters.getSkipped(), equalTo(2));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).get();
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.FAILED));
            assertNull(localClusterSearchInfo.getTotalShards());
            assertNull(localClusterSearchInfo.getSuccessfulShards());
            assertNull(localClusterSearchInfo.getSkippedShards());
            assertNull(localClusterSearchInfo.getFailedShards());
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(1));
            assertNull(localClusterSearchInfo.getTook());
            assertFalse(localClusterSearchInfo.isTimedOut());
            ShardSearchFailure localShardSearchFailure = localClusterSearchInfo.getFailures().get(0);
            assertTrue("should have 'index corrupted' in reason", localShardSearchFailure.reason().contains("index corrupted"));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
            assertNotNull(remoteClusterSearchInfo);
            SearchResponse.Cluster.Status expectedStatus = skipUnavailable
                ? SearchResponse.Cluster.Status.SKIPPED
                : SearchResponse.Cluster.Status.FAILED;
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(expectedStatus));
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
            SearchResponse.Clusters clusters = statusResponse.getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getSuccessful(), equalTo(0));
            assertThat(clusters.getSkipped(), equalTo(2));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).get();
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.FAILED));
            assertNull(localClusterSearchInfo.getTotalShards());
            assertNull(localClusterSearchInfo.getSuccessfulShards());
            assertNull(localClusterSearchInfo.getSkippedShards());
            assertNull(localClusterSearchInfo.getFailedShards());
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(1));
            assertNull(localClusterSearchInfo.getTook());
            assertFalse(localClusterSearchInfo.isTimedOut());
            ShardSearchFailure localShardSearchFailure = localClusterSearchInfo.getFailures().get(0);
            assertTrue("should have 'index corrupted' in reason", localShardSearchFailure.reason().contains("index corrupted"));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
            assertNotNull(remoteClusterSearchInfo);
            SearchResponse.Cluster.Status expectedStatus = skipUnavailable
                ? SearchResponse.Cluster.Status.SKIPPED
                : SearchResponse.Cluster.Status.FAILED;
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(expectedStatus));
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
    }

    public void testClusterDetailsAfterCCSWithFailuresOnOneShardOnly() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        SearchListenerPlugin.blockQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(true);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        // shardId 0 means to throw the Exception only on shard 0; all others should work
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("index corrupted"), 0);
        request.getSearchRequest().source(new SearchSourceBuilder().query(queryBuilder).size(10));

        AsyncSearchResponse response = submitAsyncSearch(request);
        assertNotNull(response.getSearchResponse());
        assertTrue(response.isRunning());

        {
            SearchResponse.Clusters clusters = response.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertTrue("search cluster results should be marked as partial", clusters.hasPartialResults());

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).get();
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
            assertNotNull(remoteClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));
        }

        SearchListenerPlugin.waitSearchStarted();
        SearchListenerPlugin.allowQueryPhase();

        assertBusy(() -> {
            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            assertFalse(statusResponse.isRunning());
            assertNotNull(statusResponse.getCompletionStatus());
        });

        {
            AsyncSearchResponse finishedResponse = getAsyncSearch(response.getId());

            SearchResponse.Clusters clusters = finishedResponse.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getSuccessful(), equalTo(2));
            assertThat(clusters.getSkipped(), equalTo(0));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).get();
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards - 1));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(1));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(1));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));
            ShardSearchFailure localShardSearchFailure = localClusterSearchInfo.getFailures().get(0);
            assertTrue("should have 'index corrupted' in reason", localShardSearchFailure.reason().contains("index corrupted"));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards - 1));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(1));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(1));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
            ShardSearchFailure remoteShardSearchFailure = remoteClusterSearchInfo.getFailures().get(0);
            assertTrue("should have 'index corrupted' in reason", remoteShardSearchFailure.reason().contains("index corrupted"));
        }
        // check that the async_search/status response includes the same cluster details
        {
            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            SearchResponse.Clusters clusters = statusResponse.getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getSuccessful(), equalTo(2));
            assertThat(clusters.getSkipped(), equalTo(0));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).get();
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards - 1));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(1));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(1));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));
            ShardSearchFailure localShardSearchFailure = localClusterSearchInfo.getFailures().get(0);
            assertTrue("should have 'index corrupted' in reason", localShardSearchFailure.reason().contains("index corrupted"));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards - 1));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(1));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(1));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
            ShardSearchFailure remoteShardSearchFailure = remoteClusterSearchInfo.getFailures().get(0);
            assertTrue("should have 'index corrupted' in reason", remoteShardSearchFailure.reason().contains("index corrupted"));
        }
    }

    public void testClusterDetailsAfterCCSWithFailuresOnOneClusterOnly() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        boolean skipUnavailable = (Boolean) testClusterInfo.get("remote.skip_unavailable");

        SearchListenerPlugin.blockQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(true);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        // throw Exception of all shards of remoteIndex, but against localIndex
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(
            randomLong(),
            new IllegalStateException("index corrupted"),
            remoteIndex
        );
        request.getSearchRequest().source(new SearchSourceBuilder().query(queryBuilder).size(10));

        AsyncSearchResponse response = submitAsyncSearch(request);
        assertNotNull(response.getSearchResponse());
        assertTrue(response.isRunning());
        {
            SearchResponse.Clusters clusters = response.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertTrue("search cluster results should be marked as partial", clusters.hasPartialResults());

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).get();
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
            assertNotNull(remoteClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));
        }

        SearchListenerPlugin.waitSearchStarted();
        SearchListenerPlugin.allowQueryPhase();

        assertBusy(() -> {
            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            assertFalse(statusResponse.isRunning());
            assertNotNull(statusResponse.getCompletionStatus());
        });

        {
            AsyncSearchResponse finishedResponse = getAsyncSearch(response.getId());

            SearchResponse.Clusters clusters = finishedResponse.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getSuccessful(), equalTo(1));
            assertThat(clusters.getSkipped(), equalTo(1));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).get();
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
            assertNotNull(remoteClusterSearchInfo);
            SearchResponse.Cluster.Status expectedStatus = skipUnavailable
                ? SearchResponse.Cluster.Status.SKIPPED
                : SearchResponse.Cluster.Status.FAILED;
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(expectedStatus));
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
            SearchResponse.Clusters clusters = statusResponse.getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getSuccessful(), equalTo(1));
            assertThat(clusters.getSkipped(), equalTo(1));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).get();
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
            assertNotNull(remoteClusterSearchInfo);
            SearchResponse.Cluster.Status expectedStatus = skipUnavailable
                ? SearchResponse.Cluster.Status.SKIPPED
                : SearchResponse.Cluster.Status.FAILED;
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(expectedStatus));
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
        request.setCcsMinimizeRoundtrips(true);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        request.getSearchRequest().source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(1000));

        AsyncSearchResponse response = submitAsyncSearch(request);
        assertNotNull(response.getSearchResponse());

        assertBusy(() -> {
            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            assertFalse(statusResponse.isRunning());
            assertNotNull(statusResponse.getCompletionStatus());
        });

        {
            AsyncSearchResponse finishedResponse = getAsyncSearch(response.getId());

            SearchResponse.Clusters clusters = finishedResponse.getSearchResponse().getClusters();
            assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
            assertThat(clusters.getTotal(), equalTo(1));
            assertThat(clusters.getSuccessful(), equalTo(1));
            assertThat(clusters.getSkipped(), equalTo(0));

            assertNull(clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
        }

        // check that the async_search/status response includes the same cluster details
        {
            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());

            SearchResponse.Clusters clusters = statusResponse.getClusters();
            assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
            assertThat(clusters.getTotal(), equalTo(1));
            assertThat(clusters.getSuccessful(), equalTo(1));
            assertThat(clusters.getSkipped(), equalTo(0));

            assertNull(clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
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
        request.setCcsMinimizeRoundtrips(true);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        // shardId 0 means to throw the Exception only on shard 0; all others should work
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("index corrupted"), 0);
        request.getSearchRequest().source(new SearchSourceBuilder().query(queryBuilder).size(10));

        AsyncSearchResponse response = submitAsyncSearch(request);
        assertNotNull(response.getSearchResponse());
        assertTrue(response.isRunning());

        assertBusy(() -> {
            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            assertFalse(statusResponse.isRunning());
            assertNotNull(statusResponse.getCompletionStatus());
        });

        {
            AsyncSearchResponse finishedResponse = getAsyncSearch(response.getId());

            SearchResponse.Clusters clusters = finishedResponse.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(1));
            assertThat(clusters.getSuccessful(), equalTo(1));
            assertThat(clusters.getSkipped(), equalTo(0));

            assertNull(clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards - 1));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(1));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(1));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
            ShardSearchFailure remoteShardSearchFailure = remoteClusterSearchInfo.getFailures().get(0);
            assertTrue("should have 'index corrupted' in reason", remoteShardSearchFailure.reason().contains("index corrupted"));
        }
        // check that the async_search/status response includes the same cluster details
        {
            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            SearchResponse.Clusters clusters = statusResponse.getClusters();
            assertThat(clusters.getTotal(), equalTo(1));
            assertThat(clusters.getSuccessful(), equalTo(1));
            assertThat(clusters.getSkipped(), equalTo(0));

            assertNull(clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards - 1));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(1));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(1));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
            ShardSearchFailure remoteShardSearchFailure = remoteClusterSearchInfo.getFailures().get(0);
            assertTrue("should have 'index corrupted' in reason", remoteShardSearchFailure.reason().contains("index corrupted"));
        }
    }

    public void testRemoteClusterOnlyCCSWithFailuresOnAllShards() throws Exception {
        // for remote-only queries, we can't use the SearchListenerPlugin since that listens for search
        // stage on the local cluster, so we only test final state of the search response
        SearchListenerPlugin.negate();

        Map<String, Object> testClusterInfo = setupTwoClusters();
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        boolean skipUnavailable = (Boolean) testClusterInfo.get("remote.skip_unavailable");

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(REMOTE_CLUSTER + ":" + remoteIndex);
        request.setCcsMinimizeRoundtrips(true);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        // shardId -1 means to throw the Exception on all shards, so should result in complete search failure
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("index corrupted"), -1);
        request.getSearchRequest().source(new SearchSourceBuilder().query(queryBuilder).size(10));

        AsyncSearchResponse response = submitAsyncSearch(request);
        assertNotNull(response.getSearchResponse());
        assertTrue(response.isRunning());

        assertBusy(() -> {
            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            assertFalse(statusResponse.isRunning());
            assertNotNull(statusResponse.getCompletionStatus());
        });

        {
            AsyncSearchResponse finishedResponse = getAsyncSearch(response.getId());

            SearchResponse.Clusters clusters = finishedResponse.getSearchResponse().getClusters();
            assertThat(clusters.getTotal(), equalTo(1));
            assertThat(clusters.getSuccessful(), equalTo(0));
            assertThat(clusters.getSkipped(), equalTo(1));

            assertNull(clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
            assertNotNull(remoteClusterSearchInfo);
            SearchResponse.Cluster.Status expectedStatus = skipUnavailable
                ? SearchResponse.Cluster.Status.SKIPPED
                : SearchResponse.Cluster.Status.FAILED;
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(expectedStatus));
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
            SearchResponse.Clusters clusters = statusResponse.getClusters();
            assertThat(clusters.getTotal(), equalTo(1));
            assertThat(clusters.getSuccessful(), equalTo(0));
            assertThat(clusters.getSkipped(), equalTo(1));

            assertNull(clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
            assertNotNull(remoteClusterSearchInfo);
            SearchResponse.Cluster.Status expectedStatus = skipUnavailable
                ? SearchResponse.Cluster.Status.SKIPPED
                : SearchResponse.Cluster.Status.FAILED;
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(expectedStatus));
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
        request.getSearchRequest().allowPartialSearchResults(false);
        request.getSearchRequest().source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(1000));

        AsyncSearchResponse response = submitAsyncSearch(request);
        assertNotNull(response.getSearchResponse());
        assertTrue(response.isRunning());

        SearchListenerPlugin.waitSearchStarted();

        ActionFuture<CancelTasksResponse> cancelFuture;
        try {
            ListTasksResponse listTasksResponse = client(LOCAL_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .setActions(SearchAction.INSTANCE.name())
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
                    .filter(t -> t.action().contains(SearchAction.NAME))
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
                        if (cancellableTask.getAction().contains(SearchAction.INSTANCE.name())) {
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
                .filter(t -> t.action().contains(SearchAction.INSTANCE.name()))
                .toList();
            for (TaskInfo taskInfo : remoteSearchTasksAfterCancellation) {
                assertTrue(taskInfo.description(), taskInfo.cancelled());
            }

            // check async search status before allowing query to continue but after cancellation
            AsyncSearchResponse searchResponseAfterCancellation = getAsyncSearch(response.getId());
            assertTrue(searchResponseAfterCancellation.isPartial());
            assertTrue(searchResponseAfterCancellation.isRunning());
            assertFalse(searchResponseAfterCancellation.getSearchResponse().isTimedOut());
            assertThat(searchResponseAfterCancellation.getSearchResponse().getClusters().getTotal(), equalTo(2));
            assertThat(searchResponseAfterCancellation.getSearchResponse().getFailedShards(), equalTo(0));

            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            assertTrue(statusResponse.isPartial());
            assertTrue(statusResponse.isRunning());
            assertThat(statusResponse.getClusters().getTotal(), equalTo(2));
            assertThat(statusResponse.getFailedShards(), equalTo(0));
            assertNull(statusResponse.getCompletionStatus());

        } finally {
            SearchListenerPlugin.allowQueryPhase();
        }

        assertBusy(() -> assertTrue(cancelFuture.isDone()));
        assertBusy(() -> {
            final Iterable<TransportService> transportServices = cluster(REMOTE_CLUSTER).getInstances(TransportService.class);
            for (TransportService transportService : transportServices) {
                assertThat(transportService.getTaskManager().getBannedTaskIds(), Matchers.empty());
            }
        });

        // wait until search status endpoint reports it as completed
        assertBusy(() -> {
            AsyncStatusResponse statusResponseAfterCompletion = getAsyncStatus(response.getId());
            assertNotNull(statusResponseAfterCompletion.getCompletionStatus());
        });

        AsyncStatusResponse statusResponseAfterCompletion = getAsyncStatus(response.getId());
        assertTrue(statusResponseAfterCompletion.isPartial());
        assertFalse(statusResponseAfterCompletion.isRunning());
        assertThat(statusResponseAfterCompletion.getClusters().getTotal(), equalTo(2));
        assertThat(statusResponseAfterCompletion.getFailedShards(), greaterThan(0));
        assertThat(statusResponseAfterCompletion.getCompletionStatus(), equalTo(RestStatus.BAD_REQUEST));

        AsyncSearchResponse searchResponseAfterCompletion = getAsyncSearch(response.getId());
        assertTrue(searchResponseAfterCompletion.isPartial());
        assertFalse(searchResponseAfterCompletion.isRunning());
        assertFalse(searchResponseAfterCompletion.getSearchResponse().isTimedOut());
        assertThat(searchResponseAfterCompletion.getSearchResponse().getClusters().getTotal(), equalTo(2));
        assertThat(searchResponseAfterCompletion.getSearchResponse().getFailedShards(), greaterThan(0));
        Throwable cause = ExceptionsHelper.unwrap(searchResponseAfterCompletion.getFailure(), TaskCancelledException.class);
        assertNotNull("TaskCancelledException should be in the causal chain", cause);
        ShardSearchFailure[] shardFailures = searchResponseAfterCompletion.getSearchResponse().getShardFailures();
        assertThat(shardFailures.length, greaterThan(0));
        String json = Strings.toString(
            ChunkedToXContent.wrapAsToXContent(searchResponseAfterCompletion)
                .toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)
        );
        assertThat(json, containsString("task cancelled [by user request]"));
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
        request.getSearchRequest().allowPartialSearchResults(false);
        request.getSearchRequest().source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(1000));

        AsyncSearchResponse response = submitAsyncSearch(request);
        assertNotNull(response.getSearchResponse());
        assertTrue(response.isRunning());

        SearchListenerPlugin.waitSearchStarted();

        try {
            ListTasksResponse listTasksResponse = client(LOCAL_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .setActions(SearchAction.INSTANCE.name())
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
                    .filter(t -> t.action().contains(SearchAction.NAME))
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
                        if (cancellableTask.getAction().contains(SearchAction.INSTANCE.name())) {
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
                .filter(t -> t.action().contains(SearchAction.INSTANCE.name()))
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
            assertThat(statusResponse.getFailedShards(), equalTo(0));
            assertNull(statusResponse.getCompletionStatus());
        } finally {
            SearchListenerPlugin.allowQueryPhase();
        }

        assertBusy(() -> expectThrows(ExecutionException.class, () -> getAsyncStatus(response.getId())));
        assertBusy(() -> {
            final Iterable<TransportService> transportServices = cluster(REMOTE_CLUSTER).getInstances(TransportService.class);
            for (TransportService transportService : transportServices) {
                assertThat(transportService.getTaskManager().getBannedTaskIds(), Matchers.empty());
            }
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/97286")
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
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.getSearchRequest().allowPartialSearchResults(false);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);

        AsyncSearchResponse response = submitAsyncSearch(request);
        assertNotNull(response.getSearchResponse());
        assertTrue(response.isRunning());

        SearchListenerPlugin.waitSearchStarted();

        // ensure tasks are present on both clusters and not cancelled
        try {
            ListTasksResponse listTasksResponse = client(LOCAL_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .setActions(SearchAction.INSTANCE.name())
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
                    .filter(t -> t.action().contains(SearchAction.NAME))
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

        // wait for the async_search task to be cancelled or unregistered
        assertBusy(() -> {
            ListTasksResponse taskResponses = client().admin().cluster().prepareListTasks().setDetailed(true).get();
            List<TaskInfo> asyncSearchTaskInfos = new ArrayList<>();
            for (TaskInfo task : taskResponses.getTasks()) {
                if (task.action().contains("search")) {
                    if (task.description().contains("async_search{indices[")) {
                        asyncSearchTaskInfos.add(task);
                    }
                }
            }

            if (asyncSearchTaskInfos.size() > 0) {
                // if still present, and it is cancelled, then we can proceed with the test
                assertTrue(asyncSearchTaskInfos.get(0).cancelled());
            }
            // if not present, then it has been unregistered and the async search should no longer be running, so can proceed
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> { assertFalse(getAsyncStatus(response.getId()).isRunning()); });

        AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
        assertFalse(statusResponse.isRunning());
        assertEquals(0, statusResponse.getSuccessfulShards());
        assertEquals(0, statusResponse.getSkippedShards());
        assertThat(statusResponse.getFailedShards(), greaterThanOrEqualTo(1));

        assertBusy(() -> {
            final Iterable<TransportService> transportServices = cluster(REMOTE_CLUSTER).getInstances(TransportService.class);
            for (TransportService transportService : transportServices) {
                assertThat(transportService.getTaskManager().getBannedTaskIds(), Matchers.empty());
            }
        });
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
        return client().execute(DeleteAsyncResultAction.INSTANCE, new DeleteAsyncResultRequest(id)).get();
    }

    private Map<String, Object> setupTwoClusters() {
        String localIndex = "demo";
        int numShardsLocal = randomIntBetween(3, 6);
        Settings localSettings = indexSettings(numShardsLocal, 0).build();
        assertAcked(client(LOCAL_CLUSTER).admin().indices().prepareCreate(localIndex).setSettings(localSettings));
        indexDocs(client(LOCAL_CLUSTER), localIndex);

        String remoteIndex = "prod";
        int numShardsRemote = randomIntBetween(3, 6);
        final InternalTestCluster remoteCluster = cluster(REMOTE_CLUSTER);
        remoteCluster.ensureAtLeastNumDataNodes(randomIntBetween(1, 3));
        final Settings.Builder remoteSettings = Settings.builder();
        remoteSettings.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShardsRemote);

        assertAcked(
            client(REMOTE_CLUSTER).admin()
                .indices()
                .prepareCreate(remoteIndex)
                .setSettings(Settings.builder().put(remoteSettings.build()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
        );
        assertFalse(
            client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareHealth(remoteIndex)
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
        int numDocs = between(1, 10);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(index).setSource("f", "v").get();
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
