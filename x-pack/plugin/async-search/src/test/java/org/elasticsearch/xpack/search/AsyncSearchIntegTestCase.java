/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.DeleteAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;
import org.elasticsearch.xpack.ilm.IndexLifecycle;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.search.AsyncSearch.INDEX;
import static org.elasticsearch.xpack.search.AsyncSearchMaintenanceService.ASYNC_SEARCH_CLEANUP_INTERVAL_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public abstract class AsyncSearchIntegTestCase extends ESIntegTestCase {
    interface SearchResponseIterator extends Iterator<AsyncSearchResponse>, Closeable {}

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, AsyncSearch.class, IndexLifecycle.class,
            SearchTestPlugin.class, ReindexPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(0))
            .put(ASYNC_SEARCH_CLEANUP_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(1))
            .build();
    }

    /**
     * Restart the node that runs the {@link TaskId} decoded from the provided {@link AsyncExecutionId}.
     */
    protected void restartTaskNode(String id) throws Exception {
        AsyncExecutionId searchId = AsyncExecutionId.decode(id);
        final ClusterStateResponse clusterState = client().admin().cluster()
            .prepareState().clear().setNodes(true).get();
        DiscoveryNode node = clusterState.getState().nodes().get(searchId.getTaskId().getNodeId());
        internalCluster().restartNode(node.getName(), new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                return super.onNodeStopped(nodeName);
            }
        });
        ensureYellow(INDEX);
    }

    protected AsyncSearchResponse submitAsyncSearch(SubmitAsyncSearchRequest request) throws ExecutionException, InterruptedException {
        return client().execute(SubmitAsyncSearchAction.INSTANCE, request).get();
    }

    protected AsyncSearchResponse getAsyncSearch(String id) throws ExecutionException, InterruptedException {
        return client().execute(GetAsyncSearchAction.INSTANCE, new GetAsyncSearchAction.Request(id)).get();
    }

    protected AsyncSearchResponse getAsyncSearch(String id, TimeValue keepAlive) throws ExecutionException, InterruptedException {
        return client().execute(GetAsyncSearchAction.INSTANCE, new GetAsyncSearchAction.Request(id).setKeepAlive(keepAlive)).get();
    }

    protected AcknowledgedResponse deleteAsyncSearch(String id) throws ExecutionException, InterruptedException {
        return client().execute(DeleteAsyncSearchAction.INSTANCE, new DeleteAsyncSearchAction.Request(id)).get();
    }

    /**
     * Wait the removal of the document decoded from the provided {@link AsyncExecutionId}.
     */
    protected void ensureTaskRemoval(String id) throws Exception {
        AsyncExecutionId searchId = AsyncExecutionId.decode(id);
        assertBusy(() -> {
            GetResponse resp = client().prepareGet()
                .setIndex(INDEX)
                .setId(searchId.getDocId())
                .get();
            assertFalse(resp.isExists());
        });
    }

    protected void ensureTaskNotRunning(String id) throws Exception {
        assertBusy(() -> {
            try {
                AsyncSearchResponse resp = getAsyncSearch(id);
                assertFalse(resp.isRunning());
            } catch (Exception exc) {
                if (ExceptionsHelper.unwrapCause(exc.getCause()) instanceof ResourceNotFoundException == false) {
                    throw exc;
                }
            }
        });
    }

    /**
     * Wait the completion of the {@link TaskId} decoded from the provided {@link AsyncExecutionId}.
     */
    protected void ensureTaskCompletion(String id) throws Exception {
        assertBusy(() -> {
            TaskId taskId = AsyncExecutionId.decode(id).getTaskId();
            try {
                GetTaskResponse resp = client().admin().cluster()
                    .prepareGetTask(taskId).get();
                assertNull(resp.getTask());
            } catch (Exception exc) {
                if (exc.getCause() instanceof ResourceNotFoundException == false) {
                    throw exc;
                }
            }
        });
    }

    protected SearchResponseIterator assertBlockingIterator(String indexName,
                                                            SearchSourceBuilder source,
                                                            int numFailures,
                                                            int progressStep) throws Exception {
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(source, indexName);
        request.setBatchedReduceSize(progressStep);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        ClusterSearchShardsResponse response = dataNodeClient().admin().cluster()
            .prepareSearchShards(request.getSearchRequest().indices()).get();
        AtomicInteger failures = new AtomicInteger(numFailures);
        Map<ShardId, ShardIdLatch> shardLatchMap = Arrays.stream(response.getGroups())
            .map(ClusterSearchShardsGroup::getShardId)
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    id -> new ShardIdLatch(id, failures.decrementAndGet() >= 0)
                )
            );
        ShardIdLatch[] shardLatchArray = shardLatchMap.values().stream()
            .sorted(Comparator.comparing(ShardIdLatch::shardId))
            .toArray(ShardIdLatch[]::new);
        resetPluginsLatch(shardLatchMap);
        request.getSearchRequest().source().query(new BlockingQueryBuilder(shardLatchMap));

        final AsyncSearchResponse initial = client().execute(SubmitAsyncSearchAction.INSTANCE, request).get();

        assertTrue(initial.isPartial());
        assertThat(initial.status(), equalTo(RestStatus.OK));
        assertThat(initial.getSearchResponse().getTotalShards(), equalTo(shardLatchArray.length));
        assertThat(initial.getSearchResponse().getSuccessfulShards(), equalTo(0));
        assertThat(initial.getSearchResponse().getShardFailures().length, equalTo(0));

        return new SearchResponseIterator() {
            private AsyncSearchResponse response = initial;
            private int shardIndex = 0;
            private boolean isFirst = true;

            @Override
            public boolean hasNext() {
                return response.isRunning();
            }

            @Override
            public AsyncSearchResponse next() {
                try {
                    return doNext();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            private AsyncSearchResponse doNext() throws Exception {
                if (isFirst) {
                    isFirst = false;
                    return response;
                }
                AtomicReference<AsyncSearchResponse> atomic = new AtomicReference<>();
                int step = shardIndex == 0 ? progressStep+1 : progressStep-1;
                int index = 0;
                while (index < step && shardIndex < shardLatchArray.length) {
                    if (shardLatchArray[shardIndex].shouldFail() == false) {
                        ++index;
                    }
                    shardLatchArray[shardIndex++].countDown();
                }
                AsyncSearchResponse newResponse = client().execute(GetAsyncSearchAction.INSTANCE,
                    new GetAsyncSearchAction.Request(response.getId())
                        .setWaitForCompletion(TimeValue.timeValueMillis(10))).get();

                if (newResponse.isRunning()) {
                    assertThat(newResponse.status(),  equalTo(RestStatus.OK));
                    assertTrue(newResponse.isPartial());
                    assertNull(newResponse.getFailure());
                    assertNotNull(newResponse.getSearchResponse());
                    assertThat(newResponse.getSearchResponse().getTotalShards(), equalTo(shardLatchArray.length));
                    assertThat(newResponse.getSearchResponse().getShardFailures().length, lessThanOrEqualTo(numFailures));
                } else if (numFailures == shardLatchArray.length) {
                    assertThat(newResponse.status(),  equalTo(RestStatus.INTERNAL_SERVER_ERROR));
                    assertNotNull(newResponse.getFailure());
                    assertTrue(newResponse.isPartial());
                    assertNotNull(newResponse.getSearchResponse());
                    assertThat(newResponse.getSearchResponse().getTotalShards(), equalTo(shardLatchArray.length));
                    assertThat(newResponse.getSearchResponse().getSuccessfulShards(), equalTo(0));
                    assertThat(newResponse.getSearchResponse().getShardFailures().length, equalTo(numFailures));
                    assertNull(newResponse.getSearchResponse().getAggregations());
                    assertNotNull(newResponse.getSearchResponse().getHits().getTotalHits());
                    assertThat(newResponse.getSearchResponse().getHits().getTotalHits().value, equalTo(0L));
                    assertThat(newResponse.getSearchResponse().getHits().getTotalHits().relation,
                        equalTo(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));
                } else {
                    assertThat(newResponse.status(),  equalTo(RestStatus.OK));
                    assertNotNull(newResponse.getSearchResponse());
                    assertFalse(newResponse.isPartial());
                    assertThat(newResponse.status(), equalTo(RestStatus.OK));
                    assertThat(newResponse.getSearchResponse().getTotalShards(), equalTo(shardLatchArray.length));
                    assertThat(newResponse.getSearchResponse().getShardFailures().length, equalTo(numFailures));
                    assertThat(newResponse.getSearchResponse().getSuccessfulShards(),
                        equalTo(shardLatchArray.length-newResponse.getSearchResponse().getShardFailures().length));
                }
                return response = newResponse;
            }

            @Override
            public void close() {
                Arrays.stream(shardLatchArray).forEach(shard -> {
                    if (shard.getCount() == 1) {
                        shard.countDown();
                    }
                });
            }
        };
    }

    private void resetPluginsLatch(Map<ShardId, ShardIdLatch> newLatch) {
        for (PluginsService pluginsService : internalCluster().getDataNodeInstances(PluginsService.class)) {
            pluginsService.filterPlugins(SearchTestPlugin.class).forEach(p -> p.resetQueryLatch(newLatch));
        }
    }
}
