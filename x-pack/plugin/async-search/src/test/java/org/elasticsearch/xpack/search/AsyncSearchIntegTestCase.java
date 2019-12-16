/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.search;

import org.apache.lucene.search.Query;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.DeleteAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;
import org.elasticsearch.xpack.ilm.IndexLifecycle;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public abstract class AsyncSearchIntegTestCase extends ESIntegTestCase {
    interface SearchResponseIterator extends Iterator<AsyncSearchResponse>, Closeable {}

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, AsyncSearch.class, IndexLifecycle.class, QueryBlockPlugin.class);
    }

    /**
     * Restart the node that runs the {@link TaskId} decoded from the provided {@link AsyncSearchId}.
     */
    protected void restartTaskNode(String id) throws Exception {
        AsyncSearchId searchId = AsyncSearchId.decode(id);
        final ClusterStateResponse clusterState = client().admin().cluster()
            .prepareState().clear().setNodes(true).get();
        DiscoveryNode node = clusterState.getState().nodes().get(searchId.getTaskId().getNodeId());
        internalCluster().restartNode(node.getName(), new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                return super.onNodeStopped(nodeName);
            }
        });
        ensureGreen(searchId.getIndexName());
    }

    protected AsyncSearchResponse submitAsyncSearch(SubmitAsyncSearchRequest request) throws ExecutionException, InterruptedException {
        return client().execute(SubmitAsyncSearchAction.INSTANCE, request).get();
    }

    protected AsyncSearchResponse getAsyncSearch(String id) throws ExecutionException, InterruptedException {
        return client().execute(GetAsyncSearchAction.INSTANCE,
            new GetAsyncSearchAction.Request(id, TimeValue.MINUS_ONE, -1, true)).get();
    }

    protected AcknowledgedResponse deleteAsyncSearch(String id) throws ExecutionException, InterruptedException {
        return client().execute(DeleteAsyncSearchAction.INSTANCE, new DeleteAsyncSearchAction.Request(id)).get();
    }

    /**
     * Wait the removal of the document decoded from the provided {@link AsyncSearchId}.
     */
    protected void waitTaskRemoval(String id) throws Exception {
        AsyncSearchId searchId = AsyncSearchId.decode(id);
        assertBusy(() -> {
            GetResponse resp = client().prepareGet()
                .setRouting(searchId.getDocId())
                .setIndex(searchId.getIndexName())
                .setId(searchId.getDocId())
                .get();
            assertFalse(resp.isExists());
        });
    }

    /**
     * Wait the completion of the {@link TaskId} decoded from the provided {@link AsyncSearchId}.
     */
    protected void waitTaskCompletion(String id) throws Exception {
        assertBusy(() -> {
            TaskId taskId = AsyncSearchId.decode(id).getTaskId();
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
        request.setWaitForCompletion(TimeValue.timeValueMillis(1));
        ClusterSearchShardsResponse response = dataNodeClient().admin().cluster()
            .prepareSearchShards(request.getSearchRequest().indices()).get();
        AtomicInteger failures = new AtomicInteger(numFailures);
        Map<ShardId, ShardIdLatch> shardLatchMap = Arrays.stream(response.getGroups())
            .map(ClusterSearchShardsGroup::getShardId)
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    id -> new ShardIdLatch(id, new CountDownLatch(1), failures.decrementAndGet() >= 0 ? true : false)
                )
            );
        ShardIdLatch[] shardLatchArray = shardLatchMap.values().stream()
            .sorted(Comparator.comparing(ShardIdLatch::shard))
            .toArray(ShardIdLatch[]::new);
        resetPluginsLatch(shardLatchMap);
        request.source().query(new BlockQueryBuilder(shardLatchMap));

        final AsyncSearchResponse initial;
        {
            AsyncSearchResponse resp = client().execute(SubmitAsyncSearchAction.INSTANCE, request).get();
            while (resp.getPartialResponse().getSuccessfulShards() == -1) {
                resp = client().execute(GetAsyncSearchAction.INSTANCE,
                    new GetAsyncSearchAction.Request(resp.id(), TimeValue.timeValueSeconds(1), resp.getVersion(), true)).get();
            }
            initial = resp;
        }

        assertTrue(initial.hasPartialResponse());
        assertThat(initial.status(), equalTo(RestStatus.PARTIAL_CONTENT));
        assertThat(initial.getPartialResponse().getTotalShards(), equalTo(shardLatchArray.length));
        assertThat(initial.getPartialResponse().getSuccessfulShards(), equalTo(0));
        assertThat(initial.getPartialResponse().getShardFailures(), equalTo(0));

        return new SearchResponseIterator() {
            private AsyncSearchResponse response = initial;
            private int lastVersion = initial.getVersion();
            private int shardIndex = 0;
            private boolean isFirst = true;
            private int shardFailures = 0;

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
                AtomicReference<Exception> exc = new AtomicReference<>();
                int step = shardIndex == 0 ? progressStep+1 : progressStep-1;
                int index = 0;
                while (index < step && shardIndex < shardLatchArray.length) {
                    if (shardLatchArray[shardIndex].shouldFail == false) {
                        ++index;
                    } else {
                        ++shardFailures;
                    }
                    shardLatchArray[shardIndex++].countDown();
                }
                assertBusy(() -> {
                    AsyncSearchResponse newResp = client().execute(GetAsyncSearchAction.INSTANCE,
                        new GetAsyncSearchAction.Request(response.id(), TimeValue.timeValueMillis(10), lastVersion, true)
                    ).get();
                    atomic.set(newResp);
                    assertNotEquals(RestStatus.NOT_MODIFIED, newResp.status());
                });
                AsyncSearchResponse newResponse = atomic.get();
                lastVersion = newResponse.getVersion();

                if (newResponse.isRunning()) {
                    assertThat(newResponse.status(),  equalTo(RestStatus.PARTIAL_CONTENT));
                    assertTrue(newResponse.hasPartialResponse());
                    assertFalse(newResponse.hasFailed());
                    assertFalse(newResponse.isFinalResponse());
                    assertThat(newResponse.getPartialResponse().getTotalShards(), equalTo(shardLatchArray.length));
                    assertThat(newResponse.getPartialResponse().getShardFailures(), lessThanOrEqualTo(numFailures));
                } else if (numFailures == shardLatchArray.length) {
                    assertThat(newResponse.status(),  equalTo(RestStatus.INTERNAL_SERVER_ERROR));
                    assertTrue(newResponse.hasFailed());
                    assertTrue(newResponse.hasPartialResponse());
                    assertFalse(newResponse.isFinalResponse());
                    assertThat(newResponse.getPartialResponse().getTotalShards(), equalTo(shardLatchArray.length));
                    assertThat(newResponse.getPartialResponse().getSuccessfulShards(), equalTo(0));
                    assertThat(newResponse.getPartialResponse().getShardFailures(), equalTo(numFailures));
                    assertNull(newResponse.getPartialResponse().getAggregations());
                    assertNull(newResponse.getPartialResponse().getTotalHits());
                } else {
                    assertThat(newResponse.status(),  equalTo(RestStatus.OK));
                    assertTrue(newResponse.isFinalResponse());
                    assertFalse(newResponse.hasPartialResponse());
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
                    if (shard.latch.getCount() == 1) {
                        shard.latch.countDown();
                    }
                });
            }
        };
    }

    private void resetPluginsLatch(Map<ShardId, ShardIdLatch> newLatch) {
        for (PluginsService pluginsService : internalCluster().getDataNodeInstances(PluginsService.class)) {
            pluginsService.filterPlugins(QueryBlockPlugin.class).forEach(p -> p.reset(newLatch));
        }
    }

    public static class QueryBlockPlugin extends Plugin implements SearchPlugin {
        private Map<ShardId, ShardIdLatch> shardsLatch;

        public QueryBlockPlugin() {
            this.shardsLatch = null;
        }

        public void reset(Map<ShardId, ShardIdLatch> newLatch) {
            shardsLatch = newLatch;
        }

        @Override
        public List<QuerySpec<?>> getQueries() {
            return Collections.singletonList(
                new QuerySpec<>("block_match_all",
                    in -> new BlockQueryBuilder(in, shardsLatch),
                    p -> BlockQueryBuilder.fromXContent(p, shardsLatch))
            );
        }
    }

    private static class BlockQueryBuilder extends AbstractQueryBuilder<BlockQueryBuilder> {
        public static final String NAME = "block_match_all";
        private final Map<ShardId, ShardIdLatch> shardsLatch;

        private BlockQueryBuilder(Map<ShardId, ShardIdLatch> shardsLatch) {
            super();
            this.shardsLatch = shardsLatch;
        }

        BlockQueryBuilder(StreamInput in, Map<ShardId, ShardIdLatch> shardsLatch) throws IOException {
            super(in);
            this.shardsLatch = shardsLatch;
        }

        private BlockQueryBuilder() {
            this.shardsLatch = null;
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {}

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.endObject();
        }

        private static final ObjectParser<BlockQueryBuilder, Void> PARSER = new ObjectParser<>(NAME, BlockQueryBuilder::new);

        public static BlockQueryBuilder fromXContent(XContentParser parser, Map<ShardId, ShardIdLatch> shardsLatch) {
            try {
                PARSER.apply(parser, null);
                return new BlockQueryBuilder(shardsLatch);
            } catch (IllegalArgumentException e) {
                throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
            }
        }

        @Override
        protected Query doToQuery(QueryShardContext context) {
            if (shardsLatch != null) {
                try {
                    final ShardIdLatch latch = shardsLatch.get(new ShardId(context.index(), context.getShardId()));
                    latch.await();
                    if (latch.shouldFail) {
                        throw new IllegalStateException("boum");
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            return Queries.newMatchAllQuery();
        }

        @Override
        protected boolean doEquals(BlockQueryBuilder other) {
            return true;
        }

        @Override
        protected int doHashCode() {
            return 0;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }

    private static class ShardIdLatch {
        private final ShardId shard;
        private final CountDownLatch latch;
        private final boolean shouldFail;

        private ShardIdLatch(ShardId shard, CountDownLatch latch, boolean shouldFail) {
            this.shard = shard;
            this.latch = latch;
            this.shouldFail = shouldFail;
        }

        ShardId shard() {
            return shard;
        }

        void countDown() {
            latch.countDown();
        }

        void await() throws InterruptedException {
            latch.await();
        }
    }
}
