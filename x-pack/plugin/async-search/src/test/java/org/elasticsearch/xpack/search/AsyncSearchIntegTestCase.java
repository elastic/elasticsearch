/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.search;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
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

import static org.elasticsearch.xpack.search.AsyncSearchTemplateRegistry.ASYNC_SEARCH_ALIAS;
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
        ensureGreen(ASYNC_SEARCH_ALIAS);
    }

    protected AsyncSearchResponse submitAsyncSearch(SubmitAsyncSearchRequest request) throws ExecutionException, InterruptedException {
        return client().execute(SubmitAsyncSearchAction.INSTANCE, request).get();
    }

    protected AsyncSearchResponse getAsyncSearch(String id) throws ExecutionException, InterruptedException {
        return client().execute(GetAsyncSearchAction.INSTANCE,
            new GetAsyncSearchAction.Request(id, TimeValue.MINUS_ONE, -1)).get();
    }

    protected AcknowledgedResponse deleteAsyncSearch(String id) throws ExecutionException, InterruptedException {
        return client().execute(DeleteAsyncSearchAction.INSTANCE, new DeleteAsyncSearchAction.Request(id)).get();
    }

    /**
     * Wait the removal of the document decoded from the provided {@link AsyncSearchId}.
     */
    protected void ensureTaskRemoval(String id) throws Exception {
        AsyncSearchId searchId = AsyncSearchId.decode(id);
        assertBusy(() -> {
            GetResponse resp = client().prepareGet()
                .setIndex(ASYNC_SEARCH_ALIAS)
                .setId(searchId.getDocId())
                .get();
            assertFalse(resp.isExists());
        });
    }

    /**
     * Wait the completion of the {@link TaskId} decoded from the provided {@link AsyncSearchId}.
     */
    protected void ensureTaskCompletion(String id) throws Exception {
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

        final AsyncSearchResponse initial = client().execute(SubmitAsyncSearchAction.INSTANCE, request).get();

        assertTrue(initial.isPartial());
        assertThat(initial.status(), equalTo(RestStatus.OK));
        assertThat(initial.getSearchResponse().getTotalShards(), equalTo(shardLatchArray.length));
        assertThat(initial.getSearchResponse().getSuccessfulShards(), equalTo(0));
        assertThat(initial.getSearchResponse().getShardFailures().length, equalTo(0));

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
                        new GetAsyncSearchAction.Request(response.getId(), TimeValue.timeValueMillis(10), lastVersion)
                    ).get();
                    atomic.set(newResp);
                    assertNotEquals(lastVersion, newResp.getVersion());
                });
                AsyncSearchResponse newResponse = atomic.get();
                lastVersion = newResponse.getVersion();

                if (newResponse.isRunning()) {
                    assertThat(newResponse.status(),  equalTo(RestStatus.OK));
                    assertTrue(newResponse.isPartial());
                    assertFalse(newResponse.getFailure() != null);
                    assertNotNull(newResponse.getSearchResponse());
                    assertThat(newResponse.getSearchResponse().getTotalShards(), equalTo(shardLatchArray.length));
                    assertThat(newResponse.getSearchResponse().getShardFailures().length, lessThanOrEqualTo(numFailures));
                } else if (numFailures == shardLatchArray.length) {
                    assertThat(newResponse.status(),  equalTo(RestStatus.INTERNAL_SERVER_ERROR));
                    assertTrue(newResponse.getFailure() != null);
                    assertTrue(newResponse.isPartial());
                    assertNotNull(newResponse.getSearchResponse());
                    assertThat(newResponse.getSearchResponse().getTotalShards(), equalTo(shardLatchArray.length));
                    assertThat(newResponse.getSearchResponse().getSuccessfulShards(), equalTo(0));
                    assertThat(newResponse.getSearchResponse().getShardFailures().length, equalTo(numFailures));
                    assertNull(newResponse.getSearchResponse().getAggregations());
                    assertNull(newResponse.getSearchResponse().getHits().getTotalHits());
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
            final Query delegate = Queries.newMatchAllQuery();
            return new Query() {
                @Override
                public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
                    if (shardsLatch != null) {
                        try {
                            final ShardIdLatch latch = shardsLatch.get(new ShardId(context.index(), context.getShardId()));
                            latch.await();
                            if (latch.shouldFail) {
                                throw new IOException("boum");
                            }
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return delegate.createWeight(searcher, scoreMode, boost);
                }

                @Override
                public String toString(String field) {
                    return delegate.toString(field);
                }

                @Override
                public boolean equals(Object obj) {
                    return false;
                }

                @Override
                public int hashCode() {
                    return 0;
                }
            };
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
