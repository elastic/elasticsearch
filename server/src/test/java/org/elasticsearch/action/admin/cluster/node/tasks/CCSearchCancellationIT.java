/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.node.tasks;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class CCSearchCancellationIT extends AbstractMultiClustersTestCase {
    private static final Map<String, AtomicInteger> queryExecutionCounter = new ConcurrentHashMap<>();
    private static final AtomicReference<CountDownLatch> preQueryLatch = new AtomicReference<>();
    private static final String REMOTE_CLUSTER = "remote";

    @Before
    public void resetTestStates() {
        queryExecutionCounter.clear();
        preQueryLatch.set(new CountDownLatch(1));
    }

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    static void prepareIndex(Client client, String indexName) {
        int numDocs = between(1, 10);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(indexName).setId(Integer.toString(i)).setSource("f", "v").get();
        }
        client.admin().indices().prepareRefresh(indexName).get();
        queryExecutionCounter.put(indexName, new AtomicInteger());
    }

    public void testCancelCrossClusterSearch() throws Exception {
        prepareIndex(client(LOCAL_CLUSTER), "index-1");
        prepareIndex(client(LOCAL_CLUSTER), "index-2");
        prepareIndex(client(REMOTE_CLUSTER), "index-3");
        prepareIndex(client(REMOTE_CLUSTER), "index-4");
        List<String> searchIndices = randomSubsetOf(between(1, 4), "index-1", "index-2", "remote:index-3", "remote:index-4");
        SearchRequest searchRequest = new SearchRequest(searchIndices.toArray(new String[0]));
        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()));
        searchRequest.setCcsMinimizeRoundtrips(randomBoolean());
        final ActionFuture<SearchResponse> searchFuture = client().execute(SearchAction.INSTANCE, searchRequest);
        assertBusy(() -> {
            for (String searchIndex : searchIndices) {
                final int sep = searchIndex.indexOf(":");
                final String key = sep >= 0 ? searchIndex.substring(sep + 1) : searchIndex;
                assertThat(searchIndex, queryExecutionCounter.get(key).get(), greaterThan(0));
            }
        });
        final List<TaskInfo> searchTasks = client().admin().cluster().prepareListTasks()
            .setActions(SearchAction.NAME).setDetailed(true).get().getTasks()
            .stream().filter(task -> task.getParentTaskId().isSet() == false)
            .collect(Collectors.toList());
        assertThat(searchTasks.toString(), searchTasks, hasSize(1));
        final TaskId searchTaskId = searchTasks.get(0).getTaskId();
        ActionFuture<CancelTasksResponse> cancelFuture = client().admin().cluster().prepareCancelTasks()
            .setTaskId(searchTaskId).waitForCompletion(randomBoolean()).execute();
        assertBusy(() -> {
            for (String searchIndex : searchIndices) {
                final int sep = searchIndex.indexOf(":");
                final String clusterAlias = sep < 0 ? LOCAL_CLUSTER : searchIndex.substring(0, sep);
                final InternalTestCluster cluster = cluster(clusterAlias);
                Set<TaskId> banParents = new HashSet<>();
                for (String node : cluster.getNodeNames()) {
                    final TaskManager taskManager = cluster.getInstance(TransportService.class, node).getTaskManager();
                    for (Task task : taskManager.getTasks().values()) {
                        if (task.getParentTaskId().isSet() && task instanceof CancellableTask) {
                            assertTrue(searchIndex, ((CancellableTask) task).isCancelled());
                        }
                    }
                    banParents.addAll(taskManager.getBannedTaskIds());
                }
                assertThat(searchIndex, banParents, not(empty()));
            }
        }, 30, TimeUnit.SECONDS);
        preQueryLatch.get().countDown();
        Exception searchResponse = expectThrows(Exception.class, searchFuture::actionGet);
        assertNotNull(ExceptionsHelper.unwrap(searchResponse, TaskCancelledException.class));
        cancelFuture.actionGet();
    }

    public static class BlockFetchPhasePlugin extends Plugin {
        @Override
        public void onIndexModule(IndexModule indexModule) {
            super.onIndexModule(indexModule);
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onPreQueryPhase(SearchContext searchContext) {
                    final String indexName = searchContext.indexShard().shardId().getIndexName();
                    queryExecutionCounter.get(indexName).incrementAndGet();
                    try {
                        preQueryLatch.get().await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    if (searchContext.isCancelled()) {
                        throw new TaskCancelledException("cancelled");
                    }
                }

                @Override
                public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
                    fail("All search phases were cancelled");
                }
            });
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(BlockFetchPhasePlugin.class);
        return plugins;
    }
}
