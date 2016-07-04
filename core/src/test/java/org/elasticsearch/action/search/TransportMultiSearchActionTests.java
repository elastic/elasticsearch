/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportMultiSearchActionTests extends ESTestCase {

    public void testBatchExecute() throws Exception {
        // Initialize depedencies of TransportMultiSearchAction
        Settings settings = Settings.builder()
                .put("node.name", TransportMultiSearchActionTests.class.getSimpleName())
                .build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        ThreadPool threadPool = new ThreadPool(settings);
        TaskManager taskManager = mock(TaskManager.class);
        TransportService transportService = mock(TransportService.class);
        when(transportService.getTaskManager()).thenReturn(taskManager);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("test")).build());
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(Settings.EMPTY);

        // Keep track of the number of concurrent searches started by multi search api,
        // and if there are more searches than is allowed create an error and remember that.
        int maxAllowedConcurrentSearches = scaledRandomIntBetween(1, 20);
        AtomicInteger counter = new AtomicInteger();
        AtomicReference<AssertionError> errorHolder = new AtomicReference<>();
        TransportAction<SearchRequest, SearchResponse> searchAction = new TransportAction<SearchRequest, SearchResponse>
                (Settings.EMPTY, "action", threadPool, actionFilters, resolver, taskManager) {
            @Override
            protected void doExecute(SearchRequest request, ActionListener<SearchResponse> listener) {
                int currentConcurrentSearches = counter.incrementAndGet();
                if (currentConcurrentSearches > maxAllowedConcurrentSearches) {
                    errorHolder.set(new AssertionError("Current concurrent search [" + currentConcurrentSearches +
                            "] is higher than is allowed [" + maxAllowedConcurrentSearches + "]"));
                }
                threadPool.executor(ThreadPool.Names.GENERIC).execute(
                        () -> {
                            try {
                                Thread.sleep(scaledRandomIntBetween(10, 1000));
                            } catch (InterruptedException e) {
                            }
                            counter.decrementAndGet();
                            listener.onResponse(new SearchResponse());
                        }
                );
            }
        };
        TransportMultiSearchAction action =
                new TransportMultiSearchAction(threadPool, actionFilters, transportService, clusterService, searchAction, resolver, 10);

        // Execute the multi search api and fail if we find an error after executing:
        try {
            int numSearchRequests = randomIntBetween(16, 128);
            MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
            multiSearchRequest.maxConcurrentSearchRequests(maxAllowedConcurrentSearches);
            for (int i = 0; i < numSearchRequests; i++) {
                multiSearchRequest.add(new SearchRequest());
            }

            MultiSearchResponse response = action.execute(multiSearchRequest).actionGet();
            assertThat(response.getResponses().length, equalTo(numSearchRequests));
            assertThat(errorHolder.get(), nullValue());
        } finally {
            assertTrue(ESTestCase.terminate(threadPool));
        }
    }

    public void testDefaultMaxConcurrentSearches() {
        int numDataNodes = randomIntBetween(1, 10);
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (int i = 0; i < numDataNodes; i++) {
            builder.put(new DiscoveryNode("_id" + i, new LocalTransportAddress("_id" + i), Collections.emptyMap(),
                    Collections.singleton(DiscoveryNode.Role.DATA), Version.CURRENT));
        }
        builder.put(new DiscoveryNode("master", new LocalTransportAddress("mater"), Collections.emptyMap(),
                Collections.singleton(DiscoveryNode.Role.MASTER), Version.CURRENT));
        builder.put(new DiscoveryNode("ingest", new LocalTransportAddress("ingest"), Collections.emptyMap(),
                Collections.singleton(DiscoveryNode.Role.INGEST), Version.CURRENT));

        ClusterState state = ClusterState.builder(new ClusterName("_name")).nodes(builder).build();
        int result = TransportMultiSearchAction.defaultMaxConcurrentSearches(10, state);
        assertThat(result, equalTo(10 * numDataNodes));

        state = ClusterState.builder(new ClusterName("_name")).build();
        result = TransportMultiSearchAction.defaultMaxConcurrentSearches(10, state);
        assertThat(result, equalTo(1));
    }

}
