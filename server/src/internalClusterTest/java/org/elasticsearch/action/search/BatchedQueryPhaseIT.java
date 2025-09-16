/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.search.SearchQueryThenFetchAsyncAction.NodeQueryRequest;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

import static org.elasticsearch.action.search.SearchQueryThenFetchAsyncAction.NODE_SEARCH_ACTION_NAME;
import static org.elasticsearch.action.search.SearchType.QUERY_THEN_FETCH;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.equalTo;

public class BatchedQueryPhaseIT extends ESIntegTestCase {

    // All the batched query requests that were made in each test
    private static final List<NodeQueryRequest> batchedQueryRequests = new CopyOnWriteArrayList<>();

    @Before
    public void clear() {
        batchedQueryRequests.clear();
    }

    public static class BatchedQueryCapturePlugin extends Plugin implements NetworkPlugin {
        @Override
        public List<TransportInterceptor> getTransportInterceptors(
            NamedWriteableRegistry namedWriteableRegistry,
            ThreadContext threadContext
        ) {
            return List.of(new TransportInterceptor() {
                @Override
                public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
                    String action,
                    Executor executor,
                    boolean forceExecution,
                    TransportRequestHandler<T> actualHandler
                ) {
                    if (NODE_SEARCH_ACTION_NAME.equals(action)) {
                        return (request, channel, task) -> {
                            batchedQueryRequests.add((NodeQueryRequest) request);
                            actualHandler.messageReceived(request, channel, task);
                        };
                    }
                    return actualHandler;
                }
            });
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(BatchedQueryCapturePlugin.class);
    }

    /**
     * num_reduce_phases tracks the number of times a partial reduction occurs on the coordinating node.
     * This test must be aware of how batched queries are executed because reductions on the data nodes are
     * not counted.
     */
    public void testNumReducePhases() {
        assertAcked(prepareCreate("test-idx").setMapping("title", "type=keyword"));
        for (int i = 0; i < 100; i++) {
            prepareIndex("test-idx").setId(Integer.toString(i)).setSource("title", "testing" + i).get();
        }
        refresh();

        assertNoFailuresAndResponse(
            prepareSearch("test-idx").setBatchedReduceSize(2).addAggregation(terms("terms").field("title")).setSearchType(QUERY_THEN_FETCH),
            response -> {
                final int totalShards = response.getTotalShards();
                final int numShardsBatched = batchedQueryRequests.stream()
                    .map(NodeQueryRequest::numShards)
                    .mapToInt(Integer::intValue)
                    .sum();
                final int coordNodeShards = totalShards - numShardsBatched;

                // Because batched_reduce_size = 2, whenever two or more shard results exist on the coordinating node, they will be
                // partially reduced (batched queries do not count). Hence, the formula: (# of shards on the coordinating node) - 1.
                final int expectedNumReducePhases = Math.max(1, coordNodeShards - 1);
                assertThat(response.getNumReducePhases(), equalTo(expectedNumReducePhases));
            }
        );
    }
}
