/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.fetch.ShardFetchRequest;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SearchScrollQueryThenFetchAsyncActionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    public void testShardFailure() {
        internalCluster().ensureAtLeastNumDataNodes(1);
        final String indexName = "test";
        createIndex(
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );

        // Ensure that the scroll yields documents from both shards
        String routingShard0 = routingKeyForShard(indexName, 0);
        String routingShard1 = routingKeyForShard(indexName, 1);
        for (int i = 0; i < 20; i++) {
            prepareIndex(indexName).setRouting(routingShard0).setSource("seq", 2 * i).get();
            prepareIndex(indexName).setRouting(routingShard1).setSource("seq", 2 * i + 1).get();
        }
        refresh(indexName);

        SearchResponse first = client().prepareSearch(indexName)
            .setQuery(matchAllQuery())
            .addSort("seq", SortOrder.ASC)
            .setSize(10)
            .setScroll(TimeValue.timeValueMinutes(1))
            .get();
        String scrollId = first.getScrollId();
        first.decRef();

        AtomicReference<ShardSearchContextId> firstFetch = new AtomicReference<>();
        CountDownLatch firstFetchResponded = new CountDownLatch(1);
        for (String node : internalCluster().getNodeNames()) {
            MockTransportService.getInstance(node)
                .addRequestHandlingBehavior(SearchTransportService.FETCH_ID_SCROLL_ACTION_NAME, (handler, request, channel, task) -> {
                    ShardSearchContextId contextId = ((ShardFetchRequest) request).contextId();
                    // Proceed with first fetch as normal
                    if (firstFetch.compareAndSet(null, contextId) || contextId.equals(firstFetch.get())) {
                        handler.messageReceived(request, new TransportChannel() {
                            @Override
                            public String getProfileName() {
                                return channel.getProfileName();
                            }

                            @Override
                            public void sendResponse(TransportResponse response) {
                                try {
                                    channel.sendResponse(response);
                                } finally {
                                    firstFetchResponded.countDown();
                                }
                            }

                            @Override
                            public void sendResponse(Exception exception) {
                                try {
                                    channel.sendResponse(exception);
                                } finally {
                                    firstFetchResponded.countDown();
                                }
                            }
                        }, task);
                    } else {
                        assertTrue(firstFetchResponded.await(30, TimeUnit.SECONDS));
                        // Fail second fetch
                        channel.sendResponse(new SearchContextMissingException(contextId));
                    }
                });
        }
        try {
            assertResponse(client().prepareSearchScroll(scrollId).setScroll(TimeValue.timeValueMinutes(1)), response -> {
                assertThat(response.getSuccessfulShards(), equalTo(1));
                assertThat(response.getShardFailures().length, equalTo(1));
                assertThat(response.getShardFailures()[0].getCause(), instanceOf(SearchContextMissingException.class));
            });
        } finally {
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(node).clearAllRules();
            }
            client().prepareClearScroll().addScrollId(scrollId).get();
        }
    }
}
