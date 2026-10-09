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
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SearchScrollQueryThenFetchAsyncActionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    private void testShardFailure(boolean allShardsFail) {
        internalCluster().ensureAtLeastNumDataNodes(1);
        final String indexName = "test";
        final int numShards = randomIntBetween(2, 5);
        final int numToSucceed = allShardsFail ? 0 : randomIntBetween(1, numShards - 1);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );

        // Ensure that the scroll yields documents from all shards
        for (int shard = 0; shard < numShards; shard++) {
            String routing = routingKeyForShard(indexName, shard);
            for (int i = 0; i < 5; i++) {
                prepareIndex(indexName).setRouting(routing).setSource("seq", numShards * i + shard).get();
            }
        }
        refresh(indexName);

        SearchResponse first = client().prepareSearch(indexName)
            .setQuery(matchAllQuery())
            .addSort("seq", SortOrder.ASC)
            .setSize(numShards * 2)
            .setScroll(TimeValue.timeValueMinutes(1))
            .get();
        String scrollId = first.getScrollId();
        first.decRef();

        // Allow only the first numToSucceed fetch requests to succeed, fail the rest
        AtomicInteger fetchCount = new AtomicInteger(0);
        for (String node : internalCluster().getNodeNames()) {
            MockTransportService.getInstance(node)
                .addRequestHandlingBehavior(SearchTransportService.FETCH_ID_SCROLL_ACTION_NAME, (handler, request, channel, task) -> {
                    if (fetchCount.getAndIncrement() < numToSucceed) {
                        handler.messageReceived(request, channel, task);
                    } else {
                        channel.sendResponse(new SearchContextMissingException(((ShardFetchRequest) request).contextId()));
                    }
                });
        }
        try {
            if (allShardsFail) {
                SearchPhaseExecutionException ex = expectThrows(
                    SearchPhaseExecutionException.class,
                    client().prepareSearchScroll(scrollId).setScroll(TimeValue.timeValueMinutes(1))
                );
                assertThat(ex.getPhaseName(), equalTo("fetch"));
                assertThat(ex.getMessage(), equalTo("all shards failed"));
            } else {
                assertResponse(client().prepareSearchScroll(scrollId).setScroll(TimeValue.timeValueMinutes(1)), response -> {
                    assertThat(response.getSuccessfulShards(), equalTo(numToSucceed));
                    assertThat(response.getShardFailures().length, equalTo(numShards - numToSucceed));
                    for (ShardSearchFailure failure : response.getShardFailures()) {
                        assertThat(failure.getCause(), instanceOf(SearchContextMissingException.class));
                    }
                });
            }
        } finally {
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(node).clearAllRules();
            }
            client().prepareClearScroll().addScrollId(scrollId).get();
        }
    }

    public void testPartialShardFailure() {
        testShardFailure(false);
    }

    public void testAllShardsFail() {
        testShardFailure(true);
    }
}
