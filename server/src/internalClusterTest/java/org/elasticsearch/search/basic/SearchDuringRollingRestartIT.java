/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.basic;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.ConnectTransportException;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, minNumDataNodes = 3)
public class SearchDuringRollingRestartIT extends ESIntegTestCase {

    public void testSearchesSurviveRollingRestart() throws Exception {
        final int docCount = scaledRandomIntBetween(100, 1000);
        final int size = Math.min(100, docCount);

        final List<String> dataNodeNames = internalCluster().clusterService()
            .state()
            .nodes()
            .getDataNodes()
            .values()
            .stream()
            .map(DiscoveryNode::getName)
            .toList();
        assertThat("expected at least 3 data nodes", dataNodeNames.size(), greaterThanOrEqualTo(3));

        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder().put("index.number_of_shards", dataNodeNames.size() + 2).put("index.number_of_replicas", 1)
            )
        );
        ensureGreen("test");

        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < docCount; i++) {
            bulk.add(prepareIndex("test").setId(Integer.toString(i)).setSource("field1", "i" + i));
        }
        assertFalse(bulk.get().hasFailures());

        final String coordinatingNode = randomFrom(dataNodeNames);
        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        final int threadCount = scaledRandomIntBetween(4, 16);

        final List<Thread> searchThreads = IntStream.range(0, threadCount)
            .mapToObj(
                i -> Thread.ofPlatform()
                    .name(getTestName() + "-search-" + i)
                    .daemon(true)
                    .start(() -> searchUntilStopped(coordinatingNode, size, docCount, stop, firstFailure))
            )
            .toList();

        try {
            final int restarts = scaledRandomIntBetween(2, 5);
            for (int i = 0; i < restarts && firstFailure.get() == null; i++) {
                internalCluster().restartNode(randomValueOtherThan(coordinatingNode, () -> randomFrom(dataNodeNames)));
                ensureGreen(TimeValue.timeValueMinutes(2), "test");
            }
        } finally {
            stop.set(true);
            for (Thread t : searchThreads) {
                t.join();
            }
        }

        Throwable failure = firstFailure.get();
        if (failure != null) {
            throw new AssertionError("Unexpected search failure during rolling restart", failure);
        }
    }

    private void searchUntilStopped(
        String coordinatingNode,
        int size,
        int docCount,
        AtomicBoolean stop,
        AtomicReference<Throwable> firstFailure
    ) {
        while (stop.get() == false && firstFailure.get() == null) {
            try {
                SearchRequestBuilder search = client(coordinatingNode).prepareSearch("test")
                    .setSearchType(randomFrom(SearchType.values()))
                    .setSize(size)
                    .setAllowPartialSearchResults(true);
                if (randomBoolean()) {
                    search.setQuery(new WildcardQueryBuilder("field1", "i*"));
                }
                runAndVerify(search, docCount, size);
            } catch (Throwable e) {
                if (isExpectedRollingRestartFailure(e)) {
                    continue;
                }
                firstFailure.compareAndSet(null, e);
                return;
            }
        }
    }

    private void runAndVerify(SearchRequestBuilder builder, int expectedTotalHits, int expectedReturnedHits) {
        SearchResponse response = builder.get(TimeValue.timeValueSeconds(30));
        try {
            assertThat(
                "shard accounting drift: " + response,
                response.getTotalShards(),
                equalTo(response.getSuccessfulShards() + response.getFailedShards() + response.getSkippedShards())
            );
            if (response.getFailedShards() == 0) {
                assertThat(response.getHits().getHits().length, equalTo(expectedReturnedHits));
                assertThat(response.getHits().getTotalHits().value(), equalTo((long) expectedTotalHits));
            } else {
                for (ShardSearchFailure shardFailure : response.getShardFailures()) {
                    assertTrue(
                        "unexpected shard failure during rolling restart: " + shardFailure,
                        isExpectedRollingRestartCause(shardFailure.getCause())
                    );
                }
            }
        } finally {
            response.decRef();
        }
    }

    private static boolean isExpectedRollingRestartFailure(Throwable e) {
        if (e instanceof SearchPhaseExecutionException spee && spee.shardFailures().length > 0) {
            for (ShardSearchFailure shardFailure : spee.shardFailures()) {
                if (isExpectedRollingRestartCause(shardFailure.getCause()) == false) {
                    return false;
                }
            }
            return true;
        }
        return isExpectedRollingRestartCause(e);
    }

    private static boolean isExpectedRollingRestartCause(Throwable cause) {
        return ExceptionsHelper.unwrap(
            cause,
            ConnectTransportException.class,
            SearchContextMissingException.class,
            NoShardAvailableActionException.class
        ) != null;
    }
}
