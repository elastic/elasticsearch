/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Checks aggregation correctness and failure propagation through hash-shuffle stages.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 3)
public class ShuffleIT extends AbstractEsqlIntegTestCase {
    private static final int DOC_COUNT = 20_000;
    private static final int BULK_SIZE = 1_000;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(InternalExchangePlugin.class);
        plugins.add(FailingPauseFieldPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, TimeValue.timeValueMinutes(1))
            .build();
    }

    @Before
    public void setupIndices() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("events")
                .setSettings(
                    Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .setMapping("user_id", "type=long", "host", "type=keyword", "bytes", "type=long", "dept", "type=keyword")
        );
        for (int start = 0; start < DOC_COUNT; start += BULK_SIZE) {
            BulkRequestBuilder bulk = client().prepareBulk("events");
            int end = Math.min(start + BULK_SIZE, DOC_COUNT);
            for (int doc = start; doc < end; doc++) {
                Map<String, Object> source = new HashMap<>();
                int userIdKind = randomInt(99);
                if (userIdKind == 0) {
                    source.put("user_id", null);
                } else {
                    long userId = randomLongBetween(0, 2_999);
                    if (userIdKind == 1) {
                        source.put("user_id", List.of(userId, (userId + 1) % 3_000));
                    } else {
                        source.put("user_id", userId);
                    }
                }
                source.put("host", "host-" + randomIntBetween(0, 99));
                source.put("bytes", randomIntBetween(0, 10_000));
                source.put("dept", randomFrom("engineering", "finance", "sales"));
                bulk.add(client().prepareIndex().setSource(source));
            }
            bulk.get();
        }
        client().admin().indices().prepareRefresh("events").get();
    }

    public void testShuffleAggCountByUserId() {
        String query = "FROM events | STATS c = COUNT(*) BY user_id | SORT user_id";
        assertShuffleMatches(query, 1);
    }

    public void testShuffleAgg() {
        assertShuffleMatches(
            "FROM events | STATS c = COUNT(*), b = SUM(bytes) BY user_id, host | SORT c DESC, user_id, host | LIMIT 50",
            randomIntBetween(1, 4)
        );
        assertShuffleMatches(
            "FROM events | STATS c = COUNT(*), b = SUM(bytes) BY user_id, host | SORT c DESC, user_id, host",
            randomIntBetween(1, 4)
        );
    }

    public void testShuffleAggTwoLevel() {
        assertShuffleMatches("FROM events | STATS BY user_id | STATS n = COUNT(*)", randomIntBetween(1, 4));
    }

    public void testProducerFailure() throws Exception {
        FailingPauseFieldPlugin.resetPlugin();
        addFailingRuntimeField();
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM events | STATS c = COUNT(fail_me) BY user_id").acceptedPragmaRisks(true)
            .pragmas(new QueryPragmas(shufflePragmas(randomIntBetween(1, 4))));
        ActionFuture<EsqlQueryResponse> future = client().execute(EsqlQueryAction.INSTANCE, request);

        try {
            assertTrue(FailingPauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS));
        } finally {
            FailingPauseFieldPlugin.release();
        }

        Exception failure = expectThrows(Exception.class, () -> future.actionGet(60, TimeUnit.SECONDS).close());
        assertThat(EsqlTestUtils.unwrapIfWrappedInRemoteException(failure).getMessage(), containsString("Failing query"));
    }

    private void addFailingRuntimeField() throws IOException {
        try (XContentBuilder mapping = JsonXContent.contentBuilder()) {
            mapping.startObject()
                .startObject("runtime")
                .startObject("fail_me")
                .field("type", "long")
                .startObject("script")
                .field("source", "")
                .field("lang", "pause_fail")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
            assertAcked(client().admin().indices().preparePutMapping("events").setSource(mapping));
        }
    }

    private void assertShuffleMatches(String query, int partitionsPerNode) {
        assertThat(runSorted(query, shufflePragmas(partitionsPerNode)), equalTo(runSorted(query, Settings.EMPTY)));
    }

    private Settings shufflePragmas(int partitionsPerNode) {
        return Settings.builder()
            .put(QueryPragmas.SHUFFLE_PARTITIONS_PER_NODE.getKey(), partitionsPerNode)
            .put(QueryPragmas.SHUFFLE_AGG.getKey(), true)
            .put(QueryPragmas.EXCHANGE_BUFFER_SIZE.getKey(), randomIntBetween(1, 10))
            .put(QueryPragmas.TASK_CONCURRENCY.getKey(), randomIntBetween(1, 4))
            .build();
    }

    private List<List<Object>> runSorted(String query, Settings pragmas) {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query).acceptedPragmaRisks(true).pragmas(new QueryPragmas(pragmas)))) {
            List<List<Object>> rows = new ArrayList<>(getValuesList(response));
            rows.sort(Comparator.comparing(Object::toString));
            return rows;
        }
    }
}
