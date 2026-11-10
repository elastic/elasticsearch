/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.StreamQueryTestUtils;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * The time ES|QL spends on a query is charged to the shards it reads, on the nodes holding them.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class EsqlSearchLoadIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // no reaper of search contexts on the search pool while the queries run
            .put(SearchService.KEEPALIVE_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1))
            .build();
    }

    public void testChargesTheIndicesTheQueryReads() throws Exception {
        internalCluster().startNode();
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        assertAcked(prepareCreate("read").setSettings(indexSettings(between(1, 3), 0)).setMapping("k", "type=keyword", "v", "type=long"));
        assertAcked(prepareCreate("other").setSettings(indexSettings(1, 0)).setMapping("k", "type=keyword", "v", "type=long"));
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 1_000; i++) {
            bulk.add(prepareIndex(randomFrom("read", "other")).setSource("k", "k" + (i % 10), "v", i));
        }
        assertNoFailures(bulk.get());

        for (int i = between(1, 5); i > 0; i--) {
            var request = syncEsqlQueryRequest(randomFrom("FROM read | STATS c = COUNT(*) BY k", "FROM read | SORT v DESC | LIMIT 10"));
            if (randomBoolean()) {
                StreamQueryTestUtils.executeStreamRequest(
                    client(coordinator),
                    request,
                    new StreamQueryTestUtils.CountingStreamSubscriber()
                );
            } else {
                try (var response = client(coordinator).execute(EsqlQueryAction.INSTANCE, request).actionGet()) {
                    assertNotNull(response);
                }
            }
        }

        // the data node charges the shards it read
        assertThat(searchLoad("read"), greaterThan(0.0));
        assertThat(searchLoad("other"), equalTo(0.0));
    }

    private static double searchLoad(String index) {
        return indicesAdmin().prepareStats(index)
            .setSearch(true)
            .get()
            .getIndex(index)
            .getTotal()
            .getSearch()
            .getTotal()
            .getSearchLoadRate();
    }
}
