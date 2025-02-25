/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class EarlyTerminationIT extends AbstractEsqlIntegTestCase {

    public void testAdjustLimit() {
        assumeTrue("require pragmas", canUseQueryPragmas());
        String dataNode = internalCluster().startDataOnlyNode();
        int numIndices = 10;
        int value = 0;
        for (int i = 0; i < numIndices; i++) {
            String index = "test-" + i;
            assertAcked(
                admin().indices()
                    .prepareCreate(index)
                    .setSettings(indexSettings(1, 0).put("index.routing.allocation.require._name", dataNode))
            );
            List<IndexRequestBuilder> indexRequests = new ArrayList<>();
            for (int d = 0; d < 20; d++) {
                indexRequests.add(new IndexRequestBuilder(client()).setIndex(index).setSource("v", Integer.toString(value++)));
            }
            indexRandom(true, indexRequests);
        }
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query("FROM test-* | LIMIT 95");
        QueryPragmas pragmas = new QueryPragmas(
            Settings.builder()
                .put(QueryPragmas.MAX_CONCURRENT_SHARDS_PER_NODE.getKey(), 2)
                .put(QueryPragmas.DATA_PARTITIONING.getKey(), DataPartitioning.SHARD)
                .put(QueryPragmas.TASK_CONCURRENCY.getKey(), between(3, 5))
                .build()
        );
        request.pragmas(pragmas);
        request.profile(true);
        try (EsqlQueryResponse resp = run(request)) {
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(95));
            List<DriverProfile> drivers = resp.profile().drivers();
            List<DriverProfile> queryProfiles = drivers.stream().filter(d -> d.taskDescription().equals("data")).toList();
            assertThat(queryProfiles, hasSize(6));
            var luceneOperators = queryProfiles.stream().map(d -> (LuceneSourceOperator.Status) d.operators().getFirst().status()).toList();
            assertThat(luceneOperators, hasSize(6));
            assertThat(luceneOperators.get(0).rowsEmitted() + luceneOperators.get(1).rowsEmitted(), equalTo(40L));
            assertThat(luceneOperators.get(2).rowsEmitted() + luceneOperators.get(3).rowsEmitted(), equalTo(40L));
            assertThat(luceneOperators.get(4).rowsEmitted() + luceneOperators.get(5).rowsEmitted(), equalTo(15L));
        }
    }
}
