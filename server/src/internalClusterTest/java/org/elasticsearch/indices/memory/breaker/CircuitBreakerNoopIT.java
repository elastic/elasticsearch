/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.memory.breaker;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.cardinality;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/** Tests for the noop breakers, which are non-dynamic settings */
@ESIntegTestCase.ClusterScope(scope= ESIntegTestCase.Scope.SUITE, numDataNodes=0)
public class CircuitBreakerNoopIT extends ESIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
                .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "noop")
                // This is set low, because if the "noop" is not a noop, it will break
                .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "10b")
                .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "noop")
                // This is set low, because if the "noop" is not a noop, it will break
                .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "10b")
                .build();
    }

    public void testNoopRequestBreaker() throws Exception {
        assertAcked(prepareCreate("cb-test", 1, Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, between(0, 1))));
        Client client = client();

        // index some different terms so we have some field data for loading
        int docCount = scaledRandomIntBetween(300, 1000);
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        for (long id = 0; id < docCount; id++) {
            reqs.add(client.prepareIndex("cb-test").setId(Long.toString(id)).setSource("test", id));
        }
        indexRandom(true, reqs);

        // A cardinality aggregation uses BigArrays and thus the REQUEST breaker
        client.prepareSearch("cb-test").setQuery(matchAllQuery()).addAggregation(cardinality("card").field("test")).get();
        // no exception because the breaker is a noop
    }

    public void testNoopFielddataBreaker() throws Exception {
        assertAcked(prepareCreate("cb-test", 1, Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, between(0, 1))));
        Client client = client();

        // index some different terms so we have some field data for loading
        int docCount = scaledRandomIntBetween(300, 1000);
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        for (long id = 0; id < docCount; id++) {
            reqs.add(client.prepareIndex("cb-test").setId(Long.toString(id)).setSource("test", id));
        }
        indexRandom(true, reqs);

        // Sorting using fielddata and thus the FIELDDATA breaker
        client.prepareSearch("cb-test").setQuery(matchAllQuery()).addSort("test", SortOrder.DESC).get();
        // no exception because the breaker is a noop
    }
}
