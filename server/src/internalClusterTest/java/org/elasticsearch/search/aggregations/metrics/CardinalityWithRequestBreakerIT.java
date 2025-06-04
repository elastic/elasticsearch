/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Map;
import java.util.stream.IntStream;

import static org.elasticsearch.search.aggregations.AggregationBuilders.cardinality;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;

public class CardinalityWithRequestBreakerIT extends ESIntegTestCase {

    /**
     * Test that searches using cardinality aggregations returns all request breaker memory.
     */
    public void testRequestBreaker() throws Exception {
        final String requestBreaker = randomIntBetween(1, 10000) + "kb";
        logger.info("--> Using request breaker setting: {}", requestBreaker);

        indexRandom(
            true,
            IntStream.range(0, randomIntBetween(10, 1000))
                .mapToObj(
                    i -> prepareIndex("test").setId("id_" + i)
                        .setSource(Map.of("field0", randomAlphaOfLength(5), "field1", randomAlphaOfLength(5)))
                )
                .toArray(IndexRequestBuilder[]::new)
        );

        updateClusterSettings(
            Settings.builder().put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), requestBreaker)
        );

        try {
            prepareSearch("test").addAggregation(
                terms("terms").field("field0.keyword")
                    .collectMode(randomFrom(Aggregator.SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("cardinality", randomBoolean()))
                    .subAggregation(cardinality("cardinality").precisionThreshold(randomLongBetween(1, 40000)).field("field1.keyword"))
            ).get().decRef();
        } catch (ElasticsearchException e) {
            if (ExceptionsHelper.unwrap(e, CircuitBreakingException.class) == null) {
                throw e;
            }
        }

        updateClusterSettings(Settings.builder().putNull(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey()));

        // validation done by InternalTestCluster.ensureEstimatedStats()
    }
}
