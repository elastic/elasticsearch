/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.multiterms;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * test forked from CardinalityWithRequestBreakerIT
 */
public class MultiTermsWithRequestBreakerIT extends ESIntegTestCase {

    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(AnalyticsPlugin.class);
    }

    /**
     * Test that searches using multiterms aggregations returns all request breaker memory.
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
                new MultiTermsAggregationBuilder("xxx").terms(
                    List.of(
                        new MultiValuesSourceFieldConfig.Builder().setFieldName("field0.keyword").build(),
                        new MultiValuesSourceFieldConfig.Builder().setFieldName("field1.keyword").build()
                    )
                )
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
