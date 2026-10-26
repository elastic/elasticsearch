/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.multiterms;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Reproduces the scenario from the multi_terms missing-field report: a search
 * over an alias spanning indices where one index has neither of the term
 * fields mapped. The terms aggregation handles this; multi_terms should too.
 */
public class MultiTermsMissingFieldIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(AnalyticsPlugin.class);
    }

    public void testMultiTermsAcrossIndicesWithMissingFields() throws Exception {
        indicesAdmin().prepareCreate("integer").setMapping("value", "type=integer", "value2", "type=integer").get();
        indicesAdmin().prepareCreate("long").setMapping("value", "type=long", "value2", "type=long").get();
        indicesAdmin().prepareCreate("nothing").get();
        indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .addAlias("integer", "allThree")
            .addAlias("integer", "intAndLong")
            .addAlias("long", "allThree")
            .addAlias("long", "intAndLong")
            .addAlias("nothing", "allThree")
            .get();

        // Exact shape of the reported repro: no documents anywhere.
        var response = prepareSearch("allThree").setSize(0)
            .addAggregation(
                new MultiTermsAggregationBuilder("agg").terms(
                    List.of(
                        new MultiValuesSourceFieldConfig.Builder().setFieldName("value").build(),
                        new MultiValuesSourceFieldConfig.Builder().setFieldName("value2").build()
                    )
                ).size(3)
            )
            .get();
        try {
            InternalMultiTerms agg = response.getAggregations().get("agg");
            assertThat(agg.getBuckets(), hasSize(0));
        } finally {
            response.decRef();
        }

        // And with data on the mapped indices, buckets should merge across the alias.
        prepareIndex("integer").setId("1").setSource(Map.of("value", 1, "value2", 2)).get();
        prepareIndex("long").setId("2").setSource(Map.of("value", 1L, "value2", 2L)).get();
        prepareIndex("nothing").setId("3").setSource(Map.of("unrelated", "x")).get();
        indicesAdmin().prepareRefresh().get();

        response = prepareSearch("allThree").setSize(0)
            .addAggregation(
                new MultiTermsAggregationBuilder("agg").terms(
                    List.of(
                        new MultiValuesSourceFieldConfig.Builder().setFieldName("value").build(),
                        new MultiValuesSourceFieldConfig.Builder().setFieldName("value2").build()
                    )
                ).size(3)
            )
            .get();
        try {
            InternalMultiTerms agg = response.getAggregations().get("agg");
            assertThat(agg.getBuckets(), hasSize(1));
            assertThat(agg.getBuckets().get(0).getKey(), contains(equalTo(1L), equalTo(2L)));
            assertThat(agg.getBuckets().get(0).getDocCount(), equalTo(2L));
        } finally {
            response.decRef();
        }
    }
}
