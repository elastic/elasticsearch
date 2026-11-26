/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.aggregatemetric.mapper;

import org.elasticsearch.index.mapper.AbstractColumnarNullContractTestCase;
import org.elasticsearch.index.mapper.IndexingStrategy;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class AggregateMetricDoubleColumnarNullContractTests extends AbstractColumnarNullContractTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new AggregateMetricMapperPlugin());
    }

    // NOTE: aggregate_metric_double does not implement batch indexing today, so the scenario lists no batched shape and the BATCH tests are
    // not
    // required to execute as a batch. They exercise batch indexing as soon as the mapper accepts a column.
    private static Scenario aggregateMetricDouble() {
        return scenario("aggregate_metric_double", () -> {
            final double min = randomIntBetween(0, 100) / 4.0;
            return Map.of("min", min, "max", min + randomIntBetween(0, 20) / 4.0);
        }).withoutIndex()
            .withParameters(Map.of("metrics", List.of("min", "max")))
            .withCanonical(value -> new TreeMap<>((Map<?, ?>) value).toString())
            .asSingleValued();
    }

    public void testAggregateMetricDoubleExistsDocument() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(aggregateMetricDouble(), IndexingStrategy.DOCUMENT);
    }

    public void testAggregateMetricDoubleSourceModesAgreeDocument() throws IOException {
        assertSourceModesAgree(aggregateMetricDouble(), IndexingStrategy.DOCUMENT);
    }

    public void testAggregateMetricDoubleSourceModesAgreeBatch() throws IOException {
        assertSourceModesAgree(aggregateMetricDouble(), IndexingStrategy.BATCH);
    }

    public void testAggregateMetricDoubleExistsBatch() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(aggregateMetricDouble(), IndexingStrategy.BATCH);
    }

    public void testAggregateMetricDoubleExistsCountDocument() throws IOException {
        assertExistsCountAgreesWithHits(aggregateMetricDouble(), IndexingStrategy.DOCUMENT);
    }

    public void testAggregateMetricDoubleExistsCountBatch() throws IOException {
        assertExistsCountAgreesWithHits(aggregateMetricDouble(), IndexingStrategy.BATCH);
    }

    public void testAggregateMetricDoubleSourceDocument() throws IOException {
        assertSourceDropsNullElements(aggregateMetricDouble(), IndexingStrategy.DOCUMENT);
    }

    public void testAggregateMetricDoubleSourceBatch() throws IOException {
        assertSourceDropsNullElements(aggregateMetricDouble(), IndexingStrategy.BATCH);
    }
}
