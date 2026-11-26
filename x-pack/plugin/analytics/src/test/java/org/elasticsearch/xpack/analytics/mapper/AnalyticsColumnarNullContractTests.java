/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.mapper;

import org.elasticsearch.index.mapper.AbstractColumnarNullContractTestCase;
import org.elasticsearch.index.mapper.IndexingStrategy;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class AnalyticsColumnarNullContractTests extends AbstractColumnarNullContractTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new AnalyticsPlugin());
    }

    // NOTE: histogram, tdigest and exponential_histogram does not implement batch indexing today, so the scenario lists no batched shape
    // and the BATCH tests are not
    // required to execute as a batch. They exercise batch indexing as soon as the mapper accepts a column.
    private static Scenario histogram() {
        return scenario("histogram", () -> {
            final double lower = randomIntBetween(0, 100) / 4.0;
            return Map.of("values", List.of(lower, lower + 1.5), "counts", List.of(randomIntBetween(1, 5), randomIntBetween(1, 5)));
        }).withoutIndex().withCanonical(value -> new TreeMap<>((Map<?, ?>) value).toString()).asSingleValued();
    }

    private static Scenario tdigest() {
        return scenario("tdigest", () -> {
            final double lower = randomIntBetween(0, 100) / 4.0;
            return Map.of("centroids", List.of(lower, lower + 1.5), "counts", List.of(randomIntBetween(1, 5), randomIntBetween(1, 5)));
        }).withoutIndex().withCanonical(value -> canonical(value, "centroids", "counts")).asSingleValued();
    }

    private static Scenario exponentialHistogram() {
        return scenario("exponential_histogram", () -> Map.of("scale", 0, "zero", Map.of("count", randomIntBetween(1, 5)))).withoutIndex()
            .withCanonical(value -> canonical(value, "scale", "zero"))
            .asSingleValued();
    }

    // NOTE: the source of a tdigest and of an exponential histogram adds the min, the max and the sum that it computes from the input,
    // so only the keys of the input are compared.
    private static String canonical(Object value, String... keys) {
        final Map<Object, Object> kept = new TreeMap<>();
        for (final String key : keys) {
            kept.put(key, ((Map<?, ?>) value).get(key));
        }
        return kept.toString();
    }

    public void testHistogramExistsDocument() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(histogram(), IndexingStrategy.DOCUMENT);
    }

    public void testHistogramSourceModesAgreeDocument() throws IOException {
        assertSourceModesAgree(histogram(), IndexingStrategy.DOCUMENT);
    }

    public void testHistogramSourceModesAgreeBatch() throws IOException {
        assertSourceModesAgree(histogram(), IndexingStrategy.BATCH);
    }

    public void testHistogramExistsBatch() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(histogram(), IndexingStrategy.BATCH);
    }

    public void testHistogramExistsCountDocument() throws IOException {
        assertExistsCountAgreesWithHits(histogram(), IndexingStrategy.DOCUMENT);
    }

    public void testHistogramExistsCountBatch() throws IOException {
        assertExistsCountAgreesWithHits(histogram(), IndexingStrategy.BATCH);
    }

    public void testHistogramSourceDocument() throws IOException {
        assertSourceDropsNullElements(histogram(), IndexingStrategy.DOCUMENT);
    }

    public void testHistogramSourceBatch() throws IOException {
        assertSourceDropsNullElements(histogram(), IndexingStrategy.BATCH);
    }

    public void testTdigestExistsDocument() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(tdigest(), IndexingStrategy.DOCUMENT);
    }

    public void testTdigestSourceModesAgreeDocument() throws IOException {
        assertSourceModesAgree(tdigest(), IndexingStrategy.DOCUMENT);
    }

    public void testTdigestSourceModesAgreeBatch() throws IOException {
        assertSourceModesAgree(tdigest(), IndexingStrategy.BATCH);
    }

    public void testTdigestExistsBatch() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(tdigest(), IndexingStrategy.BATCH);
    }

    public void testTdigestExistsCountDocument() throws IOException {
        assertExistsCountAgreesWithHits(tdigest(), IndexingStrategy.DOCUMENT);
    }

    public void testTdigestExistsCountBatch() throws IOException {
        assertExistsCountAgreesWithHits(tdigest(), IndexingStrategy.BATCH);
    }

    public void testTdigestSourceDocument() throws IOException {
        assertSourceDropsNullElements(tdigest(), IndexingStrategy.DOCUMENT);
    }

    public void testTdigestSourceBatch() throws IOException {
        assertSourceDropsNullElements(tdigest(), IndexingStrategy.BATCH);
    }

    public void testExponentialHistogramExistsDocument() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(exponentialHistogram(), IndexingStrategy.DOCUMENT);
    }

    public void testExponentialHistogramSourceModesAgreeDocument() throws IOException {
        assertSourceModesAgree(exponentialHistogram(), IndexingStrategy.DOCUMENT);
    }

    public void testExponentialHistogramSourceModesAgreeBatch() throws IOException {
        assertSourceModesAgree(exponentialHistogram(), IndexingStrategy.BATCH);
    }

    public void testExponentialHistogramExistsBatch() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(exponentialHistogram(), IndexingStrategy.BATCH);
    }

    public void testExponentialHistogramExistsCountDocument() throws IOException {
        assertExistsCountAgreesWithHits(exponentialHistogram(), IndexingStrategy.DOCUMENT);
    }

    public void testExponentialHistogramExistsCountBatch() throws IOException {
        assertExistsCountAgreesWithHits(exponentialHistogram(), IndexingStrategy.BATCH);
    }

    public void testExponentialHistogramSourceDocument() throws IOException {
        assertSourceDropsNullElements(exponentialHistogram(), IndexingStrategy.DOCUMENT);
    }

    public void testExponentialHistogramSourceBatch() throws IOException {
        assertSourceDropsNullElements(exponentialHistogram(), IndexingStrategy.BATCH);
    }
}
