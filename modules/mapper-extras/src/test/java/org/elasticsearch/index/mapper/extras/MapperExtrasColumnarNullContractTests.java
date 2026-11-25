/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.index.mapper.AbstractColumnarNullContractTestCase;
import org.elasticsearch.index.mapper.IndexingStrategy;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class MapperExtrasColumnarNullContractTests extends AbstractColumnarNullContractTestCase {

    private static final String NULL_ELEMENTS_KEPT = "https://github.com/elastic/elasticsearch/issues/165909";
    private static final String NULL_ONLY_MATCHES_EXISTS = "https://github.com/elastic/elasticsearch/issues/165909";
    private static final String EXISTS_COUNT_DISAGREES = "https://github.com/elastic/elasticsearch/issues/165521";

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }

    private static Scenario matchOnlyText() {
        return scenario("match_only_text", () -> randomAlphaOfLengthBetween(1, 8)).withoutIndex()
            .batchedGroups((configuration, shape) -> true);
    }

    private static Scenario scaledFloat() {
        return scenario("scaled_float", () -> randomIntBetween(0, 1000) / 4.0).withParameters(Map.of("scaling_factor", 100));
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ONLY_MATCHES_EXISTS)
    public void testMatchOnlyTextExistsDocument() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(matchOnlyText(), IndexingStrategy.DOCUMENT);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ONLY_MATCHES_EXISTS)
    public void testMatchOnlyTextExistsBatch() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(matchOnlyText(), IndexingStrategy.BATCH);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = EXISTS_COUNT_DISAGREES)
    public void testMatchOnlyTextExistsCountDocument() throws IOException {
        assertExistsCountAgreesWithHits(matchOnlyText(), IndexingStrategy.DOCUMENT);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = EXISTS_COUNT_DISAGREES)
    public void testMatchOnlyTextExistsCountBatch() throws IOException {
        assertExistsCountAgreesWithHits(matchOnlyText(), IndexingStrategy.BATCH);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testMatchOnlyTextSourceDocument() throws IOException {
        assertSourceDropsNullElements(matchOnlyText(), IndexingStrategy.DOCUMENT);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testMatchOnlyTextSourceBatch() throws IOException {
        assertSourceDropsNullElements(matchOnlyText(), IndexingStrategy.BATCH);
    }

    public void testMatchOnlyTextRemainingValuesKeepTheirOrderDocument() throws IOException {
        assertSourceKeepsTheOrderOfTheRemainingValues(matchOnlyText(), IndexingStrategy.DOCUMENT);
    }

    public void testMatchOnlyTextRemainingValuesKeepTheirOrderBatch() throws IOException {
        assertSourceKeepsTheOrderOfTheRemainingValues(matchOnlyText(), IndexingStrategy.BATCH);
    }

    public void testScaledFloatExistsMatchesOnlyDocumentsWithValueDocument() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(scaledFloat(), IndexingStrategy.DOCUMENT);
    }

    public void testScaledFloatExistsCountAgreesWithHitsDocument() throws IOException {
        assertExistsCountAgreesWithHits(scaledFloat(), IndexingStrategy.DOCUMENT);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testScaledFloatSourceDocument() throws IOException {
        assertSourceDropsNullElements(scaledFloat(), IndexingStrategy.DOCUMENT);
    }

    public void testScaledFloatNullValueReplacesNullDocument() throws IOException {
        final Scenario scenario = scaledFloat().withNullValue(0.5);
        assertExistsMatchesOnlyDocumentsWithValue(scenario, IndexingStrategy.DOCUMENT);
        assertSourceDropsNullElements(scenario, IndexingStrategy.DOCUMENT);
    }

    public void testScaledFloatRemainingValuesKeepTheirOrderDocument() throws IOException {
        assertSourceKeepsTheOrderOfTheRemainingValues(scaledFloat(), IndexingStrategy.DOCUMENT);
    }
}
