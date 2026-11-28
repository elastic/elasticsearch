/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.index.mapper.AbstractColumnarNullContractTestCase;
import org.elasticsearch.index.mapper.IndexingStrategy;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.wildcard.Wildcard;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;

public class WildcardColumnarNullContractTests extends AbstractColumnarNullContractTestCase {

    private static final String NULL_ELEMENTS_KEPT = "https://github.com/elastic/elasticsearch/issues/165909";
    private static final String NULL_ONLY_MATCHES_EXISTS = "https://github.com/elastic/elasticsearch/issues/165909";

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singleton(new Wildcard());
    }

    private static Scenario wildcard() {
        return scenario("wildcard", () -> randomAlphaOfLengthBetween(1, 8)).withDefaultIndexSetting()
            .requireBatchFor(EnumSet.allOf(Shape.class));
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ONLY_MATCHES_EXISTS)
    public void testWildcardExistsDocument() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(wildcard(), IndexingStrategy.DOCUMENT);
    }

    public void testWildcardSourceModesAgreeDocument() throws IOException {
        assertSourceModesAgree(wildcard(), IndexingStrategy.DOCUMENT);
    }

    public void testWildcardSourceModesAgreeBatch() throws IOException {
        assertSourceModesAgree(wildcard(), IndexingStrategy.BATCH);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ONLY_MATCHES_EXISTS)
    public void testWildcardExistsBatch() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(wildcard(), IndexingStrategy.BATCH);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testWildcardSourceDocument() throws IOException {
        assertSourceDropsNullElements(wildcard(), IndexingStrategy.DOCUMENT);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testWildcardSourceBatch() throws IOException {
        assertSourceDropsNullElements(wildcard(), IndexingStrategy.BATCH);
    }

    public void testWildcardRemainingValuesKeepTheirOrderDocument() throws IOException {
        assertSourceKeepsTheOrderOfTheRemainingValues(wildcard(), IndexingStrategy.DOCUMENT);
    }

    public void testWildcardRemainingValuesKeepTheirOrderBatch() throws IOException {
        assertSourceKeepsTheOrderOfTheRemainingValues(wildcard(), IndexingStrategy.BATCH);
    }

    public void testWildcardNullValueReplacesNullDocument() throws IOException {
        final Scenario scenario = wildcard().withNullValue("NA");
        assertExistsMatchesOnlyDocumentsWithValue(scenario, IndexingStrategy.DOCUMENT);
        assertSourceDropsNullElements(scenario, IndexingStrategy.DOCUMENT);
        assertSourceKeepsTheOrderOfTheRemainingValues(scenario, IndexingStrategy.DOCUMENT);
    }

    public void testWildcardNullValueReplacesNullBatch() throws IOException {
        final Scenario scenario = wildcard().withNullValue("NA");
        assertExistsMatchesOnlyDocumentsWithValue(scenario, IndexingStrategy.BATCH);
        assertSourceDropsNullElements(scenario, IndexingStrategy.BATCH);
        assertSourceKeepsTheOrderOfTheRemainingValues(scenario, IndexingStrategy.BATCH);
    }
}
