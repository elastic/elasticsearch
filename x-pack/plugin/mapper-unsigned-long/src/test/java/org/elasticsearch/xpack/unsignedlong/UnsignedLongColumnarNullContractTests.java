/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0"; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.index.mapper.AbstractColumnarNullContractTestCase;
import org.elasticsearch.index.mapper.IndexingStrategy;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class UnsignedLongColumnarNullContractTests extends AbstractColumnarNullContractTestCase {

    private static final String NULL_ELEMENTS_KEPT = "https://github.com/elastic/elasticsearch/issues/165909";
    private static final String NULL_ONLY_MATCHES_EXISTS = "https://github.com/elastic/elasticsearch/issues/165909";

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new UnsignedLongMapperPlugin());
    }

    private static Scenario unsignedLong() {
        return scenario("unsigned_long", () -> randomLongBetween(0, Long.MAX_VALUE));
    }

    public void testUnsignedLongExistsDocument() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(unsignedLong(), IndexingStrategy.DOCUMENT);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testUnsignedLongSourceDocument() throws IOException {
        assertSourceDropsNullElements(unsignedLong(), IndexingStrategy.DOCUMENT);
    }

    public void testUnsignedLongRemainingValuesKeepTheirOrderDocument() throws IOException {
        assertSourceKeepsTheOrderOfTheRemainingValues(unsignedLong(), IndexingStrategy.DOCUMENT);
    }

    public void testUnsignedLongNullValueReplacesNullDocument() throws IOException {
        final Scenario scenario = unsignedLong().withNullValue(9);
        assertExistsMatchesOnlyDocumentsWithValue(scenario, IndexingStrategy.DOCUMENT);
        assertSourceDropsNullElements(scenario, IndexingStrategy.DOCUMENT);
        assertSourceKeepsTheOrderOfTheRemainingValues(scenario, IndexingStrategy.DOCUMENT);
    }
}
