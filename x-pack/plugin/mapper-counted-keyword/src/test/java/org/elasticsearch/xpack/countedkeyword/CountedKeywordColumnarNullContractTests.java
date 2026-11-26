/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.countedkeyword;

import org.elasticsearch.index.mapper.AbstractColumnarNullContractTestCase;
import org.elasticsearch.index.mapper.IndexingStrategy;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class CountedKeywordColumnarNullContractTests extends AbstractColumnarNullContractTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new CountedKeywordMapperPlugin());
    }

    // NOTE: counted_keyword does not implement batch indexing today, so the scenario lists no batched shape and the BATCH tests are not
    // required to execute as a batch. They exercise batch indexing as soon as the mapper accepts a column.
    static Scenario countedKeyword() {
        return scenario("counted_keyword", () -> randomAlphaOfLengthBetween(1, 8)).withoutIndex();
    }

    public void testCountedKeywordExistsDocument() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(countedKeyword(), IndexingStrategy.DOCUMENT);
    }

    public void testCountedKeywordExistsBatch() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(countedKeyword(), IndexingStrategy.BATCH);
    }

    public void testCountedKeywordExistsCountDocument() throws IOException {
        assertExistsCountAgreesWithHits(countedKeyword(), IndexingStrategy.DOCUMENT);
    }

    public void testCountedKeywordExistsCountBatch() throws IOException {
        assertExistsCountAgreesWithHits(countedKeyword(), IndexingStrategy.BATCH);
    }

    public void testCountedKeywordSourceDocument() throws IOException {
        assertSourceDropsNullElements(countedKeyword(), IndexingStrategy.DOCUMENT);
    }

    public void testCountedKeywordSourceBatch() throws IOException {
        assertSourceDropsNullElements(countedKeyword(), IndexingStrategy.BATCH);
    }
}
