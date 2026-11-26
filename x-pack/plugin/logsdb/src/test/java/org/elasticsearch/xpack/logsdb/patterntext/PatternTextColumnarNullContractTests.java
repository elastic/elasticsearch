/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0"; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.AbstractColumnarNullContractTestCase;
import org.elasticsearch.index.mapper.IndexingStrategy;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.logsdb.LogsDBPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class PatternTextColumnarNullContractTests extends AbstractColumnarNullContractTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new LogsDBPlugin(Settings.EMPTY));
    }

    private static Scenario patternText() {
        return scenario("pattern_text", () -> randomAlphaOfLengthBetween(1, 8)).withoutIndex()
            .asSingleValued()
            .batchedGroups((configuration, shape) -> true);
    }

    public void testExistsMatchesOnlyDocumentsWithValueDocument() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(patternText(), IndexingStrategy.DOCUMENT);
    }

    public void testExistsMatchesOnlyDocumentsWithValueBatch() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(patternText(), IndexingStrategy.BATCH);
    }

    public void testExistsCountAgreesWithHitsDocument() throws IOException {
        assertExistsCountAgreesWithHits(patternText(), IndexingStrategy.DOCUMENT);
    }

    public void testExistsCountAgreesWithHitsBatch() throws IOException {
        assertExistsCountAgreesWithHits(patternText(), IndexingStrategy.BATCH);
    }

    public void testSourceDropsNullElementsDocument() throws IOException {
        assertSourceDropsNullElements(patternText(), IndexingStrategy.DOCUMENT);
    }

    public void testSourceDropsNullElementsBatch() throws IOException {
        assertSourceDropsNullElements(patternText(), IndexingStrategy.BATCH);
    }
}
