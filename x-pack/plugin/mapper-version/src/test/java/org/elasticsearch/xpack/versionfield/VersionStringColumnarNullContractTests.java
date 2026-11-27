/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.AbstractColumnarNullContractTestCase;
import org.elasticsearch.index.mapper.IndexingStrategy;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

public class VersionStringColumnarNullContractTests extends AbstractColumnarNullContractTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new VersionFieldPlugin(Settings.EMPTY));
    }

    private static Scenario version() {
        return scenario("version", () -> randomIntBetween(0, 9) + "." + randomIntBetween(0, 9) + "." + randomIntBetween(0, 9))
            .withDefaultIndexSetting()
            .requireBatchFor(EnumSet.allOf(Shape.class));
    }

    public void testVersionExistsDocument() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(version(), IndexingStrategy.DOCUMENT);
    }

    public void testVersionSourceModesAgreeDocument() throws IOException {
        assertSourceModesAgree(version(), IndexingStrategy.DOCUMENT);
    }

    public void testVersionSourceModesAgreeBatch() throws IOException {
        assertSourceModesAgree(version(), IndexingStrategy.BATCH);
    }

    public void testVersionExistsBatch() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(version(), IndexingStrategy.BATCH);
    }

    public void testVersionSourceDocument() throws IOException {
        assertSourceDropsNullElements(version(), IndexingStrategy.DOCUMENT);
    }

    public void testVersionSourceBatch() throws IOException {
        assertSourceDropsNullElements(version(), IndexingStrategy.BATCH);
    }

}
