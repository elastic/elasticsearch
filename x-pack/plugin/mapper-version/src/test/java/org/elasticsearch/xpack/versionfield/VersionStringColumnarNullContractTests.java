/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0"; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.AbstractColumnarNullContractTestCase;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class VersionStringColumnarNullContractTests extends AbstractColumnarNullContractTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new VersionFieldPlugin(Settings.EMPTY));
    }

    private static Scenario version() {
        return scenario("version", () -> randomIntBetween(0, 9) + "." + randomIntBetween(0, 9) + "." + randomIntBetween(0, 9))
            .withoutIndex()
            .batchedGroups((configuration, shape) -> true);
    }

    public void testVersionExistsRegular() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(version(), Path.REGULAR);
    }

    public void testVersionExistsBatch() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(version(), Path.BATCH);
    }

    public void testVersionSourceRegular() throws IOException {
        assertSourceDropsNullElements(version(), Path.REGULAR);
    }

    public void testVersionSourceBatch() throws IOException {
        assertSourceDropsNullElements(version(), Path.BATCH);
    }

}
