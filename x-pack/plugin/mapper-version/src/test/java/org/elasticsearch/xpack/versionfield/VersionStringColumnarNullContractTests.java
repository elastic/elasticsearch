/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0"; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.AbstractColumnarNullContractTestCase;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class VersionStringColumnarNullContractTests extends AbstractColumnarNullContractTestCase {

    // The reconstructed _source drops a repeated value of a version field.
    private static final String DUPLICATES_NOT_PRESERVED = "https://github.com/elastic/elasticsearch/issues/166868";

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new VersionFieldPlugin(Settings.EMPTY));
    }

    private static Scenario version() {
        return scenario("version", () -> randomIntBetween(0, 9) + "." + randomIntBetween(0, 9) + "." + randomIntBetween(0, 9))
            .withoutIndex();
    }

    public void testVersionExistsRegular() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(version(), Path.REGULAR);
    }

    public void testVersionSourceRegular() throws IOException {
        assertSourceDropsNullElements(version(), Path.REGULAR);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = DUPLICATES_NOT_PRESERVED)
    public void testVersionRemainingValuesKeepTheirOrderRegular() throws IOException {
        assertSourceKeepsTheOrderOfTheRemainingValues(version(), Path.REGULAR);
    }
}
