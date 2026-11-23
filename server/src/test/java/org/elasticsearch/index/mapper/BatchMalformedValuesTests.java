/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.index.IndexVersionUtils;

/**
 * Tests the index-layout gating of {@link BatchMalformedValues}. The encoded sidecar columns are covered end to end by the columnar
 * compatibility tests of the mappers that use it, such as {@link IpFieldMapperColumnarCompatibilityTests}, which compare them with
 * what the row path writes.
 */
public class BatchMalformedValuesTests extends ESTestCase {

    private static IndexSettings indexSettings(IndexVersion version) {
        return IndexSettingsModule.newIndexSettings("index", settings(version).build());
    }

    public void testNotSupportedWithoutIgnoreMalformed() {
        final IndexSettings indexSettings = indexSettings(IndexVersion.current());
        assertFalse(BatchMalformedValues.supported(indexSettings, false, true));
        assertFalse(BatchMalformedValues.supported(indexSettings, false, false));
    }

    /** When the row path does not store malformed values there is no layout to write, so every index version is supported. */
    public void testSupportedWhenNothingIsStored() {
        final IndexVersion version = IndexVersionUtils.randomVersion();
        assertTrue(BatchMalformedValues.supported(indexSettings(version), true, false));
    }

    public void testStoringRequiresSeparateCountLayout() {
        assertTrue(BatchMalformedValues.supported(indexSettings(IndexVersion.current()), true, true));
        final IndexVersion newer = IndexVersionUtils.randomVersionBetween(
            IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES,
            IndexVersion.current()
        );
        assertTrue(BatchMalformedValues.supported(indexSettings(newer), true, true));
        final IndexVersion older = IndexVersionUtils.randomVersionBetween(
            null,
            IndexVersionUtils.getPreviousVersion(IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES)
        );
        assertFalse(BatchMalformedValues.supported(indexSettings(older), true, true));
    }
}
