/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;

import java.util.Map;

/**
 * Unit tests for {@link ExternalMetadataColumns}, the per-file constant synthesizer for the
 * standard metadata names.
 */
public class ExternalMetadataColumnsTests extends ESTestCase {

    public void testIndexCarriesDatasetName() {
        Map<String, Object> constants = ExternalMetadataColumns.extractPerFileConstants("events", 1700000000000L);
        assertEquals(new BytesRef("events"), constants.get(ExternalMetadataColumns.INDEX));
        assertEquals(1700000000000L, constants.get(ExternalMetadataColumns.VERSION));
    }

    /**
     * The null arm of {@code _index} is defensive-only today — {@code FROM <dataset>} always binds
     * a name and no other query surface binds standard metadata — but the contract is load-bearing
     * for any future bare-URI grammar: with no dataset name there is nothing honest to report, so
     * {@code _index} must be SQL NULL, never an invented identifier.
     */
    public void testIndexIsNullWithoutDatasetName() {
        Map<String, Object> constants = ExternalMetadataColumns.extractPerFileConstants(null, 1700000000000L);
        assertTrue(constants.containsKey(ExternalMetadataColumns.INDEX));
        assertNull(constants.get(ExternalMetadataColumns.INDEX));
    }

    /** Zero is the {@code FileList} convention for a missing mtime: {@code _version} renders null, not epoch zero. */
    public void testVersionTreatsZeroMtimeAsUnknown() {
        Map<String, Object> constants = ExternalMetadataColumns.extractPerFileConstants("events", 0L);
        assertTrue(constants.containsKey(ExternalMetadataColumns.VERSION));
        assertNull(constants.get(ExternalMetadataColumns.VERSION));
    }

    /**
     * Drift tripwire: every name the analyzer can bind on an external relation
     * ({@code MetadataAttribute.ATTRIBUTES_MAP}) must be in the dedicated set. A new standard
     * metadata name added to the analyzer registry without a matching entry here would bind on
     * external datasets, escape the partition-rename guard, and then fail at runtime in the
     * producer pipeline — this assertion turns that into a compile-adjacent test failure naming
     * the missing entry.
     */
    public void testStandardNamesCoverEveryBindableMetadataName() {
        for (String name : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            assertTrue(
                "metadata name [" + name + "] is bindable on external relations but missing from STANDARD_NAMES",
                ExternalMetadataColumns.STANDARD_NAMES.contains(name)
            );
        }
    }
}
