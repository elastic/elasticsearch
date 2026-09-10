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

import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link ExternalMetadataColumns}, the per-file constant synthesizer for the
 * standard metadata names.
 */
public class ExternalMetadataColumnsTests extends ESTestCase {

    public void testIndexCarriesDatasetName() {
        Map<String, Object> constants = ExternalMetadataColumns.extractPerFileConstants("events");
        assertEquals(new BytesRef("events"), constants.get(ExternalMetadataColumns.INDEX));
    }

    /**
     * The null arm of {@code _index} is defensive-only today — {@code FROM <dataset>} always binds
     * a name and no other query surface binds standard metadata — but the contract is load-bearing
     * for any future bare-URI grammar: with no dataset name there is nothing honest to report, so
     * {@code _index} must be SQL NULL, never an invented identifier.
     */
    public void testIndexIsNullWithoutDatasetName() {
        Map<String, Object> constants = ExternalMetadataColumns.extractPerFileConstants(null);
        assertTrue(constants.containsKey(ExternalMetadataColumns.INDEX));
        assertNull(constants.get(ExternalMetadataColumns.INDEX));
    }

    /**
     * A file holds no document identity, no document version and no stored source. All three names still bind —
     * so a query naming them is answered rather than rejected — and every row is SQL NULL, which is what the
     * dataset actually knows. A value composed at the reader would be an invention.
     */
    public void testIdentityVersionAndSourceAnswerNull() {
        Map<String, Object> constants = ExternalMetadataColumns.extractPerFileConstants("events");
        for (String name : List.of(ExternalMetadataColumns.ID, ExternalMetadataColumns.VERSION, ExternalMetadataColumns.SOURCE)) {
            assertTrue("[" + name + "] must be bindable", ExternalMetadataColumns.STANDARD_NAMES.contains(name));
            assertTrue("[" + name + "] must carry a per-file entry", constants.containsKey(name));
            assertNull("[" + name + "] must answer SQL NULL", constants.get(name));
        }
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

    /** Every bindable standard name is one the analyzer knows, and one the per-file synthesizer answers. */
    public void testStandardNamesAreBindableAndAnswered() {
        Map<String, Object> constants = ExternalMetadataColumns.extractPerFileConstants("events");
        for (String name : ExternalMetadataColumns.STANDARD_NAMES) {
            assertTrue("standard name [" + name + "] is not known to the analyzer", MetadataAttribute.ATTRIBUTES_MAP.containsKey(name));
            assertTrue("standard name [" + name + "] has no per-file value", constants.containsKey(name));
        }
    }
}
