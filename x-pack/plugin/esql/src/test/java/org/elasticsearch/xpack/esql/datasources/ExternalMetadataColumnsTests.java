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
     * A file holds no document identity, no document version and no stored source, so a dataset does not
     * answer those three names at all: they are absent from the bindable set and produce no per-file value.
     */
    public void testIdentityVersionAndSourceAreNotBindable() {
        for (String name : List.of(ExternalMetadataColumns.ID, ExternalMetadataColumns.VERSION, ExternalMetadataColumns.SOURCE)) {
            assertFalse(name + " must not be bindable on a dataset", ExternalMetadataColumns.STANDARD_NAMES.contains(name));
            assertFalse(name + " must not be a per-file constant", ExternalMetadataColumns.PER_FILE_CONSTANT_NAMES.contains(name));
            assertFalse(
                name + " must produce no per-file value",
                ExternalMetadataColumns.extractPerFileConstants("events").containsKey(name)
            );
        }
    }

    /**
     * The three unbindable names stay reserved: a dataset cannot answer them, but a layout must not be able
     * to claim them either, or a file column called {@code _id} would shadow the metadata namespace.
     */
    public void testIdentityVersionAndSourceStayReserved() {
        assertTrue(ExternalMetadataColumns.RESERVED_NAMES.contains(ExternalMetadataColumns.ID));
        assertTrue(ExternalMetadataColumns.RESERVED_NAMES.contains(ExternalMetadataColumns.VERSION));
        assertTrue(ExternalMetadataColumns.RESERVED_NAMES.contains(ExternalMetadataColumns.SOURCE));
    }

    /**
     * Drift tripwire: every name the analyzer knows ({@code MetadataAttribute.ATTRIBUTES_MAP}) must be
     * reserved on an external relation. A new standard metadata name added to the analyzer registry
     * without a matching entry here would let a dataset layout claim it — this assertion turns that
     * into a test failure naming the missing entry.
     */
    public void testReservedNamesCoverEveryBindableMetadataName() {
        for (String name : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            assertTrue(
                "metadata name [" + name + "] is known to the analyzer but missing from RESERVED_NAMES",
                ExternalMetadataColumns.RESERVED_NAMES.contains(name)
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
