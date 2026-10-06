/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.DefinitionVersion;

import java.util.Map;

public class SourceScopeTests extends ESTestCase {

    /** Every component must discriminate, or two datasets share one address. */
    public void testEachComponentDiscriminates() {
        SourceScope base = SourceScope.of("csv", "s3:bucket-a|csv:sep=,", "7");
        assertNotEquals(base, SourceScope.of("ndjson", "s3:bucket-a|csv:sep=,", "7"));
        assertNotEquals(base, SourceScope.of("csv", "s3:bucket-b|csv:sep=,", "7"));
        assertNotEquals(base, SourceScope.of("csv", "s3:bucket-a|csv:sep=,", "8"));
        assertEquals(base, SourceScope.of("csv", "s3:bucket-a|csv:sep=,", "7"));
    }

    /**
     * The participants and the definition version are folded into one pair of lanes, so the fold must not let one
     * borrow the other's characters. Length prefixing is what stops that; without it these two encode identically.
     */
    public void testFoldDoesNotLetComponentsBleedIntoEachOther() {
        assertNotEquals(SourceScope.of("csv", "ab", "c"), SourceScope.of("csv", "a", "bc"));
    }

    /** A null participant identity must not encode as an empty one: absent and empty are different states. */
    public void testNullIsNotEmpty() {
        assertNotEquals(SourceScope.of("csv", null, "7"), SourceScope.of("csv", "", "7"));
    }

    public void testDefinitionVersionOfReadsTheConfigOrAnswersEmpty() {
        assertEquals("7", SourceScope.definitionVersionOf(Map.of(DefinitionVersion.CONFIG_KEY, "7")));
        assertEquals("", SourceScope.definitionVersionOf(Map.of()));
        assertEquals("", SourceScope.definitionVersionOf(null));
        // A non-String under the key is a query with no usable version, not a reason to render one.
        assertEquals("", SourceScope.definitionVersionOf(Map.of(DefinitionVersion.CONFIG_KEY, 7)));
    }

    /**
     * The scope is shared by reference across the keys of one dataset - that sharing is the memory argument, and
     * equality must be cheap for it to pay off. Pinned as a reference check so a later change to a deep-compared
     * component (a config map, say) turns this red.
     */
    public void testKeysOfOneDatasetShareOneScopeInstance() {
        SourceScope scope = SourceScope.of("parquet", "s3:bucket-a|parquet", "3");
        FileSchemaKey first = new FileSchemaKey(scope, "s3://bucket-a/part-0.parquet", 1000L);
        FileSchemaKey second = new FileSchemaKey(scope, "s3://bucket-a/part-1.parquet", 2000L);
        assertSame(first.scope(), second.scope());
        assertNotEquals(first, second);
    }
}
