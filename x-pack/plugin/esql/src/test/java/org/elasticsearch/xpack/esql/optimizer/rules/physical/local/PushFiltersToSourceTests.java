/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

public class PushFiltersToSourceTests extends ESTestCase {

    // --- resolveFormatName tests ---

    public void testResolveFormatNameFromConfig() {
        assertEquals("orc", PushFiltersToSource.resolveFormatName(Map.of("format", "ORC"), "file.parquet"));
    }

    public void testResolveFormatNameFromExtension() {
        assertEquals("orc", PushFiltersToSource.resolveFormatName(null, "s3://bucket/data/file.orc"));
    }

    public void testResolveFormatNameFromExtensionWithQueryString() {
        assertEquals("orc", PushFiltersToSource.resolveFormatName(null, "s3://bucket/file.orc?versionId=123"));
    }

    public void testResolveFormatNameFromExtensionWithFragment() {
        assertEquals("parquet", PushFiltersToSource.resolveFormatName(null, "gs://bucket/file.parquet#frag"));
    }

    public void testResolveFormatNameConfigOverridesExtension() {
        assertEquals("csv", PushFiltersToSource.resolveFormatName(Map.of("format", "csv"), "file.orc"));
    }

    public void testResolveFormatNameEmptyConfigFallsBackToExtension() {
        assertEquals("orc", PushFiltersToSource.resolveFormatName(Map.of("format", ""), "file.orc"));
    }

    public void testResolveFormatNameNoConfigNoExtension() {
        assertNull(PushFiltersToSource.resolveFormatName(null, "file_without_extension"));
    }

    public void testResolveFormatNameNullEverything() {
        assertNull(PushFiltersToSource.resolveFormatName(null, null));
    }

    public void testResolveFormatNameEmptyConfigNoPath() {
        assertNull(PushFiltersToSource.resolveFormatName(Map.of(), null));
    }
}
