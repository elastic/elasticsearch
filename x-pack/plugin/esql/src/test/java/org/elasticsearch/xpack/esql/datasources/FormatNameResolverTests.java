/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.Build;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

public class FormatNameResolverTests extends ESTestCase {

    public void testReaderJavaOverridesExtension() {
        assertEquals(FormatNameResolver.FORMAT_PARQUET, FormatNameResolver.resolve(Map.of("reader", "java"), "file.parquet"));
    }

    public void testReaderParquetRsOverridesExtension() {
        assumeTrue("parquet-rs reader alias is only registered in snapshot builds", Build.current().isSnapshot());
        assertEquals(FormatNameResolver.FORMAT_PARQUET_RS, FormatNameResolver.resolve(Map.of("reader", "parquet-rs"), "file.parquet"));
    }

    public void testReaderOverridesFormat() {
        assertEquals(
            FormatNameResolver.FORMAT_PARQUET,
            FormatNameResolver.resolve(Map.of("reader", "java", "format", "parquet-rs"), "file.parquet")
        );
    }

    public void testFormatFromConfig() {
        assertEquals("orc", FormatNameResolver.resolve(Map.of("format", "ORC"), "file.parquet"));
    }

    public void testFormatFromExtension() {
        assertEquals("orc", FormatNameResolver.resolve(null, "s3://bucket/data/file.orc"));
    }

    public void testExtensionWithQueryString() {
        assertEquals("orc", FormatNameResolver.resolve(null, "s3://bucket/file.orc?versionId=123"));
    }

    public void testExtensionWithFragment() {
        assertEquals("parquet", FormatNameResolver.resolve(null, "gs://bucket/file.parquet#frag"));
    }

    public void testFormatConfigOverridesExtension() {
        assertEquals("csv", FormatNameResolver.resolve(Map.of("format", "csv"), "file.orc"));
    }

    public void testEmptyFormatFallsBackToExtension() {
        assertEquals("orc", FormatNameResolver.resolve(Map.of("format", ""), "file.orc"));
    }

    public void testNoConfigNoExtension() {
        assertNull(FormatNameResolver.resolve(null, "file_without_extension"));
    }

    public void testNullEverything() {
        assertNull(FormatNameResolver.resolve(null, null));
    }

    public void testEmptyConfigNoPath() {
        assertNull(FormatNameResolver.resolve(Map.of(), null));
    }

    public void testUnknownReaderFallsThrough() {
        assertEquals("parquet", FormatNameResolver.resolve(Map.of("reader", "unknown"), "file.parquet"));
    }

    public void testReaderAliasToFormat() {
        assumeTrue("parquet-rs reader alias is only registered in snapshot builds", Build.current().isSnapshot());
        assertEquals(FormatNameResolver.FORMAT_PARQUET, FormatNameResolver.readerAliasToFormat(FormatNameResolver.READER_JAVA));
        assertEquals(FormatNameResolver.FORMAT_PARQUET_RS, FormatNameResolver.readerAliasToFormat(FormatNameResolver.READER_PARQUET_RS));
        assertNull(FormatNameResolver.readerAliasToFormat("unknown"));
    }
}
