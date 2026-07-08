/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FormatNameResolverTests extends ESTestCase {

    public void testReaderJavaOverridesExtension() {
        assertEquals(FormatNameResolver.FORMAT_PARQUET, FormatNameResolver.resolve(Map.of("reader", "java"), "file.parquet"));
    }

    public void testReaderParquetRsOverridesExtension() {
        assumeTrue("parquet-rs reader alias requires the parquet-rs feature flag", FormatNameResolver.parquetRsEnabled());
        assertEquals(FormatNameResolver.FORMAT_PARQUET_RS, FormatNameResolver.resolve(Map.of("reader", "parquet-rs"), "file.parquet"));
    }

    public void testReaderParquetRsUnreachableWhenDisabled() {
        assumeFalse("only when the parquet-rs feature flag is off", FormatNameResolver.parquetRsEnabled());
        // The public reader=parquet-rs selector is removed: the alias falls through to extension-based resolution.
        assertEquals(FormatNameResolver.FORMAT_PARQUET, FormatNameResolver.resolve(Map.of("reader", "parquet-rs"), "file.parquet"));
        assertNull(FormatNameResolver.readerAliasToFormat(FormatNameResolver.READER_PARQUET_RS));
        assertFalse(FormatNameResolver.supportedReaderAliases().contains(FormatNameResolver.READER_PARQUET_RS));
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

    public void testAutoFormatFallsBackToExtension() {
        assertEquals("orc", FormatNameResolver.resolve(Map.of("format", "auto"), "file.orc"));
    }

    public void testAutoFormatCaseInsensitiveFallsBackToExtension() {
        assertEquals("parquet", FormatNameResolver.resolve(Map.of("format", "AUTO"), "file.parquet"));
    }

    public void testAutoFormatWithNoExtensionIsNull() {
        assertNull(FormatNameResolver.resolve(Map.of("format", "auto"), "file_without_extension"));
    }

    public void testFormatSurroundingWhitespaceTrimmed() {
        assertEquals("orc", FormatNameResolver.resolve(Map.of("format", "  ORC  "), "file.parquet"));
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
        assumeTrue("parquet-rs reader alias requires the parquet-rs feature flag", FormatNameResolver.parquetRsEnabled());
        assertEquals(FormatNameResolver.FORMAT_PARQUET, FormatNameResolver.readerAliasToFormat(FormatNameResolver.READER_JAVA));
        assertEquals(FormatNameResolver.FORMAT_PARQUET_RS, FormatNameResolver.readerAliasToFormat(FormatNameResolver.READER_PARQUET_RS));
        assertNull(FormatNameResolver.readerAliasToFormat("unknown"));
    }

    // -- resolveFormatName: registry-routed, compound-extension aware (unlike resolve) --

    /** A compound compression extension resolves to the INNER format, not the codec suffix — the compressed-read fix. */
    public void testResolveFormatNameIsCompoundExtensionAware() {
        FormatReaderRegistry registry = csvRegistry();
        assertEquals("csv", FormatNameResolver.resolveFormatName(null, "hits.csv.gz", registry));
        assertEquals("csv", FormatNameResolver.resolveFormatName(Map.of(), "s3://b/hits.csv.gz", registry));
        assertEquals("csv", FormatNameResolver.resolveFormatName(null, "hits.csv", registry));
        // contrast: the non-compound-aware resolve() answers the codec suffix on the same input
        assertEquals("gz", FormatNameResolver.resolve(null, "hits.csv.gz"));
    }

    /** An explicit {@code format} override wins over the extension entirely (no registry extension lookup). */
    public void testResolveFormatNameConfigOverrideBeatsExtension() {
        FormatReaderRegistry registry = csvRegistry();
        assertEquals("csv", FormatNameResolver.resolveFormatName(Map.of("format", "csv"), "hits.parquet.gz", registry));
    }

    /** An extensionless, format-less strict resource fails loud at the registry rather than resolving null. */
    public void testResolveFormatNameThrowsOnExtensionless() {
        FormatReaderRegistry registry = csvRegistry();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FormatNameResolver.resolveFormatName(null, "no_extension", registry)
        );
        assertThat(e.getMessage(), containsString("without extension"));
    }

    /**
     * A registry with a single csv reader registered for {@code .csv}. A Mockito stub (not a real reader or a simplified
     * subclass) is used deliberately: {@code resolveFormatName} touches only {@link FormatReader#formatName()},
     * {@link FormatReader#fileExtensions()}, and {@link FormatReader#supportsWholeFileCompression()} (the last consulted
     * by the compound-extension wrapping), so a full {@link FormatReader} implementation ({@code metadata}/{@code read}/
     * {@code withConfigTrackingConsumedKeys}/{@code rowPositionStrategy}) would be far larger for zero added coverage.
     */
    private static FormatReaderRegistry csvRegistry() {
        FormatReader csv = mock(FormatReader.class);
        when(csv.formatName()).thenReturn("csv");
        when(csv.fileExtensions()).thenReturn(List.of(".csv"));
        when(csv.supportsWholeFileCompression()).thenReturn(true);
        DecompressionCodecRegistry codecs = new DecompressionCodecRegistry();
        codecs.register(new DecompressionCodec() {
            @Override
            public String name() {
                return "gzip";
            }

            @Override
            public List<String> extensions() {
                return List.of(".gz");
            }

            @Override
            public InputStream decompress(InputStream raw) {
                return raw;
            }
        });
        FormatReaderRegistry registry = new FormatReaderRegistry(codecs);
        registry.registerLazy("csv", (s, bf) -> csv, Settings.EMPTY, null);
        registry.registerExtension(".csv", "csv");
        return registry;
    }
}
