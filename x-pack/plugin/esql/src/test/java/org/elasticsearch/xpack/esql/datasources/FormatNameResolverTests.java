/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FormatNameResolverTests extends ESTestCase {

    public void testReaderJavaOverridesExtension() {
        assertEquals(FormatNameResolver.FORMAT_PARQUET, FormatNameResolver.resolve(Map.of("reader", "java"), "file.parquet"));
    }

    public void testReaderOverridesFormat() {
        assertEquals(
            FormatNameResolver.FORMAT_PARQUET,
            FormatNameResolver.resolve(Map.of("reader", "java", "format", "orc"), "file.parquet")
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

    /**
     * Regression test for elastic/esql-planning#1854: a dotted query value in a presigned URL caused the
     * last-dot scan to land inside the query string, yielding the wrong extension ("2" instead of "csv").
     */
    public void testExtensionWithDottedQueryString() {
        assertEquals("csv", FormatNameResolver.resolve(null, "https://host/data.csv?v=1.2"));
        assertEquals("csv", FormatNameResolver.resolve(null, "http://host/data.csv?X-Amz-Signature=a.b"));
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
        assertEquals(FormatNameResolver.FORMAT_PARQUET, FormatNameResolver.readerAliasToFormat(FormatNameResolver.READER_JAVA));
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

    /**
     * Production parquet registers {@code .parq} as an alias. Registry-routed resolution must yield
     * {@code parquet}; {@link FormatNameResolver#resolve} still last-dots to {@code parq}, which is why
     * planning must not use it.
     */
    public void testParqAliasIsParquetInRegistryAndLastDotInResolve() {
        FormatReaderRegistry registry = csvAndParquetRegistry();
        assertEquals("parquet", FormatNameResolver.resolveFormatName(null, "f.parq", registry));
        assertEquals("parquet", FormatNameResolver.datasetFormat(null, "s3://b/*.parq", registry));
        assertEquals("parquet", FormatNameResolver.datasetFormat(null, "s3://b/*.{parquet,parq}", registry));
        assertEquals("parq", FormatNameResolver.resolve(null, "f.parq"));
    }

    /**
     * {@link FormatNameResolver#resolveFormatName} wraps and vetoes parquet+gzip.
     * Identity lookup strips the codec and still answers {@code parquet}.
     */
    public void testResolveFormatNameForIdentitySkipsCompressionVeto() {
        FormatReaderRegistry registry = csvAndParquetRegistry();
        assertEquals("parquet", FormatNameResolver.resolveFormatNameForIdentity(null, "f.parquet.gz", registry));
        expectThrows(IllegalArgumentException.class, () -> FormatNameResolver.resolveFormatName(null, "f.parquet.gz", registry));
        assertEquals("csv", FormatNameResolver.resolveFormatNameForIdentity(null, "hits.csv.gz", registry));
        assertEquals("csv", FormatNameResolver.resolveFormatNameForIdentity(Map.of("format", "csv"), "file.log", registry));
    }

    /** An explicit {@code format} override wins over the extension entirely (no registry extension lookup). */
    public void testResolveFormatNameConfigOverrideBeatsExtension() {
        FormatReaderRegistry registry = csvRegistry();
        assertEquals("csv", FormatNameResolver.resolveFormatName(Map.of("format", "csv"), "hits.parquet.gz", registry));
    }

    /**
     * Regression test for the compressed-read-under-explicit-format fix: an explicit {@code format} override
     * must still compose with the resource's outer compression suffix — the reader
     * {@link FormatNameResolver#resolveReader} returns (not just the name
     * {@link FormatNameResolver#resolveFormatName} reads back) must be wrapped in a
     * {@link CompressionDelegatingFormatReader} so the returned reader actually decompresses at read time,
     * rather than resolving the plain reader over compressed bytes.
     */
    public void testResolveReaderConfigOverrideComposesWithCompressionSuffix() {
        FormatReaderRegistry registry = csvRegistry();
        FormatReader reader = FormatNameResolver.resolveReader(Map.of("format", "csv"), "hits.csv.gz", registry);
        assertEquals("csv", reader.formatName());
        assertTrue(
            "explicit format over a compressed resource must resolve a CompressionDelegatingFormatReader",
            reader instanceof CompressionDelegatingFormatReader
        );
    }

    /** An explicit {@code format} override over an uncompressed resource resolves the plain reader, unwrapped. */
    public void testResolveReaderConfigOverrideWithoutCompressionSuffixIsUnwrapped() {
        FormatReaderRegistry registry = csvRegistry();
        FormatReader reader = FormatNameResolver.resolveReader(Map.of("format", "csv"), "hits.csv", registry);
        assertFalse(reader instanceof CompressionDelegatingFormatReader);
    }

    /** An extensionless, format-less strict resource fails loud at the registry rather than resolving null. */
    public void testResolveFormatNameThrowsOnExtensionless() {
        FormatReaderRegistry registry = csvRegistry();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FormatNameResolver.resolveFormatName(null, "no_extension", registry)
        );
        // The extensionless case now shares the one unreadable-object message, so it names the object, the
        // reason, and the [format] remedy rather than a bare "without extension" phrase.
        assertThat(e.getMessage(), containsString("Cannot determine how to read"));
        assertThat(e.getMessage(), containsString("no file extension"));
        assertThat(e.getMessage(), containsString("[format]"));
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

    // --- extractCleanExtension ---

    public void testExtractCleanExtensionSimple() {
        assertEquals("csv", FormatNameResolver.extractCleanExtension("file.csv"));
    }

    public void testExtractCleanExtensionUpperCase() {
        assertEquals("csv", FormatNameResolver.extractCleanExtension("FILE.CSV"));
    }

    public void testExtractCleanExtensionQuestionMarkBeforeDot() {
        // ? before the last dot is a glob metacharacter — must not hide the extension
        assertEquals("csv", FormatNameResolver.extractCleanExtension("day?.csv"));
    }

    public void testExtractCleanExtensionQuestionMarkAfterDot() {
        // ? after the last dot (e.g. S3 versionId) must be stripped
        assertEquals("csv", FormatNameResolver.extractCleanExtension("file.csv?versionId=abc"));
    }

    public void testExtractCleanExtensionFragmentBeforeDot() {
        // # before the last dot (legal in S3 key names) must not hide the extension
        assertEquals("csv", FormatNameResolver.extractCleanExtension("report#1.csv"));
    }

    public void testExtractCleanExtensionFragmentAfterDot() {
        assertEquals("csv", FormatNameResolver.extractCleanExtension("file.csv#frag"));
    }

    public void testExtractCleanExtensionFullPath() {
        // Works on a full object-store path, not just the filename component
        assertEquals("csv", FormatNameResolver.extractCleanExtension("s3://bucket/logs/file.csv"));
    }

    public void testExtractCleanExtensionHttpUrlStripsQuery() {
        // For http/https, StoragePath strips the query string from the path before the last-dot scan,
        // so a dot inside the query (e.g. ?v=1.2) does not win over the real extension.
        assertEquals("csv", FormatNameResolver.extractCleanExtension("https://host/data.csv?v=1.2"));
        assertEquals("csv", FormatNameResolver.extractCleanExtension("http://host/data.csv?X-Amz-Signature=a.b"));
    }

    public void testExtractCleanExtensionNoDot() {
        assertNull(FormatNameResolver.extractCleanExtension("nodotfile"));
    }

    public void testExtractCleanExtensionTrailingDot() {
        assertNull(FormatNameResolver.extractCleanExtension("file."));
    }

    public void testExtractCleanExtensionNull() {
        assertNull(FormatNameResolver.extractCleanExtension(null));
    }

    public void testExtractCleanExtensionExtStrippedToEmpty() {
        // Extension that is entirely a query string (e.g. ".?v=1") should return null
        assertNull(FormatNameResolver.extractCleanExtension("file.?v=1"));
    }

    // -- datasetFormat: pattern implies exactly one format, or explicit reader/format --

    public void testDatasetFormatExplicitFormatWins() {
        FormatReaderRegistry registry = csvAndParquetRegistry();
        assertEquals("csv", FormatNameResolver.datasetFormat(Map.of("format", "csv"), "s3://b/hits/*", registry));
        assertEquals("csv", FormatNameResolver.datasetFormat(Map.of("format", "csv"), "s3://b/a.parquet,s3://b/b.csv", registry));
    }

    public void testDatasetFormatReaderAliasWins() {
        FormatReaderRegistry registry = csvAndParquetRegistry();
        assertEquals(
            FormatNameResolver.FORMAT_PARQUET,
            FormatNameResolver.datasetFormat(Map.of("reader", "java"), "s3://b/hits/*", registry)
        );
    }

    public void testDatasetFormatHomogeneousParquetGlob() {
        FormatReaderRegistry registry = csvAndParquetRegistry();
        assertEquals("parquet", FormatNameResolver.datasetFormat(null, "s3://b/*.parquet", registry));
        assertEquals("parquet", FormatNameResolver.datasetFormat(null, "s3://b/_schema.parquet,s3://b/events/" + "**/*.parquet", registry));
    }

    public void testDatasetFormatCsvAndGzipAreOneFormat() {
        FormatReaderRegistry registry = csvAndParquetRegistry();
        assertEquals("csv", FormatNameResolver.datasetFormat(null, "s3://b/a.csv,s3://b/b.csv.gz", registry));
        assertEquals("csv", FormatNameResolver.datasetFormat(null, "s3://b/b.csv.gz,s3://b/a.csv", registry));
        assertEquals("csv", FormatNameResolver.datasetFormat(null, "s3://b/*.csv.gz", registry));
    }

    public void testDatasetFormatRefusesExtensionlessOrWildcardWithoutFormat() {
        FormatReaderRegistry registry = csvAndParquetRegistry();
        for (String resource : List.of("s3://b/hits/*", "s3://dir1/,s3://dir2/", "s3://b/no_extension")) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> FormatNameResolver.datasetFormat(null, resource, registry)
            );
            assertThat(e.getMessage(), containsString(FormatNameResolver.ambiguousDatasetFormatMessage()));
        }
    }

    public void testDatasetFormatRefusesMixedRegisteredFormats() {
        FormatReaderRegistry registry = csvAndParquetRegistry();
        String comma = "s3://b/a.parquet,s3://b/b.csv";
        IllegalArgumentException commaErr = expectThrows(
            IllegalArgumentException.class,
            () -> FormatNameResolver.datasetFormat(null, comma, registry)
        );
        assertThat(commaErr.getMessage(), containsString("implied formats"));
        assertThat(commaErr.getMessage(), containsString("csv"));
        assertThat(commaErr.getMessage(), containsString("parquet"));

        String braces = "s3://b/*.{parquet,csv}";
        IllegalArgumentException braceErr = expectThrows(
            IllegalArgumentException.class,
            () -> FormatNameResolver.datasetFormat(null, braces, registry)
        );
        assertThat(braceErr.getMessage(), containsString("implied formats"));
    }

    public void testDatasetFormatParquetGzThenCsvPropagatesWrapVeto() {
        FormatReaderRegistry registry = csvAndParquetRegistry();
        String resource = "s3://b/a.parquet.gz,s3://b/b.csv";
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FormatNameResolver.datasetFormat(null, resource, registry)
        );
        assertThat(e.getMessage(), containsString("does not support whole-file compression"));
        assertThat(e.getMessage(), not(containsString("implied formats")));
    }

    public void testDatasetFormatFromExtensionMapWithoutRegistry() {
        assertEquals("parquet", FormatNameResolver.datasetFormat(null, "s3://b/path/" + "*.parquet", candidate -> {
            String ext = FormatNameResolver.extractCleanExtension(candidate);
            return "parquet".equals(ext) ? "parquet" : null;
        }));
    }

    public void testWrapForObjectReusesConfiguredReader() {
        FormatReaderRegistry registry = csvAndParquetRegistry();
        FormatReader configured = registry.byName("csv");
        assertSame(configured, registry.wrapForObject(configured, "hits.csv"));
        FormatReader wrapped = registry.wrapForObject(configured, "hits.csv.gz");
        assertTrue(wrapped instanceof CompressionDelegatingFormatReader);
        assertEquals("csv", wrapped.formatName());
        FormatReader fresh = registry.byNameForObject("csv", "hits.csv.gz");
        assertTrue(fresh instanceof CompressionDelegatingFormatReader);
        assertNotSame("byNameForObject must not be the configured instance", configured, fresh);
    }

    public void testRejectConflictingListedFormatsNamesTheObject() {
        FormatReaderRegistry registry = csvAndParquetRegistry();
        FileList listing = GlobExpander.fileListOf(
            List.of(
                new StorageEntry(StoragePath.of("s3://b/a.csv"), 1, Instant.EPOCH),
                new StorageEntry(StoragePath.of("s3://b/b.parquet"), 1, Instant.EPOCH)
            ),
            "s3://b/*"
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FormatNameResolver.rejectConflictingListedFormats(listing, "csv", registry)
        );
        assertEquals(FormatNameResolver.listedFormatConflictMessage("parquet", "csv"), e.getMessage());
    }

    public void testResolveReaderDiagnosesBareCompressionSuffix() {
        FormatReaderRegistry registry = csvAndParquetRegistry();
        FormatReaderRegistry.UnreadableObjectException e = expectThrows(
            FormatReaderRegistry.UnreadableObjectException.class,
            () -> FormatNameResolver.resolveReader(null, "archive.gz", registry)
        );
        assertThat(e.getMessage(), containsString("names a compression codec, not a data format"));
        assertThat(e.getMessage(), containsString(".csv.gz"));
        assertThat(e.getMessage(), not(containsString("does not match any registered format")));
    }

    public void testResolveReaderDiagnosesOnlyTrailingExtensionOnDottedStem() {
        FormatReaderRegistry registry = csvAndParquetRegistry();
        FormatReaderRegistry.UnreadableObjectException e = expectThrows(
            FormatReaderRegistry.UnreadableObjectException.class,
            () -> FormatNameResolver.resolveReader(null, "2026.07.26.data.xyz", registry)
        );
        assertThat(e.getMessage(), containsString("extension [.xyz]"));
        assertThat(e.getMessage(), not(containsString("extension [.data.xyz]")));
    }

    public void testRejectConflictingListedFormatsAllowsUnrecognizedExtension() {
        FormatReaderRegistry registry = csvAndParquetRegistry();
        FileList listing = GlobExpander.fileListOf(
            List.of(
                new StorageEntry(StoragePath.of("s3://b/a.csv"), 1, Instant.EPOCH),
                new StorageEntry(StoragePath.of("s3://b/flow.log.gz"), 1, Instant.EPOCH)
            ),
            "s3://b/*"
        );
        FormatNameResolver.rejectConflictingListedFormats(listing, "csv", registry);
    }

    /**
     * A registry with csv (compression-capable) and parquet (not). Mockito stubs: {@code resolveFormatName}
     * touches only {@link FormatReader#formatName()}, {@link FormatReader#fileExtensions()}, and
     * {@link FormatReader#supportsWholeFileCompression()}.
     */
    private static FormatReaderRegistry csvAndParquetRegistry() {
        FormatReader csv = mock(FormatReader.class);
        when(csv.formatName()).thenReturn("csv");
        when(csv.fileExtensions()).thenReturn(List.of(".csv"));
        when(csv.supportsWholeFileCompression()).thenReturn(true);
        FormatReader parquet = mock(FormatReader.class);
        when(parquet.formatName()).thenReturn("parquet");
        when(parquet.fileExtensions()).thenReturn(List.of(".parquet", ".parq"));
        when(parquet.supportsWholeFileCompression()).thenReturn(false);
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
        registry.registerLazy("parquet", (s, bf) -> parquet, Settings.EMPTY, null);
        registry.registerExtension(".parquet", "parquet");
        registry.registerExtension(".parq", "parquet");
        return registry;
    }
}
