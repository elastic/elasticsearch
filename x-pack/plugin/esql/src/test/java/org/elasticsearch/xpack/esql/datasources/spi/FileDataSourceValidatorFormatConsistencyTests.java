/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.DecompressionCodecRegistry;
import org.elasticsearch.xpack.esql.datasources.FormatNameResolver;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;

import java.io.InputStream;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Pins {@link FileDataSourceValidator#formatFromExtension} to the registry route for the resource
 * names in elastic/esql-planning#1869. The validator must not walk suffixes itself.
 */
public class FileDataSourceValidatorFormatConsistencyTests extends ESTestCase {

    public void testFormatFromExtensionMatchesRegistryRoute() {
        FormatReaderRegistry registry = csvAndParquetRegistry();
        FileDataSourceValidator validator = new FileDataSourceValidator("s3", (raw, consumed) -> null, Set.of("s3"))
            .withFormatReaderRegistry(registry);

        for (String resource : List.of("data.csv", "data.csv.gz", "data.parquet")) {
            String expected = FormatNameResolver.resolveFormatName(null, resource, registry);
            assertEquals(resource, expected, validator.formatFromExtension(resource));
            assertEquals(resource, expected, safeFormatName(registry, resource));
        }
        assertEquals("csv", validator.formatFromExtension("data.csv"));
        assertEquals("csv", validator.formatFromExtension("data.csv.gz"));
        assertEquals("parquet", validator.formatFromExtension("data.parquet"));

        // Unreadable on the raw registry route; CRUD adapter returns null rather than throw.
        for (String resource : List.of("data.tar.gz", "no_extension")) {
            expectThrows(IllegalArgumentException.class, () -> FormatNameResolver.resolveFormatName(null, resource, registry));
            assertNull(resource, validator.formatFromExtension(resource));
            assertNull(resource, safeFormatName(registry, resource));
        }
    }

    public void testFormatFromExtensionWithoutRegistryIsNull() {
        FileDataSourceValidator validator = new FileDataSourceValidator("s3", (raw, consumed) -> null, Set.of("s3"));
        assertNull(validator.formatFromExtension("data.csv"));
        assertNull(validator.formatFromExtension("data.csv.gz"));
    }

    /**
     * Same adapter the validator uses: the registry throws for unreadable names; CRUD needs null.
     */
    private static String safeFormatName(FormatReaderRegistry registry, String resource) {
        try {
            return FormatNameResolver.resolveFormatName(null, resource, registry);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Mockito stubs: {@link FormatNameResolver#resolveFormatName} and {@link FormatReaderRegistry#byExtension}
     * touch only {@link FormatReader#formatName()}, {@link FormatReader#fileExtensions()}, and
     * {@link FormatReader#supportsWholeFileCompression()}.
     */
    private static FormatReaderRegistry csvAndParquetRegistry() {
        FormatReader csv = mock(FormatReader.class);
        when(csv.formatName()).thenReturn("csv");
        when(csv.fileExtensions()).thenReturn(List.of(".csv"));
        when(csv.supportsWholeFileCompression()).thenReturn(true);
        FormatReader parquet = mock(FormatReader.class);
        when(parquet.formatName()).thenReturn("parquet");
        when(parquet.fileExtensions()).thenReturn(List.of(".parquet"));
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
        return registry;
    }
}
