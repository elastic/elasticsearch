/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Unit tests for {@link DecompressionCodecRegistry}.
 */
public class DecompressionCodecRegistryTests extends ESTestCase {

    public void testRegisterAndLookupByExtension() {
        DecompressionCodecRegistry registry = new DecompressionCodecRegistry();
        DecompressionCodec codec = mockCodec("gzip", List.of(".gz", ".gzip"));

        registry.register(codec);

        assertEquals(codec, registry.byExtension(".gz"));
        assertEquals(codec, registry.byExtension(".gzip"));
        assertEquals(codec, registry.byExtension("gz"));
        assertEquals(codec, registry.byExtension("GZ"));
    }

    public void testHasCompressionExtension() {
        DecompressionCodecRegistry registry = new DecompressionCodecRegistry();
        registry.register(mockCodec("gzip", List.of(".gz")));

        assertTrue(registry.hasCompressionExtension(".gz"));
        assertTrue(registry.hasCompressionExtension("gz"));
        assertFalse(registry.hasCompressionExtension(".csv"));
        assertFalse(registry.hasCompressionExtension(""));
        assertFalse(registry.hasCompressionExtension(null));
    }

    public void testStripCompressionSuffix() {
        DecompressionCodecRegistry registry = new DecompressionCodecRegistry();
        registry.register(mockCodec("gzip", List.of(".gz")));

        assertEquals("data.csv", registry.stripCompressionSuffix("data.csv.gz"));
        assertEquals("logs.ndjson", registry.stripCompressionSuffix("logs.ndjson.gz"));
        assertNull(registry.stripCompressionSuffix("data.csv"));
        assertNull(registry.stripCompressionSuffix("data"));
        assertNull(registry.stripCompressionSuffix(""));
        assertNull(registry.stripCompressionSuffix(null));
    }

    public void testStripCompressionSuffixUnknownExtension() {
        DecompressionCodecRegistry registry = new DecompressionCodecRegistry();
        registry.register(mockCodec("gzip", List.of(".gz")));

        assertNull(registry.stripCompressionSuffix("data.csv.bz2"));
    }

    public void testByExtensionUnknownReturnsNull() {
        DecompressionCodecRegistry registry = new DecompressionCodecRegistry();
        registry.register(mockCodec("gzip", List.of(".gz")));

        assertNull(registry.byExtension(".csv"));
        assertNull(registry.byExtension(""));
        assertNull(registry.byExtension(null));
    }

    public void testRegisterNullCodecThrows() {
        DecompressionCodecRegistry registry = new DecompressionCodecRegistry();
        expectThrows(QlIllegalArgumentException.class, () -> registry.register(null));
    }

    public void testDuplicateExtensionDifferentCodecThrows() {
        DecompressionCodecRegistry registry = new DecompressionCodecRegistry();
        registry.register(mockCodec("gzip", List.of(".gz")));
        expectThrows(IllegalArgumentException.class, () -> registry.register(mockCodec("other", List.of(".gz"))));
    }

    public void testSameCodecMultipleExtensions() {
        DecompressionCodecRegistry registry = new DecompressionCodecRegistry();
        DecompressionCodec codec = mockCodec("gzip", List.of(".gz", ".gzip"));
        registry.register(codec);

        assertEquals(codec, registry.byExtension(".gz"));
        assertEquals(codec, registry.byExtension(".gzip"));
    }

    private static DecompressionCodec mockCodec(String name, List<String> extensions) {
        return new DecompressionCodec() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public List<String> extensions() {
                return extensions;
            }

            @Override
            public InputStream decompress(InputStream raw) throws IOException {
                return raw;
            }
        };
    }
}
