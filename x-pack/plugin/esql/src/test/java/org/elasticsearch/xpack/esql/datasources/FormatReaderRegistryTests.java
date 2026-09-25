/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.gzip.GzipDecompressionCodec;
import org.elasticsearch.xpack.esql.datasource.zstd.ZstdDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Pins the single-writer rule for the extension&rarr;format mapping: an extension claimed by two
 * formats would validate against one format at PUT and read as the other at query time, so BOTH
 * write paths — the eager spec-declared {@link FormatReaderRegistry#registerExtension} and the lazy
 * reader-declared claim inside {@code registerLazy}'s supplier — must reject the conflict instead of
 * silently overwriting.
 */
public class FormatReaderRegistryTests extends ESTestCase {

    public void testEagerDuplicateExtensionAcrossFormatsThrows() {
        FormatReaderRegistry registry = new FormatReaderRegistry(new DecompressionCodecRegistry());
        registry.registerLazy("csv", (s, bf) -> reader("csv", ".csv"), Settings.EMPTY, null);
        registry.registerLazy("tsv", (s, bf) -> reader("tsv", ".tsv"), Settings.EMPTY, null);
        registry.registerExtension(".csv", "csv");

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> registry.registerExtension(".csv", "tsv"));
        assertThat(e.getMessage(), containsString("conflicting formats for extension [.csv]"));
        assertThat(e.getMessage(), containsString("csv"));
        assertThat(e.getMessage(), containsString("tsv"));
    }

    public void testEagerReRegistrationForTheSameFormatIsANoOp() {
        FormatReaderRegistry registry = new FormatReaderRegistry(new DecompressionCodecRegistry());
        registry.registerLazy("csv", (s, bf) -> reader("csv", ".csv"), Settings.EMPTY, null);
        registry.registerExtension(".csv", "csv");
        registry.registerExtension(".csv", "csv");
        assertTrue(registry.hasExtension(".csv"));
    }

    /**
     * The lazy path: a reader declaring an extension already owned by ANOTHER format must fail at
     * materialization, not steal the mapping. Before this rule the supplier's plain {@code put}
     * silently rewired the extension to whichever format materialized last.
     */
    public void testLazyMaterializationConflictingWithAnotherFormatThrows() {
        FormatReaderRegistry registry = new FormatReaderRegistry(new DecompressionCodecRegistry());
        registry.registerLazy("csv", (s, bf) -> reader("csv", ".csv"), Settings.EMPTY, null);
        // greedy claims its own extension first, then the conflicting .csv
        registry.registerLazy("greedy", (s, bf) -> reader("greedy", ".greedy", ".csv"), Settings.EMPTY, null);
        registry.registerExtension(".csv", "csv");

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> registry.byName("greedy"));
        assertThat(e.getMessage(), containsString("conflicting formats for extension [.csv]"));

        // The victim's mapping is intact: .csv still resolves to the csv reader.
        assertEquals("csv", registry.byExtension("data.csv").formatName());
        // The failed attempt's own claims are rolled back: .greedy was claimed before .csv
        // conflicted and must not stay owned by a reader that never published.
        assertFalse(registry.hasExtension(".greedy"));
    }

    /** A reader re-declaring the extension its own spec registered eagerly materializes fine (idempotent claim). */
    public void testLazyMaterializationReclaimingOwnSpecExtensionIsFine() {
        FormatReaderRegistry registry = new FormatReaderRegistry(new DecompressionCodecRegistry());
        FormatReader csv = reader("csv", ".csv");
        registry.registerLazy("csv", (s, bf) -> csv, Settings.EMPTY, null);
        registry.registerExtension(".csv", "csv");

        assertThat(registry.byName("csv"), sameInstance(csv));
        assertThat(registry.byExtension("data.csv"), sameInstance(csv));
    }

    /** A reader-declared extension nobody else claimed registers normally through the lazy path. */
    public void testLazyMaterializationClaimsItsOwnNewExtension() {
        FormatReaderRegistry registry = new FormatReaderRegistry(new DecompressionCodecRegistry());
        FormatReader csv = reader("csv", ".csv", ".txt");
        registry.registerLazy("csv", (s, bf) -> csv, Settings.EMPTY, null);
        registry.registerExtension(".csv", "csv");

        assertFalse(registry.hasExtension(".txt"));
        registry.byName("csv"); // materialize
        assertTrue(registry.hasExtension(".txt"));
        assertThat(registry.byExtension("data.txt"), sameInstance(csv));
    }

    /**
     * The registry picks the ratio by codec and reads it live, so a dynamic setting update reaches
     * existing readers. Verified behaviorally: drive {@code metadata()} with a small highly-compressed
     * object; after tightening the limit the guard fires, after disabling it the read succeeds.
     */
    public void testDecompressionRatioFollowsCodecAndLiveUpdates() throws Exception {
        // Build a ~130 KB gzip that expands ~515:1 (64 MiB of repeated text)
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gz2 = new GZIPOutputStream(baos)) {
            byte[] line = "{\"val\":1}\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            for (int i = 0; i < 64 * 1024 * 1024 / line.length; i++) {
                gz2.write(line);
            }
        }
        byte[] compressed = baos.toByteArray();

        StorageObject highlyCompressible = mock(StorageObject.class);
        when(highlyCompressible.newStream()).thenAnswer(inv -> new java.io.ByteArrayInputStream(compressed));
        when(highlyCompressible.knownLength()).thenReturn((long) compressed.length);

        DecompressionCodecRegistry codecs = new DecompressionCodecRegistry();
        codecs.register(new GzipDecompressionCodec());
        codecs.register(new ZstdDecompressionCodec());
        FormatReaderRegistry registry = new FormatReaderRegistry(codecs);
        FormatReader csv = reader("csv", ".csv");
        when(csv.supportsWholeFileCompression()).thenReturn(true);
        // stub metadata() to actually consume the decompressed stream — this drives the ratio guard
        when(csv.metadata(any())).thenAnswer(inv -> {
            StorageObject obj = inv.getArgument(0);
            try (InputStream s = obj.newStream()) {
                s.transferTo(java.io.OutputStream.nullOutputStream());
            }
            return null;
        });

        FormatReader gz = registry.wrapForObject(csv, "data.csv.gz");

        // ratio=200: input is ~515:1 so the guard must fire
        registry.setMaxDecompressionRatio(200);
        expectThrows(ExternalClientException.class, () -> gz.metadata(highlyCompressible));

        // Disable the guard: same object must pass
        registry.setMaxDecompressionRatio(0);
        gz.metadata(highlyCompressible); // must not throw
    }

    /**
     * Mockito stub deliberately: the registry touches only {@code formatName()} and {@code fileExtensions()};
     * a full {@link FormatReader} implementation ({@code metadata}/{@code read}/{@code withConfigTrackingConsumedKeys})
     * would be far larger for zero added coverage.
     */
    private static FormatReader reader(String format, String... extensions) {
        FormatReader reader = mock(FormatReader.class);
        when(reader.formatName()).thenReturn(format);
        when(reader.fileExtensions()).thenReturn(List.of(extensions));
        return reader;
    }
}
