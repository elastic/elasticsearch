/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.sameInstance;
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
