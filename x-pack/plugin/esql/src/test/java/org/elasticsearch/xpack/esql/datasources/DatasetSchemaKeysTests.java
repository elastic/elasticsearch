/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.cache.DatasetSchemaKey;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * The key a dataset's inferred schema is cached under must change exactly when the schema could, and not otherwise:
 * a key too narrow serves one dataset another's schema, and one too wide only misses.
 */
public class DatasetSchemaKeysTests extends ESTestCase {

    private static final String BASE = "s3://bucket/data/";
    /** A text reader's declaration: its settings reach inference, and so does its error policy. */
    private static final Set<String> TEXT_READER_KEYS = Set.of("delimiter", "quote", "null_value", "error_mode", "max_errors");
    /** A footer reader's declaration: nothing configurable reaches a schema read from the file's own footer. */
    private static final Set<String> FOOTER_READER_KEYS = Set.of();

    private static FileList listing(String... names) {
        return GlobExpander.fileListOf(
            Arrays.stream(names).map(n -> new StorageEntry(StoragePath.of(BASE + n), 100, Instant.ofEpochMilli(5_000))).toList(),
            BASE + "*"
        );
    }

    private static DatasetSchemaKey key(SchemaBreadth breadth, FileList listing, Set<String> readerKeys, Map<String, Object> config) {
        return DatasetSchemaKeys.of(breadth, listing, "csv", readerKeys, config);
    }

    public void testUnionByNameAndStrictOverTheSameFilesAreDifferentSchemas() {
        FileList files = listing("a.csv", "b.csv");
        assertNotEquals(
            key(SchemaBreadth.EVERY_FILE, files, TEXT_READER_KEYS, Map.of("schema_resolution", "union_by_name")),
            key(SchemaBreadth.EVERY_FILE, files, TEXT_READER_KEYS, Map.of("schema_resolution", "strict"))
        );
    }

    /** The over-keying the shared per-file list carries: a footer format's key must not vary with text settings. */
    public void testAFooterFormatKeyIgnoresTextSettings() {
        FileList files = listing("a.parquet", "b.parquet");
        assertEquals(
            key(SchemaBreadth.EVERY_FILE, files, FOOTER_READER_KEYS, Map.of("delimiter", ",")),
            key(SchemaBreadth.EVERY_FILE, files, FOOTER_READER_KEYS, Map.of("delimiter", "|"))
        );
    }

    public void testATextFormatKeyFollowsItsDeclaredSettings() {
        FileList files = listing("a.csv", "b.csv");
        assertNotEquals(
            "the error budget reaches CSV inference",
            key(SchemaBreadth.EVERY_FILE, files, TEXT_READER_KEYS, Map.of("max_errors", "1")),
            key(SchemaBreadth.EVERY_FILE, files, TEXT_READER_KEYS, Map.of("max_errors", "1000"))
        );
        assertEquals(
            "a setting the reader did not declare cannot reach the key",
            key(SchemaBreadth.EVERY_FILE, files, TEXT_READER_KEYS, Map.of("target_split_size", "1mb")),
            key(SchemaBreadth.EVERY_FILE, files, TEXT_READER_KEYS, Map.of("target_split_size", "64mb"))
        );
    }

    /**
     * Under first_file_wins the listing order only decides which file is the anchor, and the anchor's own identity is
     * already in the key; so the order settings cannot change the cached schema and must not split the cache.
     */
    public void testFirstFileWinsKeyIsIndependentOfTheOrderSettings() {
        FileList files = listing("a.csv", "b.csv");
        assertEquals(
            key(SchemaBreadth.ONE_FILE, files, TEXT_READER_KEYS, Map.of("file_sort_by", "name", "file_order", "asc")),
            key(SchemaBreadth.ONE_FILE, files, TEXT_READER_KEYS, Map.of("file_sort_by", "modified", "file_order", "desc"))
        );
    }

    public void testNoCredentialReachesTheKey() {
        DatasetSchemaKey key = key(
            SchemaBreadth.EVERY_FILE,
            listing("a.csv", "b.csv"),
            TEXT_READER_KEYS,
            Map.of("delimiter", ",", "access_key", "AKIA...", "secret_key", "shh", "auth", "anonymous")
        );
        assertEquals(Map.of("delimiter", ","), key.schemaSettings());
    }

    public void testWhereTheFilesLiveIsPartOfTheKey() {
        FileList files = listing("a.csv", "b.csv");
        assertNotEquals(
            "one path on two endpoints is two files",
            key(SchemaBreadth.EVERY_FILE, files, TEXT_READER_KEYS, Map.of("endpoint", "https://s3.amazonaws.com")),
            key(SchemaBreadth.EVERY_FILE, files, TEXT_READER_KEYS, Map.of("endpoint", "http://minio:9000"))
        );
    }

    /**
     * Why the settings are a map and not a rendered string: two configurations that differ render to the SAME
     * {@code key=value,...} string once a value contains a comma, and a CSV delimiter is often one. Held as a map, the
     * two keys stay apart.
     */
    public void testSettingsThatWouldRenderAlikeStillKeyApart() {
        Map<String, Object> commaDelimited = Map.of("delimiter", ",", "quote", "x");
        Map<String, Object> oneOddDelimiter = Map.of("delimiter", ",,quote=x");
        assertEquals("the rendered forms collide", render(commaDelimited), render(oneOddDelimiter));
        FileList files = listing("a.csv", "b.csv");
        assertNotEquals(
            key(SchemaBreadth.EVERY_FILE, files, TEXT_READER_KEYS, commaDelimited),
            key(SchemaBreadth.EVERY_FILE, files, TEXT_READER_KEYS, oneOddDelimiter)
        );
    }

    public void testDeclarationProducesNoKey() {
        assertNull(key(SchemaBreadth.DECLARATION, listing("a.csv", "b.csv"), TEXT_READER_KEYS, Map.of()));
    }

    private static String render(Map<String, Object> config) {
        return new TreeMap<>(config).entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(","));
    }
}
