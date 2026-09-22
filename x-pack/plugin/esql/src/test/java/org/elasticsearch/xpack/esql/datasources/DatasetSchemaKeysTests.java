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
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * The key a dataset's resolution is cached under must change whenever something the cached result depends on
 * changes — the files, or a setting that changes what is read — and not otherwise.
 */
public class DatasetSchemaKeysTests extends ESTestCase {

    private static final String BASE = "s3://bucket/data/";

    private static FileList listing(String... names) {
        return GlobExpander.fileListOf(
            Arrays.stream(names).map(n -> new StorageEntry(StoragePath.of(BASE + n), 100, Instant.ofEpochMilli(5_000))).toList(),
            BASE + "*"
        );
    }

    private static DatasetSchemaKey key(SchemaBreadth breadth, FileList listing, Map<String, Object> config) {
        return DatasetSchemaKeys.of(breadth, listing, "csv", config);
    }

    public void testUnionByNameAndStrictOverTheSameFilesAreDifferentResults() {
        FileList files = listing("a.csv", "b.csv");
        assertNotEquals(
            key(SchemaBreadth.EVERY_FILE, files, Map.of("schema_resolution", "union_by_name")),
            key(SchemaBreadth.EVERY_FILE, files, Map.of("schema_resolution", "strict"))
        );
    }

    /**
     * Why the settings come from the per-file cache's identity and not from what reaches schema inference: the entry
     * carries statistics too. The error mode decides whether a row that fails to parse is dropped, which changes a
     * row count without changing a single column of the schema — so a key made only of schema-affecting settings
     * would serve one error mode's counts to another.
     */
    public void testASettingThatChangesWhatIsReadButNotTheSchemaStillMisses() {
        FileList files = listing("a.ndjson", "b.ndjson");
        assertNotEquals(
            key(SchemaBreadth.EVERY_FILE, files, Map.of("error_mode", "skip_row")),
            key(SchemaBreadth.EVERY_FILE, files, Map.of("error_mode", "null_field"))
        );
    }

    public void testASettingThatChangesNothingReadCannotSplitTheCache() {
        FileList files = listing("a.csv", "b.csv");
        assertEquals(
            "split sizing shapes how files are read, not what is read from them",
            key(SchemaBreadth.EVERY_FILE, files, Map.of("target_split_size", "1mb")),
            key(SchemaBreadth.EVERY_FILE, files, Map.of("target_split_size", "64mb"))
        );
    }

    public void testNoCredentialReachesTheKey() {
        DatasetSchemaKey key = key(
            SchemaBreadth.EVERY_FILE,
            listing("a.csv", "b.csv"),
            Map.of("delimiter", ",", "access_key", "AKIA...", "secret_key", "shh", "auth", "anonymous")
        );
        assertEquals(Map.of("delimiter", ","), key.schemaSettings());
    }

    public void testWhereTheFilesLiveIsPartOfTheKey() {
        FileList files = listing("a.csv", "b.csv");
        assertNotEquals(
            "one path on two endpoints is two files",
            key(SchemaBreadth.EVERY_FILE, files, Map.of("endpoint", "https://s3.amazonaws.com")),
            key(SchemaBreadth.EVERY_FILE, files, Map.of("endpoint", "http://minio:9000"))
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
        assertNotEquals(key(SchemaBreadth.EVERY_FILE, files, commaDelimited), key(SchemaBreadth.EVERY_FILE, files, oneOddDelimiter));
    }

    public void testDeclarationProducesNoKey() {
        assertNull(key(SchemaBreadth.DECLARATION, listing("a.csv", "b.csv"), Map.of()));
    }

    private static String render(Map<String, Object> config) {
        return new TreeMap<>(config).entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(","));
    }
}
