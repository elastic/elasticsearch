/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.glob.FileOrderConfig.CONFIG_FILE_ORDER;
import static org.elasticsearch.xpack.esql.datasources.glob.FileOrderConfig.CONFIG_FILE_SORT_BY;
import static org.hamcrest.Matchers.containsString;

/**
 * First-file-wins listing knobs: defaults, FFW-only rejection, and the six {@link FileOrderConfig#apply}
 * orderings. Unknown values throw rather than degrading — these keys never existed on stored datasets.
 */
public class FileOrderConfigTests extends ESTestCase {

    private static final String FFW = "first_file_wins";
    private static final String SCHEMA_RESOLUTION = "schema_resolution";

    public void testAbsentKeysUnderFfwDefaultToListAsc() {
        assertEquals(FileOrderConfig.DEFAULT, FileOrderConfig.fromConfig(null));
        assertEquals(FileOrderConfig.DEFAULT, FileOrderConfig.fromConfig(Map.of()));
        assertEquals(FileOrderConfig.DEFAULT, FileOrderConfig.fromConfig(Map.of(SCHEMA_RESOLUTION, FFW)));
        assertEquals(FileOrderConfig.DEFAULT, FileOrderConfig.forListing(Map.of(SCHEMA_RESOLUTION, FFW)));
    }

    public void testEitherKeyAloneDefaultsTheOther() {
        assertEquals(
            new FileOrderConfig(FileOrderConfig.SortBy.MTIME, FileOrderConfig.Order.ASC),
            FileOrderConfig.fromConfig(Map.of(CONFIG_FILE_SORT_BY, "mtime"))
        );
        assertEquals(
            new FileOrderConfig(FileOrderConfig.SortBy.LIST, FileOrderConfig.Order.DESC),
            FileOrderConfig.fromConfig(Map.of(CONFIG_FILE_ORDER, "desc"))
        );
    }

    public void testValuesAreCaseInsensitive() {
        assertEquals(
            new FileOrderConfig(FileOrderConfig.SortBy.NAME, FileOrderConfig.Order.DESC),
            FileOrderConfig.fromConfig(Map.of(CONFIG_FILE_SORT_BY, "NAME", CONFIG_FILE_ORDER, "Desc"))
        );
    }

    public void testUnknownSortByIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FileOrderConfig.fromConfig(Map.of(CONFIG_FILE_SORT_BY, "created_time"))
        );
        assertThat(e.getMessage(), containsString("Unknown file_sort_by value [created_time]"));
        assertThat(e.getMessage(), containsString("list, name, mtime"));
    }

    public void testUnknownOrderIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FileOrderConfig.fromConfig(Map.of(CONFIG_FILE_ORDER, "newest"))
        );
        assertThat(e.getMessage(), containsString("Unknown file_order value [newest]"));
        assertThat(e.getMessage(), containsString("asc, desc"));
    }

    public void testForListingWithoutFfwIsNameAsc() {
        assertEquals(FileOrderConfig.NAME_ASC, FileOrderConfig.forListing(null));
        assertEquals(FileOrderConfig.NAME_ASC, FileOrderConfig.forListing(Map.of()));
        assertEquals(FileOrderConfig.NAME_ASC, FileOrderConfig.forListing(Map.of(SCHEMA_RESOLUTION, "union_by_name")));
        assertEquals(FileOrderConfig.NAME_ASC, FileOrderConfig.forListing(Map.of(SCHEMA_RESOLUTION, "strict")));
    }

    public void testKnobsWithoutFfwAreRejected() {
        for (Map<String, Object> config : List.of(
            Map.<String, Object>of(CONFIG_FILE_SORT_BY, "list"),
            Map.<String, Object>of(CONFIG_FILE_ORDER, "desc"),
            Map.<String, Object>of(SCHEMA_RESOLUTION, "union_by_name", CONFIG_FILE_SORT_BY, "list"),
            Map.<String, Object>of(SCHEMA_RESOLUTION, "strict", CONFIG_FILE_ORDER, "asc"),
            Map.<String, Object>of(SCHEMA_RESOLUTION, "banana", CONFIG_FILE_SORT_BY, "name")
        )) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> FileOrderConfig.validate(config));
            assertThat(e.getMessage(), containsString(CONFIG_FILE_SORT_BY));
            assertThat(e.getMessage(), containsString(CONFIG_FILE_ORDER));
            assertThat(e.getMessage(), containsString("first_file_wins"));
        }
    }

    public void testValidateAcceptsFfwWithKnobs() {
        FileOrderConfig.validate(Map.of(SCHEMA_RESOLUTION, FFW));
        FileOrderConfig.validate(Map.of(SCHEMA_RESOLUTION, FFW, CONFIG_FILE_SORT_BY, "mtime"));
        FileOrderConfig.validate(Map.of(SCHEMA_RESOLUTION, FFW, CONFIG_FILE_ORDER, "desc"));
        FileOrderConfig.validate(Map.of(SCHEMA_RESOLUTION, FFW, CONFIG_FILE_SORT_BY, "name", CONFIG_FILE_ORDER, "desc"));
    }

    public void testApplyListAscIsNoOp() {
        List<StorageEntry> files = files("s3://b/z.parquet", "s3://b/a.parquet");
        FileOrderConfig.DEFAULT.apply(files);
        assertEquals("s3://b/z.parquet", files.get(0).path().toString());
        assertEquals("s3://b/a.parquet", files.get(1).path().toString());
    }

    public void testApplyListDescReverses() {
        List<StorageEntry> files = files("s3://b/z.parquet", "s3://b/a.parquet");
        new FileOrderConfig(FileOrderConfig.SortBy.LIST, FileOrderConfig.Order.DESC).apply(files);
        assertEquals("s3://b/a.parquet", files.get(0).path().toString());
        assertEquals("s3://b/z.parquet", files.get(1).path().toString());
    }

    public void testApplyNameAscIsLexSmallest() {
        List<StorageEntry> files = files("s3://b/z.parquet", "s3://b/a.parquet");
        FileOrderConfig.NAME_ASC.apply(files);
        assertEquals("s3://b/a.parquet", files.get(0).path().toString());
    }

    public void testApplyNameDescPicksLastHiveDate() {
        List<StorageEntry> files = files("s3://b/dt=2024-01-01/p.parquet", "s3://b/dt=2026-09-04/p.parquet");
        new FileOrderConfig(FileOrderConfig.SortBy.NAME, FileOrderConfig.Order.DESC).apply(files);
        assertEquals("s3://b/dt=2026-09-04/p.parquet", files.get(0).path().toString());
    }

    public void testApplyMtimeTiesBreakByNameAsc() {
        List<StorageEntry> files = new ArrayList<>();
        files.add(entry("s3://b/z.parquet", 100));
        files.add(entry("s3://b/a.parquet", 100));
        files.add(entry("s3://b/m.parquet", 50));
        new FileOrderConfig(FileOrderConfig.SortBy.MTIME, FileOrderConfig.Order.DESC).apply(files);
        assertEquals("same mtime, name-asc tie-break: a before z, both newer than m", "s3://b/a.parquet", files.get(0).path().toString());
        assertEquals("s3://b/z.parquet", files.get(1).path().toString());
        assertEquals("s3://b/m.parquet", files.get(2).path().toString());
    }

    public void testApplyMtimeAscMissingMtimeIsOldest() {
        List<StorageEntry> files = new ArrayList<>();
        files.add(entry("s3://b/new.parquet", 200));
        files.add(new StorageEntry(StoragePath.of("s3://b/old.parquet"), 1, null));
        new FileOrderConfig(FileOrderConfig.SortBy.MTIME, FileOrderConfig.Order.ASC).apply(files);
        assertEquals("s3://b/old.parquet", files.get(0).path().toString());
    }

    public void testForListingRejectsKnobsOnUbnWithoutDegrading() {
        Map<String, Object> config = new HashMap<>();
        config.put(SCHEMA_RESOLUTION, "union_by_name");
        config.put(CONFIG_FILE_SORT_BY, "mtime");
        expectThrows(IllegalArgumentException.class, () -> FileOrderConfig.forListing(config));
    }

    private static List<StorageEntry> files(String... paths) {
        List<StorageEntry> files = new ArrayList<>(paths.length);
        for (String path : paths) {
            files.add(entry(path, 0));
        }
        return files;
    }

    private static StorageEntry entry(String path, long mtimeMillis) {
        return new StorageEntry(StoragePath.of(path), 1, Instant.ofEpochMilli(mtimeMillis));
    }
}
