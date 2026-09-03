/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Datasets co-queried by one {@code FROM} whose backing files carry colliding names, up to and including being the
 * same file.
 *
 * <p>A {@code FileSplit} names the bytes to read by full {@link StoragePath} plus offset and length, and carries its
 * own format, config, and read schema. It carries no identity for the relation it was discovered for. So a repeated
 * <em>basename</em> under distinct directories is not ambiguous to the read path, but the binding of a discovered
 * split list to a producer plan is positional: the list is applied wholesale to the single external source in the
 * plan it is dispatched with. Nothing in a split would contradict being applied to the wrong relation.
 *
 * <p>The discriminating fixture is therefore two datasets over one directory with <em>unequal</em> file counts, where
 * the file they share is the identical path. Crossed or pooled split lists change how many splits each side reads,
 * and unequal counts make that visible and directional; equal counts could mask it. {@code shared_one} reads
 * {@code file1.csv} alone and {@code shared_two} reads both files, so {@code file1.csv} must be read once per dataset
 * referencing it and {@code file2.csv} exactly once overall.
 */
public class SameFileNamesAcrossDatasetsIT extends AbstractExternalDataSourceIT {

    private static final int DATASETS = 3;
    private static final int FILES_PER_DATASET = 3;
    private static final int RECORDS_PER_FILE = 3;
    private static final int TOTAL_RECORDS = DATASETS * FILES_PER_DATASET * RECORDS_PER_FILE;

    private final List<String> datasets = new ArrayList<>();

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    /**
     * Three datasets, each under its own directory holding {@code file1.csv} through {@code file3.csv}. The full paths
     * differ, so this covers the plain multi-dataset union rather than any path ambiguity.
     */
    @Before
    public void registerDatasets() throws Exception {
        datasets.clear();
        Path root = createTempDir();
        for (int ds = 1; ds <= DATASETS; ds++) {
            String name = "ds" + ds;
            Path dir = root.resolve(name);
            Files.createDirectories(dir);
            for (int file = 1; file <= FILES_PER_DATASET; file++) {
                StringBuilder csv = new StringBuilder("rec:keyword\n");
                for (int record = 1; record <= RECORDS_PER_FILE; record++) {
                    csv.append("ds").append(ds).append("_file").append(file).append("_record").append(record).append('\n');
                }
                Files.writeString(dir.resolve("file" + file + ".csv"), csv.toString(), StandardCharsets.UTF_8);
            }
            datasets.add(registerDataset(name, StoragePath.fileUri(dir) + "/*.csv", Map.of("format", "csv")));
        }
    }

    public void testEveryRecordIsReturnedExactlyOnceUnderItsOwnDataset() {
        String query = "FROM " + String.join(", ", datasets) + " | KEEP rec | SORT rec ASC";
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query), TIMEOUT)) {
            List<Object> actual = getValuesList(response).stream().map(row -> row.getFirst()).toList();
            assertThat(actual, equalTo(allRecordsSorted()));
        }
    }

    /**
     * Separates "wrong rows" from "right rows, wrong multiplicity": binding a split list onto the wrong relation
     * duplicates reads, which moves {@code COUNT(*)} while leaving the distinct set intact.
     */
    public void testNoRecordIsDuplicatedOrDropped() {
        String query = "FROM " + String.join(", ", datasets) + " | STATS total = COUNT(*), distinct = COUNT_DISTINCT(rec)";
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(((Number) rows.getFirst().get(0)).longValue(), equalTo((long) TOTAL_RECORDS));
            assertThat(((Number) rows.getFirst().get(1)).longValue(), equalTo((long) TOTAL_RECORDS));
        }
    }

    public void testEachDatasetAloneReturnsOnlyItsOwnRecords() {
        for (int ds = 1; ds <= DATASETS; ds++) {
            try (EsqlQueryResponse response = run(syncEsqlQueryRequest("FROM ds" + ds + " | KEEP rec | SORT rec ASC"), TIMEOUT)) {
                List<Object> actual = getValuesList(response).stream().map(row -> row.getFirst()).toList();
                assertThat(actual, equalTo(recordsOf(ds)));
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Unequal file counts over one directory, so the shared file is the identical full path.
    // ---------------------------------------------------------------------------------------------

    /**
     * Writes {@code file1.csv} and {@code file2.csv} into one directory and registers {@code shared_one} against
     * {@code file1.csv} alone and {@code shared_two} against both. Records are named per file, not per dataset,
     * because {@code file1.csv} is one file that both datasets read.
     */
    private void registerSharedDirDatasets() throws Exception {
        Path dir = createTempDir();
        for (int file = 1; file <= 2; file++) {
            StringBuilder csv = new StringBuilder("rec:keyword\n");
            for (int record = 1; record <= RECORDS_PER_FILE; record++) {
                csv.append("file").append(file).append("_record").append(record).append('\n');
            }
            Files.writeString(dir.resolve("file" + file + ".csv"), csv.toString(), StandardCharsets.UTF_8);
        }
        registerDataset("shared_one", StoragePath.fileUri(dir.resolve("file1.csv")), Map.of("format", "csv"));
        registerDataset("shared_two", StoragePath.fileUri(dir) + "/*.csv", Map.of("format", "csv"));
    }

    /**
     * The per-value multiplicity is the assertion that bites: the shared file contributes two rows per record and the
     * file only {@code shared_two} reads contributes one. Either dataset reading the wrong number of splits moves one
     * of those counts.
     */
    public void testSharedFileIsReadOncePerReferencingDataset() throws Exception {
        registerSharedDirDatasets();
        String query = "FROM shared_one, shared_two | STATS n = COUNT(*) BY rec | SORT rec ASC";
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query), TIMEOUT)) {
            List<List<Object>> expected = new ArrayList<>();
            for (int record = 1; record <= RECORDS_PER_FILE; record++) {
                expected.add(List.of(2L, "file1_record" + record));
            }
            for (int record = 1; record <= RECORDS_PER_FILE; record++) {
                expected.add(List.of(1L, "file2_record" + record));
            }
            assertThat(getValuesList(response), equalTo(expected));
        }
    }

    /** The one-file dataset must not pick up the second file, which only its sibling's glob covers. */
    public void testSingleFileDatasetAloneReadsOnlyItsOwnFile() throws Exception {
        registerSharedDirDatasets();
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest("FROM shared_one | KEEP rec | SORT rec ASC"), TIMEOUT)) {
            List<Object> actual = getValuesList(response).stream().map(row -> row.getFirst()).toList();
            assertThat(actual, equalTo(List.of("file1_record1", "file1_record2", "file1_record3")));
        }
    }

    public void testTwoFileDatasetAloneReadsBothFiles() throws Exception {
        registerSharedDirDatasets();
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest("FROM shared_two | KEEP rec | SORT rec ASC"), TIMEOUT)) {
            List<Object> actual = getValuesList(response).stream().map(row -> row.getFirst()).toList();
            assertThat(
                actual,
                equalTo(List.of("file1_record1", "file1_record2", "file1_record3", "file2_record1", "file2_record2", "file2_record3"))
            );
        }
    }

    /**
     * All records of the three-directory fixture in ascending order. Single-digit counters keep the nested emission
     * order identical to the lexicographic order {@code SORT rec ASC} produces.
     */
    private static List<Object> allRecordsSorted() {
        List<Object> expected = new ArrayList<>(TOTAL_RECORDS);
        for (int ds = 1; ds <= DATASETS; ds++) {
            expected.addAll(recordsOf(ds));
        }
        return expected;
    }

    private static List<Object> recordsOf(int dataset) {
        List<Object> expected = new ArrayList<>(FILES_PER_DATASET * RECORDS_PER_FILE);
        for (int file = 1; file <= FILES_PER_DATASET; file++) {
            for (int record = 1; record <= RECORDS_PER_FILE; record++) {
                expected.add("ds" + dataset + "_file" + file + "_record" + record);
            }
        }
        return expected;
    }
}
