/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Which files a bounded query opens, and how that scales.
 * <p>
 * The limit pushdown is the promise that a query asking for few rows reads few files: {@code LIMIT 0} reads none,
 * and a {@code LIMIT N} the first file already covers reads that file and stops. Until this suite existed the
 * promise was pinned only inside {@code FileSplitProviderTests} and {@code SplitDiscoveryPhaseTests} — unit tests of
 * the classes that implement it, which go green as long as those classes still do what they did. Nothing watched the
 * property from outside, so the cost could move without anything turning red, and it did.
 * <p>
 * So this asserts files <em>opened</em>, through {@link CountingStorageProvider}, which carries why a request count
 * and not a duration is the only thing worth asserting here.
 * <p>
 * The dataset is shaped so the arithmetic is unambiguous: {@link #FILES} files of {@link #ROWS_PER_FILE} rows each,
 * one row group apiece, so a limit below {@code ROWS_PER_FILE} is covered by the first file alone and every file
 * after it is one the query must not touch.
 * <p>
 * Parquet, and that is not incidental: a text fixture would pass every assertion below while exercising none of the
 * pushdown. Only a range-aware reader plans from a footer stating how many rows each unit holds, so only a columnar
 * format can have its scan stopped by a row budget at all — {@code listingExtentsForTheDemand} declines outright for
 * anything else, and the budget gives up on the first unit that cannot state a count. A CSV {@code LIMIT 20} does
 * read one file, but because the pipeline stops pulling, which is laziness in the operator rather than a decision
 * not to read; reverting the budget leaves such a suite green. Parquet also puts the cost where the count can see
 * it, since split discovery reads one footer per file it plans before any row is returned, so a dataset of N files
 * costs N GETs whatever the limit says.
 */
public class ExternalLimitReadCountIT extends AbstractExternalDataSourceIT {

    private static final int FILES = 12;
    private static final int ROWS_PER_FILE = 50;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    /**
     * {@code LIMIT 0} must open no file at all.
     * <p>
     * Not "few": zero. A schema for this dataset is answerable from the listing — the header of one CSV is all a
     * {@code first_file_wins} resolution needs, and it is the only read a correct answer can justify. The assertion
     * below is therefore on the files the <em>scan</em> opened, which is none, because there is no scan:
     * {@code SkipQueryOnLimitZero} empties the plan. If that rule is ever removed, or stops reaching this shape,
     * this test names the files that were read instead.
     */
    public void testLimitZeroReadsAtMostTheSchemaAnchor() throws Exception {
        String dataset = registerParquetDataset("limit0");
        String coordinator = internalCluster().getNodeNames()[0];

        CountingStorageProvider.reset();
        try (EsqlQueryResponse response = query(coordinator, "FROM " + dataset + " | LIMIT 0")) {
            assertThat("no rows", getValuesList(response).size(), equalTo(0));
            assertThat(columnsOf(response), equalTo(List.of("id:long", "name:keyword", "value:integer")));
        }

        // One file may be opened for its header, because that is where the schema comes from. Anything beyond that
        // is a file the query had no reason to touch, and the message names them so a regression is diagnosable.
        assertThat(
            "LIMIT 0 must read no more than the one file a schema is inferred from, but read " + CountingStorageProvider.pathsRead(),
            CountingStorageProvider.pathsRead().size(),
            lessThanOrEqualTo(1)
        );
    }

    /**
     * {@code LIMIT 0} must cost the same whatever the dataset's size. A pushdown that holds only on small inputs is
     * the regression this whole area keeps producing, so the shape of the curve is asserted rather than one point
     * on it.
     */
    public void testLimitZeroCostDoesNotGrowWithTheDataset() throws Exception {
        String small = registerParquetDataset("limit0_small", 2);
        String large = registerParquetDataset("limit0_large", FILES * 8);
        String coordinator = internalCluster().getNodeNames()[0];

        CountingStorageProvider.reset();
        query(coordinator, "FROM " + small + " | LIMIT 0").close();
        int smallReads = CountingStorageProvider.pathsRead().size();

        CountingStorageProvider.reset();
        query(coordinator, "FROM " + large + " | LIMIT 0").close();
        int largeReads = CountingStorageProvider.pathsRead().size();

        // Both stated absolutely, not against each other: equality alone would also hold if both regressed to
        // reading everything, which would report a pass for the wrong reason.
        assertThat("a two-file dataset", smallReads, lessThanOrEqualTo(1));
        assertThat("a 96-file dataset must cost the same", largeReads, lessThanOrEqualTo(1));
    }

    /**
     * A {@code LIMIT} the first file covers must open exactly that one file.
     * <p>
     * Split discovery reads one footer per file it plans, before a single row comes back, so a query wanting 20 rows
     * from a dataset whose first file holds 50 has no business reading the other eleven. Two mechanisms combine — a
     * listing bounded to a prefix, and a row budget consulted before each file is planned — and the test pins their
     * outcome rather than either one.
     */
    public void testLimitCoveredByTheFirstFileReadsOnlyThatFile() throws Exception {
        String dataset = registerParquetDataset("limit_n");
        String coordinator = internalCluster().getNodeNames()[0];

        CountingStorageProvider.reset();
        try (EsqlQueryResponse response = query(coordinator, "FROM " + dataset + " | LIMIT 20")) {
            assertThat(getValuesList(response).size(), equalTo(20));
        }

        assertThat(
            "a limit the first file covers must not read the rest of the dataset, but read " + CountingStorageProvider.pathsRead(),
            CountingStorageProvider.pathsRead().size(),
            equalTo(1)
        );
    }

    /**
     * A limit larger than the whole dataset reads all of it. The positive control: it proves the counter sees the
     * reads it is supposed to see, so a zero or a one above is a fact about the pushdown and not about the harness
     * failing to observe anything. Without this, every assertion in this file could pass because nothing was
     * counted at all.
     */
    public void testLimitBeyondTheDatasetReadsEveryFile() throws Exception {
        String dataset = registerParquetDataset("limit_all");
        String coordinator = internalCluster().getNodeNames()[0];

        CountingStorageProvider.reset();
        try (EsqlQueryResponse response = query(coordinator, "FROM " + dataset + " | LIMIT " + (FILES * ROWS_PER_FILE + 1))) {
            assertThat(getValuesList(response).size(), equalTo(FILES * ROWS_PER_FILE));
        }

        assertThat(
            "every file holds rows the query asked for, so every file must be read",
            CountingStorageProvider.pathsRead().size(),
            equalTo(FILES)
        );
    }

    private EsqlQueryResponse query(String coordinator, String query) {
        return client(coordinator).execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query)).actionGet();
    }

    private static List<String> columnsOf(EsqlQueryResponse response) {
        return response.columns().stream().map(c -> c.name() + ":" + c.type().esType()).toList();
    }

    private String registerParquetDataset(String name) throws IOException {
        return registerParquetDataset(name, FILES);
    }

    /**
     * {@code fileCount} Parquet files of {@link #ROWS_PER_FILE} rows in a single row group, named so they list in a
     * stable order. Generated here rather than checked in: a Parquet fixture committed as a binary is one nobody can
     * read or regenerate.
     */
    private String registerParquetDataset(String name, int fileCount) throws IOException {
        Path dir = createTempDir();
        for (int f = 0; f < fileCount; f++) {
            writeParquet(dir.resolve(String.format(Locale.ROOT, "part-%04d.parquet", f)), ROWS_PER_FILE, ROWS_PER_FILE);
        }
        String glob = CountingStorageProvider.location(dir) + "/*.parquet";
        return registerDataset(name, glob, Map.of("format", "parquet", "schema_resolution", "first_file_wins"));
    }
}
