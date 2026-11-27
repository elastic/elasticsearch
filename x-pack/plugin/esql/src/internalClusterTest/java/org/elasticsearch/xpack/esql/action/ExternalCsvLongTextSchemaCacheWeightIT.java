/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheService;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheTestAccess;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * End-to-end guard over CSV files whose text cells are a megabyte wide. A read withholds an extremum that
 * wide, so the rest of the file's measurements are small enough to cache: the retained weight stays inside
 * its slice of {@code esql.external.cache.size}, every file keeps a record, a warm aggregate over the
 * withheld column still answers correctly by scanning, and a narrow column in the same record still serves.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class ExternalCsvLongTextSchemaCacheWeightIT extends AbstractExternalDataSourceIT {

    private static final int FILE_COUNT = 8;
    private static final int VALUE_CHARS = 1_000_000;
    /** The one file whose {@code note} is narrow enough to publish. Neither the minimum nor the maximum. */
    private static final int NARROW_NOTE_FILE = 3;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).put("esql.external.cache.size", "2mb").build();
    }

    public void testWideExtremaAreWithheldSoTheFileStaysCachedAndWarmAnswersStayCorrect() throws Exception {
        Path dir = createTempDir();
        String pad = "x".repeat(VALUE_CHARS);
        // Distinct prefixes so MIN/MAX are well-defined (lexicographic) and the query result is checkable.
        String expectedMin = "a0-" + pad;
        String expectedMax = "a" + (FILE_COUNT - 1) + "-" + pad;
        // A narrow text column beside the wide one. Without it the fixture has only the wide `note` and the
        // numeric `id`, so nothing here would exercise a narrow TEXT extremum surviving the cap.
        String expectedTagMin = "tag-0";
        String expectedTagMax = "tag-" + (FILE_COUNT - 1);
        // One file's note is narrow, and it is neither end of the column. With every file withholding both
        // ends the fold has no candidate, so a fold that SKIPPED a withheld file rather than marking its
        // column unservable would answer identically; with a candidate present, that fold serves it.
        String narrowNote = "a" + NARROW_NOTE_FILE + "-short";
        for (int i = 0; i < FILE_COUNT; i++) {
            Path file = dir.resolve("part" + i + ".csv");
            String note = i == NARROW_NOTE_FILE ? narrowNote : "a" + i + "-" + pad;
            Files.writeString(file, "id,tag,note\n" + i + ",tag-" + i + "," + note + "\n");
        }
        String dataset = registerLocalFileDataset("wide_text", StoragePath.fileUri(dir) + "/*.csv", Map.of("format", "csv"));
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS MIN(note), MAX(note)"), TimeValue.timeValueMinutes(5))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            assertThat(rows.get(0).get(0), equalTo(expectedMin));
            assertThat(rows.get(0).get(1), equalTo(expectedMax));
        }

        ExternalSourceCacheService cacheService = internalCluster().getInstance(PlanExecutor.class, internalCluster().getMasterName())
            .cacheService();
        Map<String, Object> stats = cacheService.usageStats();
        long schemaBudget = (Long) stats.get("schema_budget_bytes");
        long retained = ExternalSourceCacheTestAccess.retainedSchemaWeightBytes(cacheService);
        assertThat(
            "schema cache retained weight [" + retained + "] must stay inside schema budget [" + schemaBudget + "]",
            retained,
            lessThanOrEqualTo(schemaBudget)
        );
        // A read does not publish a text extremum wider than a kilobyte, so what reaches the statistics store
        // for these files is counts and no extrema: small, and therefore retained. Every file keeps its
        // measurements.
        assertThat(
            "every file must retain its measurements now that the oversized extrema are not in them",
            (Integer) stats.get("statistics_cache.count"),
            equalTo(FILE_COUNT)
        );
        assertThat(
            "and the schema records they were measured against must be retained too",
            (Integer) stats.get("schema_cache.count"),
            greaterThan(0)
        );
        // A megabyte value outweighs the per-entry ceiling, so every file having a record is the evidence
        // that none carries one.
        long retainedStatistics = ExternalSourceCacheTestAccess.retainedStatisticsWeightBytes(cacheService);
        long statisticsBudget = (Long) stats.get("statistics_budget_bytes");
        assertThat("statistics retained weight must stay inside its own budget", retainedStatistics, lessThanOrEqualTo(statisticsBudget));

        // The counts are what withholding is traded for: a record that keeps them answers COUNT without a
        // scan, which is the whole gain over caching nothing for the file. MIN/MAX below would pass even if
        // the counts had gone with the extrema, so they are asserted here on their own.
        try (
            var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS COUNT(*), COUNT(note)"), TimeValue.timeValueMinutes(5))
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            assertThat("every row of every file", rows.get(0).get(0), equalTo((long) FILE_COUNT));
            assertThat("and every one has a note", rows.get(0).get(1), equalTo((long) FILE_COUNT));
            assertThat(
                "a withheld extremum leaves the counts servable, so neither count reads a file",
                response.documentsFound(),
                equalTo(0L)
            );
        }

        // Withholding the extrema creates a state that did not exist before: a retained record for a column
        // that has no MIN and no MAX. A warm query over that column must fall back to a scan and return the
        // file's true extrema, not serve nothing from the record it found. The fold is read to safe-miss on an
        // absent extremum and a unit test holds that fold, but this is the only place it runs end to end.
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS MIN(note), MAX(note)"), TimeValue.timeValueMinutes(5))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            assertThat(
                "a warm MIN over a withheld extremum must still be the file's true minimum",
                rows.get(0).get(0),
                equalTo(expectedMin)
            );
            assertThat(
                "a warm MAX over a withheld extremum must still be the file's true maximum",
                rows.get(0).get(1),
                equalTo(expectedMax)
            );
            assertThat(
                "the column is unservable, so the answer comes from a scan rather than the record",
                response.documentsFound(),
                equalTo((long) FILE_COUNT)
            );
        }

        // Ask for both columns so one record holds `tag` published and `note` withheld; a read harvests only
        // the columns it projects (`esql.external.cache.stripe.columns` defaults to `projected`). This read
        // scans because `tag` is not yet harvested, so it says nothing about `note`.
        try (
            var response = run(
                syncEsqlQueryRequest("FROM " + dataset + " | STATS MIN(tag), MAX(tag), MIN(note), MAX(note)"),
                TimeValue.timeValueMinutes(5)
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            assertThat(rows.get(0).get(0), equalTo(expectedTagMin));
            assertThat(rows.get(0).get(1), equalTo(expectedTagMax));
            assertThat(rows.get(0).get(2), equalTo(expectedMin));
            assertThat(rows.get(0).get(3), equalTo(expectedMax));
            assertThat(
                "the first read that projects `tag` scans every row, which is what puts both columns in one record",
                response.documentsFound(),
                equalTo((long) FILE_COUNT)
            );
        }
        // Now the narrow column alone, against the record that read wrote. The values would not tell a served
        // answer from a scanned one -- both are the file's true extrema -- so the row count is the gate.
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS MIN(tag), MAX(tag)"), TimeValue.timeValueMinutes(5))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            assertThat("a narrow text column beside a withheld one keeps its minimum", rows.get(0).get(0), equalTo(expectedTagMin));
            assertThat("a narrow text column beside a withheld one keeps its maximum", rows.get(0).get(1), equalTo(expectedTagMax));
            assertThat("and is answered from statistics rather than scanned", response.documentsFound(), equalTo(0L));
        }

        // The combined query again, now that the query above has proved `tag` is servable from the record.
        // The first run of it scanned for two reasons at once -- `tag` unharvested, `note` withheld -- so it
        // could not say which. Here only `note` is left, so a scan is attributable to the withheld extremum.
        try (
            var response = run(
                syncEsqlQueryRequest("FROM " + dataset + " | STATS MIN(tag), MAX(tag), MIN(note), MAX(note)"),
                TimeValue.timeValueMinutes(5)
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            assertThat(rows.get(0).get(0), equalTo(expectedTagMin));
            assertThat(rows.get(0).get(1), equalTo(expectedTagMax));
            assertThat(rows.get(0).get(2), equalTo(expectedMin));
            assertThat(rows.get(0).get(3), equalTo(expectedMax));
            assertThat(
                "a servable `tag` beside a withheld `note` still scans, so the withheld end is what costs the read",
                response.documentsFound(),
                equalTo((long) FILE_COUNT)
            );
        }
    }
}
