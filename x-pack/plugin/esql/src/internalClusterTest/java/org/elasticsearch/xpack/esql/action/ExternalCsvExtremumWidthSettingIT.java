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
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * That {@code esql.external.cache.stats.max_extremum_size} reaches the read that applies it: five-byte values
 * under a four-byte setting scan, where the default serves them.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class ExternalCsvExtremumWidthSettingIT extends AbstractExternalDataSourceIT {

    private static final int FILE_COUNT = 4;

    /** Above one, so a compressed read enters {@code StreamingParallelParsingCoordinator} whatever the runner's processor count. */
    private static final QueryPragmas PARALLEL_PARSE = new QueryPragmas(Settings.builder().put("external_parsing_parallelism", 2).build());

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("esql.external.cache.stats.max_extremum_size", "4b")
            .build();
    }

    public void testANarrowSettingWithholdsAnExtremumTheDefaultWouldPublish() throws Exception {
        Path dir = createTempDir();
        for (int i = 0; i < FILE_COUNT; i++) {
            Files.writeString(dir.resolve("part" + i + ".csv"), "id,tag\n" + i + ",tag-" + i + "\n");
        }
        String dataset = registerLocalFileDataset("narrow_width", StoragePath.fileUri(dir) + "/*.csv", Map.of("format", "csv"));

        String expectedTagMin = "tag-0";
        String expectedTagMax = "tag-" + (FILE_COUNT - 1);

        try (
            var response = run(
                syncEsqlQueryRequest("FROM " + dataset + " | STATS MIN(tag), MAX(tag), MIN(id), MAX(id)"),
                TimeValue.timeValueMinutes(5)
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            assertThat(rows.get(0).get(0), equalTo(expectedTagMin));
            assertThat(rows.get(0).get(1), equalTo(expectedTagMax));
            assertThat("the harvesting read scans", response.documentsFound(), equalTo((long) FILE_COUNT));
        }

        // The control, and it has to come first: a numeric extremum is not text and no width applies to it, so
        // this serving proves the record was retained and the warm path works for this dataset. Without it, the
        // scan below would also pass if nothing had been cached at all.
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS MIN(id), MAX(id)"), TimeValue.timeValueMinutes(5))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            assertThat(rows.get(0).get(0), equalTo(0));
            assertThat(rows.get(0).get(1), equalTo(FILE_COUNT - 1));
            assertThat("a numeric extremum is served warm, so the record is cached", response.documentsFound(), equalTo(0L));
        }

        // Five bytes against a four-byte setting: the default serves this from statistics, so a scan is the
        // setting having travelled the whole way.
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS MIN(tag), MAX(tag)"), TimeValue.timeValueMinutes(5))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            assertThat("the answer is still correct, by scanning", rows.get(0).get(0), equalTo(expectedTagMin));
            assertThat("the answer is still correct, by scanning", rows.get(0).get(1), equalTo(expectedTagMax));
            assertThat(
                "a five-byte extremum is withheld under a four-byte setting, so this scans where the default serves",
                response.documentsFound(),
                equalTo((long) FILE_COUNT)
            );
        }
    }

    /**
     * The same over a file big enough to segment, which routes the harvest through
     * {@code ParallelParsingCoordinator}'s per-segment read. Two things make this test discriminate, and both
     * are easy to get wrong: a file under {@code minimumSegmentSize() * 2} takes the single-segment fallback
     * and tests nothing the plain case does not, and with more than one file a segmented file that wrongly
     * published would still be poisoned out by its neighbours' withheld ends, so the scan would prove nothing.
     * One file over the threshold leaves the published-or-withheld state of that one read visible.
     */
    public void testTheSettingReachesTheSegmentedReadOfALargeFile() throws Exception {
        assumeTrue("the segmented read is reached by pinning external_parsing_parallelism, a snapshot-only pragma", canUseQueryPragmas());
        Path dir = createTempDir();
        // Over 2 MiB so computeSegments runs rather than the fileLength < minSegment * 2 fallback. The padding
        // rides in a column the queries never project, so `tag` stays five bytes against the four-byte setting.
        StringBuilder csv = new StringBuilder("id,tag,pad\n");
        String pad = "p".repeat(512);
        for (int row = 0; csv.length() < 3 * 1024 * 1024; row++) {
            csv.append(row).append(",tag-").append(row % 10).append(',').append(pad).append('\n');
        }
        Files.writeString(dir.resolve("big.csv"), csv.toString());
        String dataset = registerLocalFileDataset("narrow_width_segmented", StoragePath.fileUri(dir) + "/*.csv", Map.of("format", "csv"));

        long rowCount;
        try (
            var response = run(
                syncEsqlQueryRequest("FROM " + dataset + " | STATS MIN(tag), MAX(tag), MIN(id), MAX(id)").pragmas(PARALLEL_PARSE),
                TimeValue.timeValueMinutes(5)
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            assertThat("the harvesting read scans", response.documentsFound(), greaterThan(0L));
            rowCount = response.documentsFound();
        }

        // The control, for the reason it is the control in the test above.
        try (
            var response = run(
                syncEsqlQueryRequest("FROM " + dataset + " | STATS MIN(id), MAX(id)").pragmas(PARALLEL_PARSE),
                TimeValue.timeValueMinutes(5)
            )
        ) {
            assertThat("a numeric extremum is served warm, so the record is cached", response.documentsFound(), equalTo(0L));
        }

        try (
            var response = run(
                syncEsqlQueryRequest("FROM " + dataset + " | STATS MIN(tag), MAX(tag)").pragmas(PARALLEL_PARSE),
                TimeValue.timeValueMinutes(5)
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            assertThat("the answer is still correct, by scanning", rows.get(0).get(0), equalTo("tag-0"));
            assertThat("the answer is still correct, by scanning", rows.get(0).get(1), equalTo("tag-9"));
            assertThat(
                "the per-segment read withholds it too, so this scans where a published extremum would serve",
                response.documentsFound(),
                equalTo(rowCount)
            );
        }
    }

    /**
     * The same over gzipped files, which stream through {@code StreamingParallelParsingCoordinator}. The
     * pragma forces that path on a one-processor runner.
     */
    public void testTheSettingReachesTheStreamingReadOfACompressedFile() throws Exception {
        assumeTrue("the streaming read is reached by pinning external_parsing_parallelism, a snapshot-only pragma", canUseQueryPragmas());
        Path dir = createTempDir();
        for (int i = 0; i < FILE_COUNT; i++) {
            try (OutputStream gz = new GZIPOutputStream(Files.newOutputStream(dir.resolve("part" + i + ".csv.gz")))) {
                gz.write(("id,tag\n" + i + ",tag-" + i + "\n").getBytes(StandardCharsets.UTF_8));
            }
        }
        String dataset = registerLocalFileDataset("narrow_width_gz", StoragePath.fileUri(dir) + "/*.csv.gz", Map.of("format", "csv"));

        String expectedTagMin = "tag-0";
        String expectedTagMax = "tag-" + (FILE_COUNT - 1);

        try (
            var response = run(
                syncEsqlQueryRequest("FROM " + dataset + " | STATS MIN(tag), MAX(tag), MIN(id), MAX(id)").pragmas(PARALLEL_PARSE),
                TimeValue.timeValueMinutes(5)
            )
        ) {
            assertThat("the harvesting read scans", response.documentsFound(), equalTo((long) FILE_COUNT));
        }

        try (
            var response = run(
                syncEsqlQueryRequest("FROM " + dataset + " | STATS MIN(id), MAX(id)").pragmas(PARALLEL_PARSE),
                TimeValue.timeValueMinutes(5)
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            assertThat(rows.get(0).get(0), equalTo(0));
            assertThat("a numeric extremum is served warm, so the record is cached", response.documentsFound(), equalTo(0L));
        }

        try (
            var response = run(
                syncEsqlQueryRequest("FROM " + dataset + " | STATS MIN(tag), MAX(tag)").pragmas(PARALLEL_PARSE),
                TimeValue.timeValueMinutes(5)
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            assertThat("the answer is still correct, by scanning", rows.get(0).get(0), equalTo(expectedTagMin));
            assertThat("the answer is still correct, by scanning", rows.get(0).get(1), equalTo(expectedTagMax));
            assertThat(
                "the streaming read withholds it too, so this scans where the default serves",
                response.documentsFound(),
                equalTo((long) FILE_COUNT)
            );
        }
    }
}
