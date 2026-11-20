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

/**
 * That {@code esql.external.cache.stats.max_extremum_size} reaches the read that applies it. Every link from
 * the node's settings to the predicate — registration, {@code FileSourceFactory}, the operator factory, the
 * parsing coordinator, the reader — yields the 1 KB default when it is dropped, so a suite that only ever
 * passes the default cannot tell a wired setting from an unwired one. These {@code tag} values are five bytes
 * and the setting here is four, so they publish under the default and are withheld under this node's setting:
 * the sibling {@code ExternalCsvLongTextSchemaCacheWeightIT} asserts the same column is served from statistics
 * with no rows read, and here it has to scan.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class ExternalCsvExtremumWidthSettingIT extends AbstractExternalDataSourceIT {

    private static final int FILE_COUNT = 4;

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

        // A read harvests only what it projects, so this one puts both columns in the record and scans for both.
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

        // Five bytes against a four-byte setting. Under the 1 KB default this query is answered from statistics
        // with no rows read, so a scan here is the setting having travelled the whole way.
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
     * The same, over gzipped files. A compressed object has no known decompressed length, so it is read as a
     * stream through {@code StreamingParallelParsingCoordinator} rather than split into ranges — a different
     * forward of the same setting out of the operator factory, and one the plain-CSV case above never reaches.
     */
    public void testTheSettingReachesTheStreamingReadOfACompressedFile() throws Exception {
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
                syncEsqlQueryRequest("FROM " + dataset + " | STATS MIN(tag), MAX(tag), MIN(id), MAX(id)"),
                TimeValue.timeValueMinutes(5)
            )
        ) {
            assertThat("the harvesting read scans", response.documentsFound(), equalTo((long) FILE_COUNT));
        }

        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS MIN(id), MAX(id)"), TimeValue.timeValueMinutes(5))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.get(0).get(0), equalTo(0));
            assertThat("a numeric extremum is served warm, so the record is cached", response.documentsFound(), equalTo(0L));
        }

        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS MIN(tag), MAX(tag)"), TimeValue.timeValueMinutes(5))) {
            List<List<Object>> rows = getValuesList(response);
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
