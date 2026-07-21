/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
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

/**
 * Regression coverage for an NDJSON file whose final record is not terminated by a newline.
 *
 * <p>At {@code parsing_parallelism=1} a whole-file read takes the single-threaded fallback in
 * {@link org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceOperatorFactory}. A genuine
 * whole-file {@code FileSplit} carries none of the split-partitioning markers, so the fallback must
 * recognize it as owning the file's true EOF and build a
 * {@link org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext} with {@code lastSplit=true}.
 * With {@code lastSplit=false} the reader instead applies the split-continuation trim, dropping
 * everything after the last {@code '\n'}; that trim is correct only for a genuine non-final split
 * (whose tail the next split re-reads). On a whole-file read the trim drops an un-terminated final
 * record from a column projection. A {@code COUNT(*)} over the same data shares this same fallback, but
 * derives its count from the harvested whole-file stats rather than from the trimmed materialised pages,
 * so it still counts the record. Projection (short by one) and count (correct) then disagree and the last
 * row is silently lost. These tests assert that a projection reads every record and that it equals
 * {@code COUNT(*)}.
 */
public class ExternalNdJsonUnterminatedFinalRecordIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(NdJsonDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        // Pin the single-threaded whole-file fallback: that is where the last-split flag is derived and
        // where the trailing-record trim is applied.
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 1).build());
    }

    /**
     * Pin every query to one coordinator so the reconciled schema/stats cache the cold scan enriches
     * is the same one a follow-up query reads (see {@link ExternalNdJsonAggregatePushdownIT#run}).
     */
    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request, TimeValue timeout) {
        try {
            return client(internalCluster().getMasterName()).execute(EsqlQueryAction.INSTANCE, request).actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    public void testProjectionReadsUnterminatedFinalRecord() throws Exception {
        assertProjectionReadsAllRows(3, false);
    }

    public void testProjectionReadsUnterminatedFinalRecordCrlf() throws Exception {
        assertProjectionReadsAllRows(3, false, "\r\n");
    }

    public void testProjectionReadsSingleUnterminatedRecord() throws Exception {
        assertProjectionReadsAllRows(1, false);
    }

    public void testTrailingNewlineBaselineUnaffected() throws Exception {
        assertProjectionReadsAllRows(3, true);
    }

    public void testCountMatchesProjectionForUnterminatedFinalRecord() throws Exception {
        assertCountMatchesProjection(3, "\n");
    }

    public void testCountMatchesProjectionForUnterminatedFinalRecordCrlf() throws Exception {
        assertCountMatchesProjection(3, "\r\n");
    }

    public void testCountMatchesProjectionForSingleUnterminatedRecord() throws Exception {
        assertCountMatchesProjection(1, "\n");
    }

    private void assertCountMatchesProjection(int totalRows, String lineTerminator) throws Exception {
        // Two distinct files with identical content. The external-stats cache is keyed by (path, mtime,
        // config), so separate paths give independent cache keys. A genuine whole-file read is cacheable,
        // so pointing both queries at one file would let COUNT(*) warm-fold off the projection scan;
        // distinct files keep the COUNT(*) read cold.
        Path projFile = writeNdJsonFile(totalRows, false, lineTerminator);
        Path countFile = writeNdJsonFile(totalRows, false, lineTerminator);
        try {
            String projDataset = registerDataset("ndjson_proj", StoragePath.fileUri(projFile), Map.of());
            long projectedRows;
            try (var resp = run(syncEsqlQueryRequest("FROM " + projDataset + " | KEEP id"))) {
                projectedRows = getValuesList(resp).size();
            }

            String countDataset = registerDataset("ndjson_count", StoragePath.fileUri(countFile), Map.of());
            long count;
            try (var resp = run(syncEsqlQueryRequest("FROM " + countDataset + " | STATS c = COUNT(*)"))) {
                count = ((Number) getValuesList(resp).get(0).get(0)).longValue();
            }

            assertThat("COUNT(*) must see every record", count, equalTo((long) totalRows));
            assertThat("projection must materialise every record COUNT(*) sees", projectedRows, equalTo(count));
        } finally {
            Files.deleteIfExists(projFile);
            Files.deleteIfExists(countFile);
        }
    }

    private void assertProjectionReadsAllRows(int totalRows, boolean trailingNewline) throws Exception {
        assertProjectionReadsAllRows(totalRows, trailingNewline, "\n");
    }

    private void assertProjectionReadsAllRows(int totalRows, boolean trailingNewline, String lineTerminator) throws Exception {
        Path file = writeNdJsonFile(totalRows, trailingNewline, lineTerminator);
        try {
            String dataset = registerDataset("ndjson_unterminated", StoragePath.fileUri(file), Map.of());
            try (var resp = run(syncEsqlQueryRequest("FROM " + dataset + " | KEEP id | SORT id"))) {
                List<List<Object>> rows = getValuesList(resp);
                assertThat(rows.size(), equalTo(totalRows));
                List<Long> ids = new ArrayList<>();
                for (List<Object> row : rows) {
                    ids.add(((Number) row.get(0)).longValue());
                }
                List<Long> expected = new ArrayList<>();
                for (int i = 0; i < totalRows; i++) {
                    expected.add((long) i);
                }
                assertThat(ids, equalTo(expected));
            }
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private Path writeNdJsonFile(int rowCount, boolean trailingNewline, String lineTerminator) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rowCount; i++) {
            sb.append("{\"id\":").append(i).append(",\"name\":\"row_").append(i).append("\",\"value\":").append(i * 10).append("}");
            if (i < rowCount - 1 || trailingNewline) {
                sb.append(lineTerminator);
            }
        }
        Path tempFile = createTempDir().resolve("unterminated_final_record.ndjson");
        Files.writeString(tempFile, sb.toString(), StandardCharsets.UTF_8);
        return tempFile;
    }
}
