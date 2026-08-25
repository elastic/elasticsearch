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
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

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
import static org.hamcrest.Matchers.notNullValue;

/**
 * End-to-end pin for the {@code mode: escaped} × {@code _rowPosition} double-decode: projecting a metadata
 * column must never change a data column's value.
 *
 * <p>{@code METADATA _id} forces the synthetic {@code _rowPosition} column, which routes the CSV reader onto its
 * record-materialized iterator. That iterator's {@code parseRecord} already un-escapes each field, so a second
 * un-escape in the batch consumer silently re-interpreted the escape sequences the first pass had produced —
 * a literal {@code \t} became a TAB, a literal {@code \N} became SQL null. The unit-level pin lives in
 * {@code CsvDirectBlockParityTests}; this IT proves the user-visible contract through the real planner, the
 * metadata-column injection and the response layer, which is where the corruption actually surfaced.
 *
 * <p>Single-node and {@code file://}-backed: the seam is deterministic and reproduces without S3 or multi-node
 * concurrency.
 */
public class ExternalCsvEscapedRowPositionIT extends AbstractExternalDataSourceIT {

    /** On disk row 1's {@code note} is {@code x\\ty} and row 2's is {@code \\N} (each backslash doubled). */
    private static final String FIXTURE = """
        id:integer\tnote:keyword
        1\tx\\\\ty
        2\t\\\\N
        """;

    /** After exactly one escaped-mode decode: {@code \\\\} un-escapes to one backslash, the rest is literal. */
    private static final List<String> EXPECTED_NOTES = List.of("x\\ty", "\\N");

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    /** Determinism over planner-regression diversity: this test pins a specific projection shape. */
    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    public void testRowPositionProjectionDoesNotChangeEscapedValues() throws Exception {
        Path file = createTempDir().resolve("notes.tsv");
        Files.writeString(file, FIXTURE);
        String dataset = registerDataset("escaped_notes", StoragePath.fileUri(file), Map.of("format", "tsv", "mode", "escaped"));

        assertThat(notes("FROM " + dataset + " | SORT id | KEEP note"), equalTo(EXPECTED_NOTES));

        // METADATA _id forces _rowPosition onto the reader's projection. The note values must not move.
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " METADATA _id | SORT id | KEEP note, _id"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            List<String> notes = new ArrayList<>();
            for (List<Object> row : rows) {
                notes.add((String) row.get(0));
                assertThat("_id must surface alongside the data column", row.get(1), notNullValue());
            }
            assertThat("projecting _rowPosition must not re-decode escape sequences", notes, equalTo(EXPECTED_NOTES));
        }

        // _file.record_ref is the other user-facing column that forces _rowPosition onto the reader, via the same
        // SyntheticColumns.rowPositionIndexInNames rail. Pin it too: it is named in the bug report as an affected
        // surface, and nothing else would catch it were the two rails ever to diverge.
        assertThat(notes("FROM " + dataset + " METADATA _file.record_ref | SORT id | KEEP note"), equalTo(EXPECTED_NOTES));
    }

    private List<String> notes(String query) throws Exception {
        try (var response = run(syncEsqlQueryRequest(query), TIMEOUT)) {
            List<String> notes = new ArrayList<>();
            for (List<Object> row : getValuesList(response)) {
                notes.add((String) row.get(0));
            }
            return notes;
        }
    }
}
