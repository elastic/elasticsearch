/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Cross-format guard for the boolean hive-partition cast: a capitalized boolean partition folder
 * ({@code flag=True/}, {@code flag=False/}, the casing common data writers emit) must type the partition
 * column as {@code BOOLEAN} and be queryable rather than failing the query during partition-value casting.
 *
 * <p>Type inference ({@code HivePartitionDetector.tryAllBoolean}) accepts {@code true}/{@code false}
 * case-insensitively, and the cast agrees: any casing of {@code true}/{@code false} folder value casts to the
 * boolean. All four format readers funnel through the same detector, so each concrete leg (CSV, TSV, NDJSON,
 * Parquet) exercises the same detector path with its own reader.
 *
 * <p>Runs forced-distributed ({@code round_robin}, {@code >= 2} data nodes, {@code profile(true)}) so the
 * data-node partition-attachment leg is exercised, mirroring {@link ExternalParquetHivePartitionNullValueIT}.
 * Subclasses supply the per-format data-column writer; the {@code flag} column is path-derived.
 */
public abstract class AbstractHivePartitionBooleanCastIT extends AbstractExternalDataSourceIT {

    /** File extension for this format (also the reader selector, e.g. {@code "csv"}, {@code "parquet"}). */
    protected abstract String extension();

    /**
     * Writes the file payload for {@code ids} to {@code file}, always including an {@code id: int} data column
     * (subclasses may add more columns). The boolean {@code flag} column is path-derived (the enclosing
     * {@code flag=True/}/{@code flag=False/} directory), so the payload never carries it.
     */
    protected abstract void writeIdRows(Path file, List<Integer> ids) throws IOException;

    /** {@code flag=True/} has ids 0,1,2; {@code flag=False/} has ids 3,4. */
    private String registerBooleanPartitionedDataset(String name) throws IOException {
        Path root = createTempDir().resolve(name);
        Path truePart = root.resolve("flag=True");
        Path falsePart = root.resolve("flag=False");
        Files.createDirectories(truePart);
        Files.createDirectories(falsePart);
        writeIdRows(truePart.resolve("data." + extension()), List.of(0, 1, 2));
        writeIdRows(falsePart.resolve("data." + extension()), List.of(3, 4));
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*." + extension();
        return registerDataset(name, glob, Map.of("hive_partitioning", true));
    }

    /**
     * {@code FROM ds} over capitalized boolean folders returns every row, types the {@code flag} column as
     * {@code BOOLEAN}, and carries each folder's boolean value on its rows.
     */
    public void testCapitalizedBooleanPartitionFolderQueryable() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String dataset = registerBooleanPartitionedDataset("hive_bool_" + extension());

        QueryResult result = runDistributed("FROM " + dataset + " | SORT id");
        assertThat("all five rows return across both boolean partitions", result.rows().size(), equalTo(5));
        assertThat("path-derived boolean partition types as BOOLEAN", result.type("flag"), equalTo(DataType.BOOLEAN));

        int flag = result.index("flag");
        int id = result.index("id");
        for (List<Object> row : result.rows()) {
            int rowId = ((Number) row.get(id)).intValue();
            boolean expected = rowId <= 2;
            assertThat("id " + rowId + " carries its folder's boolean value", row.get(flag), equalTo(expected));
        }
    }

    /**
     * Typed boolean equality over the path-derived column: {@code WHERE flag == false} must keep exactly the two
     * {@code flag=False/} rows, proving the partition value is a real boolean rather than the string {@code "False"}.
     */
    public void testBooleanPartitionFilter() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String dataset = registerBooleanPartitionedDataset("hive_bool_filter_" + extension());

        QueryResult result = runDistributed("FROM " + dataset + " | WHERE flag == false | SORT id");
        assertThat("only the two flag=False rows match", result.rows().size(), equalTo(2));
        int flag = result.index("flag");
        int id = result.index("id");
        List<Integer> ids = new ArrayList<>();
        for (List<Object> row : result.rows()) {
            assertThat(row.get(flag), equalTo(false));
            ids.add(((Number) row.get(id)).intValue());
        }
        assertThat("the flag=False rows are exactly ids 3 and 4", ids, equalTo(List.of(3, 4)));
    }

    /**
     * Runs {@code query} forced-distributed and extracts rows + columns INSIDE the try-with-resources, so the
     * refcounted response is always closed (a leaked response fires a misleading second failure in teardown).
     */
    private QueryResult runDistributed(String query) {
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build());
        var request = syncEsqlQueryRequest(query);
        request.pragmas(pragmas);
        request.acceptedPragmaRisks(true); // pragmas are rejected on non-snapshot builds without this
        request.profile(true);
        try (var response = run(request)) {
            assertThat("external scan must run on >= 1 data node", externalScanNodeNames(response).size(), greaterThanOrEqualTo(1));
            return new QueryResult(
                getValuesList(response),
                response.columns().stream().map(ColumnInfoImpl::name).toList(),
                response.columns().stream().map(ColumnInfoImpl::type).toList()
            );
        }
    }

    private record QueryResult(List<List<Object>> rows, List<String> columns, List<DataType> types) {
        int index(String name) {
            int i = columns.indexOf(name);
            if (i < 0) {
                throw new AssertionError("column [" + name + "] not found in " + columns);
            }
            return i;
        }

        DataType type(String name) {
            return types.get(index(name));
        }
    }
}
