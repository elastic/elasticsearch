/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that filter adaptation for {@code union_by_name} does not treat a present but widened
 * Parquet column as absent. A predicate on such a column cannot be pushed in the narrower file's
 * type when its widening is not invertible; the retained engine filter must see every row instead.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ExternalParquetUnionByNameOrFilterIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    public void testOrAndNegatedAndFiltersOnIntWidenedToDouble() throws Exception {
        Path dir = createTempDir().resolve("ubn_or_filter");
        Files.createDirectories(dir);
        writeParquet(dir.resolve("a.parquet"), "message test { optional int32 x; required int64 y; }", 3, 1024, (group, row) -> {
            if (row < 2) {
                group.add("x", row + 1);
            }
            group.add("y", (long) row + 1);
        });
        writeParquet(dir.resolve("b.parquet"), "message test { optional double x; required int64 y; }", 2, 1024, (group, row) -> {
            if (row == 0) {
                group.add("x", 1.5);
            }
            group.add("y", (long) row + 1);
        });
        String dataset = registerDataset(
            "ubn_or_filter",
            StoragePath.fileUri(dir) + "/*.parquet",
            Map.of("schema_resolution", "union_by_name")
        );

        List<List<Object>> nonNullOrYEqualsTwo = List.of(List.of(1.0, 1L), List.of(1.5, 1L), List.of(2.0, 2L), Arrays.asList(null, 2L));
        assertRows(dataset, "x IS NOT NULL OR y == 2", nonNullOrYEqualsTwo);
        assertRows(dataset, "NOT (x IS NULL) OR y == 2", nonNullOrYEqualsTwo);

        List<List<Object>> xEqualsOneOrYEqualsTwo = List.of(List.of(1.0, 1L), List.of(2.0, 2L), Arrays.asList(null, 2L));
        assertRows(dataset, "x == 1.0 OR y == 2", xEqualsOneOrYEqualsTwo);
        assertRows(dataset, "x IN (1.0) OR y == 2", xEqualsOneOrYEqualsTwo);

        assertRows(
            dataset,
            "NOT (x IS NULL AND y == 2)",
            List.of(List.of(1.0, 1L), List.of(1.5, 1L), List.of(2.0, 2L), Arrays.asList(null, 3L))
        );
    }

    private void assertRows(String dataset, String filter, List<List<Object>> expected) throws Exception {
        String query = "FROM " + dataset + " | WHERE " + filter + " | KEEP x, y | SORT y, x";
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query), TIMEOUT)) {
            assertThat(filter, getValuesList(response), equalTo(expected));
        }
    }
}
