/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * A Hive partition value can be null: the sentinel directory {@code region=__HIVE_DEFAULT_PARTITION__/}
 * decodes to a null partition value ({@link org.elasticsearch.xpack.esql.datasources.HivePartitionDetector#HIVE_DEFAULT_PARTITION}).
 * This guards two things touched by the #153503 hive partition-identity convergence:
 * <ul>
 *   <li>The partition column pinned eager under {@code SORT} (the fix) must materialise a null constant block
 *       through {@code VirtualColumnIterator}, not crash or mis-attach — the null cell of the TopN materialisation
 *       matrix, which the non-null hive ITs never exercised.</li>
 *   <li>{@code WHERE region IS NULL} / {@code IS NOT NULL} over the path-derived column must be held in the
 *       {@code FilterExec} and evaluated against the injected constants on every node.</li>
 * </ul>
 * Forced distributed ({@code round_robin}, ≥2 data nodes) so the data-node path (UNRESOLVED fileList) is the one
 * exercised. Five data columns keep the deferred count ≥ {@code DEFERRED_COLUMN_MIN} so the TopN rule actually fires.
 */
public class ExternalParquetHivePartitionNullValueIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    /** region=east has ids 0,1,2; region=__HIVE_DEFAULT_PARTITION__ (null) has ids 3,4. */
    private String dataset(String name) throws Exception {
        Path root = createTempDir().resolve(name);
        writeFiveColParquet(root.resolve("region=east"), List.of(0, 1, 2));
        writeFiveColParquet(root.resolve("region=__HIVE_DEFAULT_PARTITION__"), List.of(3, 4));
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.parquet";
        return registerDataset(name, glob, Map.of("hive_partitioning", true));
    }

    public void testSortMaterializesNullPartitionValue() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String ds = dataset("hive_null_sort");
        try (var response = runDistributed("FROM " + ds + " | SORT id")) {
            List<List<Object>> rows = getValuesList(response);
            assertThat("all five rows return", rows.size(), equalTo(5));
            int region = columnIndex(response, "region");
            int id = columnIndex(response, "id");
            for (List<Object> row : rows) {
                long rid = ((Number) row.get(id)).longValue();
                if (rid <= 2) {
                    assertThat("non-sentinel partition materialises its value", row.get(region), equalTo("east"));
                } else {
                    assertThat("sentinel partition materialises a null under SORT", row.get(region), nullValue());
                }
            }
        }
    }

    public void testWhereRegionIsNull() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String ds = dataset("hive_null_isnull");
        try (var response = runDistributed("FROM " + ds + " | WHERE region IS NULL | SORT id")) {
            List<List<Object>> rows = getValuesList(response);
            assertThat("only the two sentinel-partition rows match IS NULL", rows.size(), equalTo(2));
            int region = columnIndex(response, "region");
            for (List<Object> row : rows) {
                assertThat(row.get(region), nullValue());
            }
        }
    }

    public void testWhereRegionIsNotNull() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String ds = dataset("hive_null_isnotnull");
        try (var response = runDistributed("FROM " + ds + " | WHERE region IS NOT NULL | SORT id")) {
            List<List<Object>> rows = getValuesList(response);
            assertThat("only the three region=east rows match IS NOT NULL", rows.size(), equalTo(3));
            int region = columnIndex(response, "region");
            for (List<Object> row : rows) {
                assertThat(row.get(region), equalTo("east"));
            }
        }
    }

    private org.elasticsearch.xpack.esql.action.EsqlQueryResponse runDistributed(String query) {
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build());
        var request = syncEsqlQueryRequest(query);
        request.pragmas(pragmas);
        request.acceptedPragmaRisks(true); // pragmas are rejected on non-snapshot builds without this
        request.profile(true);
        var response = run(request);
        assertThat("external scan must distribute across >= 2 data nodes", externalScanNodeNames(response).size(), greaterThanOrEqualTo(2));
        return response;
    }

    private static int columnIndex(EsqlQueryResponse response, String name) {
        var columns = response.columns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(name)) {
                return i;
            }
        }
        throw new AssertionError("column [" + name + "] not found in " + columns);
    }

    private static Path writeFiveColParquet(Path dir, List<Integer> ids) throws IOException {
        Files.createDirectories(dir);
        return writeParquet(
            dir.resolve("data.parquet"),
            "message t { required int64 id; required int32 w; required int32 x; required int32 y; required int32 z; }",
            ids.size(),
            1024,
            (g, i) -> {
                int id = ids.get(i);
                g.add("id", (long) id);
                g.add("w", id * 10);
                g.add("x", id * 100);
                g.add("y", id * 1000);
                g.add("z", id * 10000);
            }
        );
    }
}
