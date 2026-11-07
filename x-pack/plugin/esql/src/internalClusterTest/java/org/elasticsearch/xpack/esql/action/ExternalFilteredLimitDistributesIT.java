/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * A {@code WHERE ... | LIMIT} over a many-file dataset reads on more than one node.
 * <p>
 * Every ES|QL query without STATS or SORT carries a limit (the implicit one included). An unfiltered
 * limit stops after about {@code LIMIT} rows, so reading it on the coordinator is cheapest. A filter
 * under the limit can make the scan read most of the dataset before the limit fills, and doing that on
 * one node leaves the other data nodes idle.
 * <p>
 * Runs with the default distribution strategy and no pragma, so it checks what Adaptive decides.
 */
public class ExternalFilteredLimitDistributesIT extends AbstractExternalDataSourceIT {

    /** Enough files that the split count stays above the number of data nodes, and under the coalescing threshold. */
    private static final int FILES = 16;
    private static final int ROWS_PER_FILE = 10;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    private String registerManyFileDataset(String name) throws Exception {
        Path dir = createTempDir();
        Files.createDirectories(dir);
        for (int f = 0; f < FILES; f++) {
            int offset = f * ROWS_PER_FILE;
            writeParquet(
                dir.resolve("part" + f + ".parquet"),
                "message test { required int32 id; }",
                ROWS_PER_FILE,
                1024,
                (g, i) -> g.add("id", offset + i)
            );
        }
        return registerDataset(name, StoragePath.fileUri(dir) + "/*.parquet", Map.of());
    }

    /**
     * The filter matches no row, so the limit never fills and every file is read. Parquet statistics
     * can't prune a modulo, so the splits reach the distribution strategy intact.
     */
    public void testFilteredLimitReadsOnMoreThanOneNode() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String dataset = registerManyFileDataset("filtered_limit_ds");

        var request = syncEsqlQueryRequest("FROM " + dataset + " | WHERE id % 1000 == 999 | LIMIT 10");
        request.profile(true);
        try (EsqlQueryResponse response = run(request, TIMEOUT)) {
            assertThat(getValuesList(response), empty());
            assertThat(externalScanNodeNames(response).size(), greaterThanOrEqualTo(2));
        }
    }

    public void testUnfilteredLimitStillReadsOnOneNode() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String dataset = registerManyFileDataset("unfiltered_limit_ds");

        var request = syncEsqlQueryRequest("FROM " + dataset + " | LIMIT 10");
        request.profile(true);
        try (EsqlQueryResponse response = run(request, TIMEOUT)) {
            assertThat(getValuesList(response), hasSize(10));
            assertThat(externalScanNodeNames(response), hasSize(1));
        }
    }
}
