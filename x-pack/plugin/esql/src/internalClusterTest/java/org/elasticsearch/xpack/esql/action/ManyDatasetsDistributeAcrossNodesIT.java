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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * A query over many single-file datasets reads on more than one node.
 * <p>
 * Each dataset here yields exactly one split. A lone one-split read stays on the coordinator: a hop
 * buys no parallelism. The same rule applied per leaf of a many-dataset UNION would park every file
 * on the coordinator at once. Sibling-aware Adaptive therefore hops a one-split UNION leaf.
 * <p>
 * Runs with the default distribution strategy and no pragma: a forced {@code round_robin} would
 * distribute regardless of sibling count and report a green that says nothing about Adaptive.
 */
public class ManyDatasetsDistributeAcrossNodesIT extends AbstractExternalDataSourceIT {

    private static final int DATASETS = 4;
    private static final int ROWS_PER_DATASET = 2;

    private final List<String> datasets = new ArrayList<>();

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    /** One file per dataset, so every producer discovers exactly one split. */
    private void registerSingleFileDatasets() throws Exception {
        datasets.clear();
        Path root = createTempDir();
        for (int ds = 0; ds < DATASETS; ds++) {
            Path file = writeSingleColumnIdParquet(root.resolve("ds" + ds), ROWS_PER_DATASET);
            datasets.add(registerDataset("spread_ds_" + ds, StoragePath.fileUri(file), Map.of()));
        }
    }

    public void testSingleSplitProducersReadOnMoreThanOneNode() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        registerSingleFileDatasets();

        var request = syncEsqlQueryRequest("FROM " + String.join(", ", datasets) + " | STATS s = SUM(id)");
        request.profile(true);
        try (EsqlQueryResponse response = run(request, TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(((Number) rows.getFirst().getFirst()).longValue(), equalTo((long) DATASETS));

            assertThat(externalScanNodeNames(response).size(), greaterThanOrEqualTo(2));
        }
    }

    /**
     * One dataset with one split has no siblings to share the load with, so it is read in place
     * rather than paying a hop for no parallelism.
     */
    public void testLoneSingleSplitDatasetStillReadsOnOneNode() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        registerSingleFileDatasets();

        var request = syncEsqlQueryRequest("FROM " + datasets.getFirst() + " | STATS s = SUM(id)");
        request.profile(true);
        try (EsqlQueryResponse response = run(request, TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(((Number) rows.getFirst().getFirst()).longValue(), equalTo(1L));
            assertThat(externalScanNodeNames(response), hasSize(1));
        }
    }
}
