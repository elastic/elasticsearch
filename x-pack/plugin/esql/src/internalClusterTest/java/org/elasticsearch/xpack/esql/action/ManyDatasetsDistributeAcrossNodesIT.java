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
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * A query over many single-file datasets reads on more than one node.
 *
 * <p>Each dataset here yields exactly one split, and a producer holding one split gains no parallelism from being
 * distributed, so on its own it is cheapest read where it is planned. Every producer of a fan-in reaches that
 * conclusion separately, which for a query over many small datasets put every read on the coordinator at once and made
 * the coordinator's own capacity, rather than the transport hop it avoided, the thing limiting the query. The decision
 * therefore accounts for how many producers the fan-in has.
 *
 * <p>Deliberately runs with the default distribution strategy and no pragma: the {@code round_robin} pragma its sibling
 * tests use to force a fan-out would distribute regardless of producer count and report a green that says nothing about
 * the decision under test.
 */
public class ManyDatasetsDistributeAcrossNodesIT extends AbstractExternalDataSourceIT {

    private static final int DATASETS = 6;
    private static final int ROWS_PER_DATASET = 2;

    private final List<String> datasets = new ArrayList<>();

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    /** One file per dataset, so every producer discovers exactly one split. */
    private void registerSingleFileDatasets() throws Exception {
        datasets.clear();
        Path root = createTempDir();
        for (int ds = 0; ds < DATASETS; ds++) {
            StringBuilder csv = new StringBuilder("id:integer,name:keyword\n");
            for (int row = 0; row < ROWS_PER_DATASET; row++) {
                csv.append(ds * ROWS_PER_DATASET + row).append(",ds").append(ds).append('\n');
            }
            Path file = root.resolve("ds" + ds + ".csv");
            Files.writeString(file, csv.toString(), StandardCharsets.UTF_8);
            datasets.add(registerDataset("spread_ds_" + ds, StoragePath.fileUri(file), Map.of("format", "csv")));
        }
    }

    public void testSingleSplitProducersReadOnMoreThanOneNode() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        registerSingleFileDatasets();

        var request = syncEsqlQueryRequest("FROM " + String.join(", ", datasets) + " | STATS c = COUNT(*)");
        request.profile(true);
        try (EsqlQueryResponse response = run(request, TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(((Number) rows.getFirst().getFirst()).longValue(), equalTo((long) (DATASETS * ROWS_PER_DATASET)));

            assertThat(
                "every producer holds one split, so a per-producer decision keeps them all on the coordinator",
                externalScanNodeNames(response).size(),
                greaterThanOrEqualTo(2)
            );
        }
    }

    /**
     * The counterpart: one dataset with one split has no siblings to share the load with, so it is read in place
     * rather than paying a hop for no parallelism. Spreading is confined to the many-producer case.
     */
    public void testLoneSingleSplitDatasetStillReadsOnOneNode() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        registerSingleFileDatasets();

        var request = syncEsqlQueryRequest("FROM " + datasets.getFirst() + " | STATS c = COUNT(*)");
        request.profile(true);
        try (EsqlQueryResponse response = run(request, TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(((Number) rows.getFirst().getFirst()).longValue(), equalTo((long) ROWS_PER_DATASET));
            assertThat(externalScanNodeNames(response), hasSize(1));
        }
    }
}
