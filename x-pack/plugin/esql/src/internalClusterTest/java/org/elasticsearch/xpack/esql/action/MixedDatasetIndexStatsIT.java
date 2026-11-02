/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Reproduction scaffold for elastic/elasticsearch#164372: {@code STATS} over a dataset and an index in one
 * {@code FROM} fails with {@code illegal_state_exception: unexpected byte}. Multi-data-node on purpose — the
 * reported failure surfaces when deserializing an {@code ExchangeResponse} off the wire.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 2)
public class MixedDatasetIndexStatsIT extends AbstractExternalDataSourceIT {

    private static final int DATASET_ROWS = 100;
    private static final int INDEX_DOCS = 10;

    private static final String INDEX = "cloudtrail_recent";

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    /** {@code eventTime} as epoch-millis int64, {@code eventName} + {@code awsRegion} as UTF8. */
    private Path writeDatasetFile() throws IOException {
        return writeParquet(
            createTempDir().resolve("cloudtrail.parquet"),
            "message test { required int64 eventTime; required binary eventName (UTF8); required binary awsRegion (UTF8); }",
            DATASET_ROWS,
            1024,
            (g, i) -> {
                g.add("eventTime", 1_500_000_000_000L + i * 1000L);
                g.add("eventName", i % 2 == 0 ? "AssumeRole" : "ListBuckets");
                g.add("awsRegion", "eu-west-1");
            }
        );
    }

    private void createIndex(int docs) {
        assertAcked(
            prepareCreate(INDEX).setMapping(
                "@timestamp",
                "type=date",
                "eventName",
                "type=keyword",
                "sourceIPAddress",
                "type=keyword",
                "userIdentity_userName",
                "type=keyword",
                "awsRegion",
                "type=keyword"
            )
        );
        for (int i = 0; i < docs; i++) {
            prepareIndex(INDEX).setSource(
                "@timestamp",
                "2024-01-0" + (1 + i % 9) + "T00:00:00Z",
                "eventName",
                i % 2 == 0 ? "AssumeRole" : "ListBuckets",
                "sourceIPAddress",
                "10.0.0." + i,
                "userIdentity_userName",
                "user" + i,
                "awsRegion",
                "eu-west-1"
            ).get();
        }
        refresh(INDEX);
    }

    /** Dataset with no declared mapping at all — schema purely inferred from the Parquet footer. */
    private String inferredDataset() throws IOException {
        return registerDataset("cloudtrail_inferred", StoragePath.fileUri(writeDatasetFile()), Map.of());
    }

    /** The issue's shape: {@code dynamic: true} plus a declared {@code @timestamp} aliasing {@code eventTime}. */
    private String declaredPathDataset() throws IOException {
        LinkedHashMap<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("@timestamp", new DatasetFieldMapping("date", "eventTime"));
        return registerNonStrictDataset("cloudtrail_declared", StoragePath.fileUri(writeDatasetFile()), props, Map.of());
    }

    private long count(String query) {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query), TIMEOUT)) {
            return ((Number) getValuesList(response).get(0).get(0)).longValue();
        }
    }

    public void testCountStarInferredDataset() throws Exception {
        createIndex(INDEX_DOCS);
        String ds = inferredDataset();
        assertThat(count("FROM " + ds + " | STATS rows = COUNT(*)"), equalTo((long) DATASET_ROWS));
        assertThat(count("FROM " + INDEX + " | STATS rows = COUNT(*)"), equalTo((long) INDEX_DOCS));
        assertThat(count("FROM " + ds + ", " + INDEX + " | STATS rows = COUNT(*)"), equalTo((long) DATASET_ROWS + INDEX_DOCS));
    }

    public void testCountStarDeclaredPathDataset() throws Exception {
        createIndex(INDEX_DOCS);
        String ds = declaredPathDataset();
        assertThat(count("FROM " + ds + ", " + INDEX + " | STATS rows = COUNT(*)"), equalTo((long) DATASET_ROWS + INDEX_DOCS));
    }

    public void testCountStarEmptyIndex() throws Exception {
        createIndex(0);
        String ds = inferredDataset();
        assertThat(count("FROM " + ds + ", " + INDEX + " | STATS rows = COUNT(*)"), equalTo((long) DATASET_ROWS));
    }

    public void testCountStarWithEqualityFilter() throws Exception {
        createIndex(INDEX_DOCS);
        String ds = inferredDataset();
        assertThat(
            count("FROM " + ds + ", " + INDEX + " | WHERE eventName == \"AssumeRole\" | STATS rows = COUNT(*)"),
            equalTo((long) DATASET_ROWS / 2 + INDEX_DOCS / 2)
        );
    }
}
