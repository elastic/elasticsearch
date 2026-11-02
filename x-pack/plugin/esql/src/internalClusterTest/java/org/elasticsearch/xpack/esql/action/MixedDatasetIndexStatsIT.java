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
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Reproduction scaffold for elastic/elasticsearch#164372: {@code STATS} over a dataset and an index in one
 * {@code FROM} fails with {@code illegal_state_exception: unexpected byte}, thrown while deserializing an
 * {@code ExchangeResponse} off the wire.
 *
 * <p>Three ingredients of the report are load-bearing and must all be present, or the query never crosses a
 * node boundary and a green here says nothing:
 * <ul>
 *   <li><b>More than one data node.</b></li>
 *   <li><b>A multi-split dataset.</b> A lone one-split read stays on the coordinator (see
 *       {@code ManyDatasetsDistributeAcrossNodesIT}), so the dataset branch would never serialize a page.
 *       The reporter's dataset was a {@code *.parquet} glob over ~500k rows; this one is a glob over
 *       {@link #FILE_COUNT} files, and {@link #assertScanRan} proves the read was not folded away.</li>
 *   <li><b>A dataset much wider than the index.</b> The reporter's profile shows {@code EvalOperator x13} on
 *       the index branch, i.e. ~13 union columns the index must null-fill. The fixture carries boolean columns
 *       the index has no counterpart for, and the reported failure always lands inside a {@code BooleanBlock}
 *       read.</li>
 * </ul>
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 2)
@TestLogging(value = "org.elasticsearch.xpack.esql.session.EsqlSession:DEBUG", reason = "capture optimized plans while reproducing #164372")
public class MixedDatasetIndexStatsIT extends AbstractExternalDataSourceIT {

    private static final int FILE_COUNT = 8;
    private static final int ROWS_PER_FILE = 25_000;
    private static final int DATASET_ROWS = FILE_COUNT * ROWS_PER_FILE;
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

    /**
     * A dataset far wider than the index it is co-queried with, including BOOLEAN columns the index lacks.
     * {@code eventTime} is epoch-millis so a declared {@code date} mapping can alias it as {@code @timestamp}.
     */
    private static final String PARQUET_SCHEMA = """
        message test {
          required int64 eventTime;
          required binary eventName (UTF8);
          required binary awsRegion (UTF8);
          required binary sourceIPAddress (UTF8);
          required binary userIdentity_userName (UTF8);
          required boolean readOnly;
          required boolean managementEvent;
          optional boolean sharedEventID;
          required int32 recipientAccountId;
          required int64 requestId;
          required double durationMillis;
          required binary eventSource (UTF8);
          required binary userAgent (UTF8);
          required binary eventCategory (UTF8);
          required binary errorCode (UTF8);
        }
        """;

    /** Writes {@link #FILE_COUNT} Parquet files into a fresh directory and returns a {@code *.parquet} glob URI. */
    private String writeDatasetGlob() throws IOException {
        Path dir = createTempDir().resolve("cloudtrail");
        Files.createDirectories(dir);
        for (int f = 0; f < FILE_COUNT; f++) {
            int base = f * ROWS_PER_FILE;
            writeParquet(dir.resolve("part-" + f + ".parquet"), PARQUET_SCHEMA, ROWS_PER_FILE, 128, (g, r) -> {
                int i = base + r;
                g.add("eventTime", 1_500_000_000_000L + i * 1000L);
                g.add("eventName", i % 2 == 0 ? "AssumeRole" : "ListBuckets");
                g.add("awsRegion", "eu-west-1");
                g.add("sourceIPAddress", "192.168.1." + (i % 255));
                g.add("userIdentity_userName", "svc" + (i % 7));
                g.add("readOnly", i % 2 == 0);
                g.add("managementEvent", i % 3 == 0);
                if (i % 5 != 0) {
                    g.add("sharedEventID", i % 4 == 0);
                }
                g.add("recipientAccountId", 1000 + i);
                g.add("requestId", (long) i * 31);
                g.add("durationMillis", i * 1.5);
                g.add("eventSource", "sts.amazonaws.com");
                g.add("userAgent", "aws-cli/2." + (i % 9));
                g.add("eventCategory", "Management");
                g.add("errorCode", i % 11 == 0 ? "AccessDenied" : "");
            });
        }
        String dirUri = StoragePath.fileUri(dir);
        if (dirUri.endsWith("/") == false) {
            dirUri += "/";
        }
        return dirUri + "*.parquet";
    }

    private void createCloudtrailIndex(int docs) {
        assertTrue(
            indicesAdmin().prepareCreate(INDEX)
                .setMapping(
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
                .get()
                .isAcknowledged()
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

    /** Dataset with no declared mapping at all — schema purely inferred from the Parquet footers. */
    private String inferredDataset() throws IOException {
        return registerDataset("cloudtrail_inferred", writeDatasetGlob(), Map.of());
    }

    /** The issue's shape: {@code dynamic: true} plus a declared {@code @timestamp} aliasing {@code eventTime}. */
    private String declaredPathDataset() throws IOException {
        LinkedHashMap<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("@timestamp", new DatasetFieldMapping("date", "eventTime"));
        return registerNonStrictDataset("cloudtrail_declared", writeDatasetGlob(), props, Map.of());
    }

    /** Plain run with no distribution requirement — a dataset-only COUNT(*) folds from footer statistics and never scans. */
    private long count(String query) {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query), TIMEOUT)) {
            return ((Number) getValuesList(response).get(0).get(0)).longValue();
        }
    }

    /**
     * Runs {@code query} with profiling, logs every driver's node and operator chain (so the shape can be read
     * against the profile in the issue), proves the external read distributed off the coordinator, and returns
     * the single aggregate value.
     */
    private long countDistributed(String query) {
        EsqlQueryRequest request = syncEsqlQueryRequest(query);
        request.profile(true);
        try (EsqlQueryResponse response = run(request, TIMEOUT)) {
            for (var driver : response.profile().drivers()) {
                logger.info(
                    "PROFILE node=[{}] desc=[{}] ops={}",
                    driver.nodeName(),
                    driver.description(),
                    driver.operators().stream().map(op -> op.operator().split("\\[")[0]).toList()
                );
            }
            assertScanRan(response);
            return ((Number) getValuesList(response).get(0).get(0)).longValue();
        }
    }

    /**
     * The external scan must have run rather than been folded away from file statistics. This does NOT prove the
     * read left the coordinator: the coordinator is a node, so a coordinator-local read satisfies it too. Proving
     * the page crossed the wire needs the scan to appear on a node other than the coordinating one, which this
     * assertion deliberately does not claim.
     */
    private static void assertScanRan(EsqlQueryResponse response) {
        assertThat("external scan did not run on any node", externalScanNodeNames(response).size(), greaterThanOrEqualTo(1));
    }

    public void testCountStarInferredDataset() throws Exception {
        createCloudtrailIndex(INDEX_DOCS);
        String ds = inferredDataset();
        assertThat(count("FROM " + ds + " | STATS rows = COUNT(*)"), equalTo((long) DATASET_ROWS));
        assertThat(countDistributed("FROM " + ds + ", " + INDEX + " | STATS rows = COUNT(*)"), equalTo((long) DATASET_ROWS + INDEX_DOCS));
    }

    public void testCountStarDeclaredPathDataset() throws Exception {
        createCloudtrailIndex(INDEX_DOCS);
        String ds = declaredPathDataset();
        assertThat(countDistributed("FROM " + ds + ", " + INDEX + " | STATS rows = COUNT(*)"), equalTo((long) DATASET_ROWS + INDEX_DOCS));
    }

    public void testMaxOverDatasetAndIndex() throws Exception {
        createCloudtrailIndex(INDEX_DOCS);
        String ds = inferredDataset();
        assertThat(
            countDistributed("FROM " + ds + ", " + INDEX + " | STATS m = MAX(recipientAccountId)"),
            equalTo((long) (1000 + DATASET_ROWS - 1))
        );
    }

    public void testCountStarGroupedByKeyword() throws Exception {
        createCloudtrailIndex(INDEX_DOCS);
        String ds = inferredDataset();
        EsqlQueryRequest request = syncEsqlQueryRequest(
            "FROM " + ds + ", " + INDEX + " | STATS rows = COUNT(*) BY awsRegion | SORT awsRegion"
        );
        try (EsqlQueryResponse response = run(request, TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo((long) DATASET_ROWS + INDEX_DOCS));
        }
    }

    public void testCountStarNegatedFilter() throws Exception {
        createCloudtrailIndex(INDEX_DOCS);
        String ds = inferredDataset();
        assertThat(
            countDistributed("FROM " + ds + ", " + INDEX + " | WHERE eventName != \"ListBuckets\" | STATS rows = COUNT(*)"),
            equalTo((long) DATASET_ROWS / 2 + INDEX_DOCS / 2)
        );
    }

    /** The reporter's control: an equality filter on the shared keyword passes even when the others fail. */
    public void testCountStarWithEqualityFilter() throws Exception {
        createCloudtrailIndex(INDEX_DOCS);
        String ds = inferredDataset();
        assertThat(
            countDistributed("FROM " + ds + ", " + INDEX + " | WHERE eventName == \"AssumeRole\" | STATS rows = COUNT(*)"),
            equalTo((long) DATASET_ROWS / 2 + INDEX_DOCS / 2)
        );
    }

    /**
     * {@code COUNT_DISTINCT} is not algebraically decomposable, so a decomposed union emits per-branch
     * intermediate state as a {@code PARTIAL_AGG} composite block — a different serialization path from the
     * plain element-type blocks every other case here exercises. It is on the reporter's failing list.
     */
    public void testCountDistinctOverDatasetAndIndex() throws Exception {
        createCloudtrailIndex(INDEX_DOCS);
        String ds = inferredDataset();
        assertThat(countDistributed("FROM " + ds + ", " + INDEX + " | STATS v = COUNT_DISTINCT(eventName)"), equalTo(2L));
    }

    /** Grouping by a BOOLEAN column only the dataset has: the index branch must null-fill the grouping key. */
    public void testCountStarGroupedByDatasetOnlyBoolean() throws Exception {
        createCloudtrailIndex(INDEX_DOCS);
        String ds = inferredDataset();
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM " + ds + ", " + INDEX + " | STATS rows = COUNT(*) BY readOnly");
        request.profile(true);
        try (EsqlQueryResponse response = run(request, TIMEOUT)) {
            long total = 0;
            for (List<Object> row : getValuesList(response)) {
                total += ((Number) row.get(0)).longValue();
            }
            assertThat(total, equalTo((long) DATASET_ROWS + INDEX_DOCS));
        }
    }

    /** A filter on a boolean column present only in the dataset. */
    public void testCountStarFilterOnDatasetOnlyBoolean() throws Exception {
        createCloudtrailIndex(INDEX_DOCS);
        String ds = inferredDataset();
        assertThat(
            countDistributed("FROM " + ds + ", " + INDEX + " | WHERE sharedEventID IS NOT NULL | STATS rows = COUNT(*)"),
            equalTo((long) DATASET_ROWS - DATASET_ROWS / 5)
        );
    }

    /** The reporter's {@code METADATA _class, _name} shape. */
    public void testCountStarByMetadataClassAndName() throws Exception {
        createCloudtrailIndex(INDEX_DOCS);
        String ds = inferredDataset();
        EsqlQueryRequest request = syncEsqlQueryRequest(
            "FROM " + ds + ", " + INDEX + " METADATA _class, _name | STATS rows = COUNT(*) BY _class, _name"
        );
        try (EsqlQueryResponse response = run(request, TIMEOUT)) {
            long total = 0;
            for (List<Object> row : getValuesList(response)) {
                total += ((Number) row.get(0)).longValue();
            }
            assertThat(total, equalTo((long) DATASET_ROWS + INDEX_DOCS));
        }
    }

    /** The reporter's subquery form: {@code FROM (FROM dataset), (FROM index)}. */
    public void testCountStarThroughSubqueries() throws Exception {
        createCloudtrailIndex(INDEX_DOCS);
        String ds = inferredDataset();
        assertThat(
            count("FROM (FROM " + ds + "), (FROM " + INDEX + ") | STATS rows = COUNT(*)"),
            equalTo((long) DATASET_ROWS + INDEX_DOCS)
        );
    }

    /** MIN over a keyword shared by both sources. */
    public void testMinKeywordOverDatasetAndIndex() throws Exception {
        createCloudtrailIndex(INDEX_DOCS);
        String ds = inferredDataset();
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest("FROM " + ds + ", " + INDEX + " | STATS m = MIN(eventName)"), TIMEOUT)) {
            assertThat(getValuesList(response).get(0).get(0).toString(), equalTo("AssumeRole"));
        }
    }
}
