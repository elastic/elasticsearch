/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.csv;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.datasources.AbstractFromDatasetSubqueryRestTestCase;
import org.elasticsearch.xpack.esql.datasources.AzureBackendFixture;
import org.elasticsearch.xpack.esql.datasources.AzureFixtureUtils.DataSourcesAzureHttpFixture;
import org.elasticsearch.xpack.esql.datasources.BackendFixture;
import org.elasticsearch.xpack.esql.datasources.GcsBackendFixture;
import org.elasticsearch.xpack.esql.datasources.GcsFixtureUtils.DataSourcesGcsHttpFixture;
import org.elasticsearch.xpack.esql.datasources.S3BackendFixture;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.WAREHOUSE;
import static org.hamcrest.Matchers.hasSize;

/**
 * CSV counterpart of the Parquet {@code MultiBackendSubqueryRestIT}. Builds a single ES|QL query whose
 * three subqueries straddle <em>different</em> storage providers (S3, GCS, Azure) so the planner,
 * file-source factory and per-backend storage providers are all exercised within one statement, this
 * time reading the dataset through {@code org.elasticsearch.xpack.esql.datasource.csv.CsvFormatReader}.
 *
 * <p>The three CSV blobs share the {@code employees(emp_no, first_name, last_name, salary)} schema but
 * use disjoint {@code emp_no} ranges (1..3 vs 101..103 vs 201..203) and disjoint first-name sets so the
 * combined {@code FROM (A), (B), (C)} UNION assertion can unambiguously attribute every output row to a
 * single backend origin.
 *
 * <p>Backend-specific wiring (auth shape, URI scheme, blob upload) is delegated to
 * {@link S3BackendFixture}, {@link GcsBackendFixture} and {@link AzureBackendFixture}; this class
 * expresses only the cross-backend intent.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class MultiBackendSubqueryRestIT extends AbstractFromDatasetSubqueryRestTestCase {

    // S3 side: emp_no 1..3 (Alice, Bob, Carol)
    private static final String S3_DATA_SOURCE = "mixed_csv_s3_ds";
    private static final String S3_DATASET = "mixed_csv_s3_employees";
    private static final String S3_BLOB_KEY = WAREHOUSE + "/standalone/multi_backend_subquery_s3.csv";

    // GCS side: emp_no 101..103 (Dave, Eve, Frank)
    private static final String GCS_DATA_SOURCE = "mixed_csv_gcs_ds";
    private static final String GCS_DATASET = "mixed_csv_gcs_employees";
    private static final String GCS_BLOB_KEY = WAREHOUSE + "/standalone/multi_backend_subquery_gcs.csv";

    // Azure side: emp_no 201..203 (Gina, Henry, Ivy)
    private static final String AZURE_DATA_SOURCE = "mixed_csv_azure_ds";
    private static final String AZURE_DATASET = "mixed_csv_azure_employees";
    private static final String AZURE_BLOB_KEY = WAREHOUSE + "/standalone/multi_backend_subquery_azure.csv";

    // IN-subquery main dataset on S3: emp_no 1..3 (Alice, Bob, Carol)
    private static final String IN_S3_DATA_SOURCE = "mixed_csv_in_s3_ds";
    private static final String IN_S3_DATASET = "mixed_csv_in_s3_employees";
    private static final String IN_S3_BLOB_KEY = WAREHOUSE + "/standalone/in_subquery_csv_s3.csv";

    // IN-subquery filter dataset on GCS: emp_no 2..4 (overlaps the S3 side on {2, 3})
    private static final String IN_GCS_DATA_SOURCE = "mixed_csv_in_gcs_ds";
    private static final String IN_GCS_DATASET = "mixed_csv_in_gcs_employees";
    private static final String IN_GCS_BLOB_KEY = WAREHOUSE + "/standalone/in_subquery_csv_gcs.csv";

    // Local time-series index whose `name` dimension joins against the S3 dataset's first_name.
    private static final String TS_INDEX = "ts_csv_employees";

    // Union branch: a plain local index (emp_no 301..303).
    private static final String REGULAR_INDEX = "reg_csv_employees";
    // Union branch: a time-series local index read via `TS` (emp_no 401..403).
    private static final String TS_UNION_INDEX = "ts_union_csv_employees";

    public static DataSourcesS3HttpFixture s3Fixture = new DataSourcesS3HttpFixture();
    public static DataSourcesGcsHttpFixture gcsFixture = new DataSourcesGcsHttpFixture();
    public static DataSourcesAzureHttpFixture azureFixture = new DataSourcesAzureHttpFixture();
    public static ElasticsearchCluster cluster = Clusters.testClusterWithEncryption(() -> s3Fixture.getAddress());

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(gcsFixture).around(azureFixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @AfterClass
    public static void cleanupRegistry() throws IOException {
        // Cluster is shared across the suite; explicit deletes keep state from leaking into sibling
        // REST ITs reusing the same cluster.
        deleteIgnoringMissing("/_query/dataset/" + S3_DATASET);
        deleteIgnoringMissing("/_query/data_source/" + S3_DATA_SOURCE);
        deleteIgnoringMissing("/_query/dataset/" + GCS_DATASET);
        deleteIgnoringMissing("/_query/data_source/" + GCS_DATA_SOURCE);
        deleteIgnoringMissing("/_query/dataset/" + AZURE_DATASET);
        deleteIgnoringMissing("/_query/data_source/" + AZURE_DATA_SOURCE);
        deleteIgnoringMissing("/_query/dataset/" + IN_S3_DATASET);
        deleteIgnoringMissing("/_query/data_source/" + IN_S3_DATA_SOURCE);
        deleteIgnoringMissing("/_query/dataset/" + IN_GCS_DATASET);
        deleteIgnoringMissing("/_query/data_source/" + IN_GCS_DATA_SOURCE);
        deleteIgnoringMissing("/" + TS_INDEX);
        deleteIgnoringMissing("/" + REGULAR_INDEX);
        deleteIgnoringMissing("/" + TS_UNION_INDEX);
    }

    /**
     * End-to-end: register a CSV-on-S3 dataset, a CSV-on-GCS dataset and a CSV-on-Azure dataset, then run a
     * single {@code FROM (sub-A), ..., (sub-E)} query whose five subqueries mix all three production storage
     * backends (through the CSV format reader) with two local-index source kinds.
     *
     * <p>Sub-A keeps {@code emp_no IN {2, 3}} from the S3 dataset (Bob, Carol). Sub-B keeps {@code emp_no >= 102}
     * from the GCS dataset (Eve, Frank). Sub-C keeps {@code emp_no >= 202} from the Azure dataset (Henry, Ivy).
     * Sub-D keeps {@code emp_no >= 302} from a plain local index (Kate, Leo). Sub-E reads a local time-series
     * index via {@code TS} and keeps {@code emp_no >= 402} (Nina, Oscar). Combined output, sorted by
     * {@code emp_no}, is exactly the ten-row interleave asserted below.
     */
    public void testThreeSubqueriesAcrossS3GcsAzure() throws Exception {
        BackendFixture s3Backend = new S3BackendFixture(s3Fixture);
        BackendFixture gcsBackend = new GcsBackendFixture(gcsFixture);
        BackendFixture azureBackend = new AzureBackendFixture(azureFixture);

        // CSV → S3. Format is inferred from the .csv resource extension; datasets do not accept a `format` setting.
        s3Backend.uploadBlob(S3_BLOB_KEY, csvBytes("1,Alice,Anderson,50000", "2,Bob,Brown,60000", "3,Carol,Cox,55000"));
        putDataSource(S3_DATA_SOURCE, s3Backend.dataSourceType(), s3Backend.dataSourceSettings());
        putDataset(S3_DATASET, S3_DATA_SOURCE, s3Backend.resourceUri(S3_BLOB_KEY), Map.of());

        // CSV → GCS
        gcsBackend.uploadBlob(GCS_BLOB_KEY, csvBytes("101,Dave,Davis,70000", "102,Eve,Edwards,65000", "103,Frank,Foster,80000"));
        putDataSource(GCS_DATA_SOURCE, gcsBackend.dataSourceType(), gcsBackend.dataSourceSettings());
        putDataset(GCS_DATASET, GCS_DATA_SOURCE, gcsBackend.resourceUri(GCS_BLOB_KEY), Map.of());

        // CSV → Azure
        azureBackend.uploadBlob(AZURE_BLOB_KEY, csvBytes("201,Gina,Green,70000", "202,Henry,Hill,75000", "203,Ivy,Ito,80000"));
        putDataSource(AZURE_DATA_SOURCE, azureBackend.dataSourceType(), azureBackend.dataSourceSettings());
        putDataset(AZURE_DATASET, AZURE_DATA_SOURCE, azureBackend.resourceUri(AZURE_BLOB_KEY), Map.of());

        // Plain local index branch (emp_no 301..303) and time-series local index branch (emp_no 401..403).
        createEmployeeIndex(REGULAR_INDEX, Map.of(301, "Jack", 302, "Kate", 303, "Leo"));
        createTimeSeriesEmployeeIndex(TS_UNION_INDEX, Map.of(401, "Mia", 402, "Nina", 403, "Oscar"));

        String query = "FROM (FROM "
            + S3_DATASET
            + " | WHERE emp_no >= 2 AND emp_no <= 3), (FROM "
            + GCS_DATASET
            + " | WHERE emp_no >= 102), (FROM "
            + AZURE_DATASET
            + " | WHERE emp_no >= 202), (FROM "
            + REGULAR_INDEX
            + " | WHERE emp_no >= 302), (TS "
            + TS_UNION_INDEX
            + " | WHERE emp_no >= 402 | KEEP emp_no, first_name)"
            + " | KEEP emp_no, first_name"
            + " | SORT emp_no";

        Map<String, Object> response = runQuery(query);
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) response.get("values");

        assertThat("two rows per source (3 datasets + regular index + TS index) expected", values, hasSize(10));
        assertEmployeeRow(values.get(0), 2, "Bob");
        assertEmployeeRow(values.get(1), 3, "Carol");
        assertEmployeeRow(values.get(2), 102, "Eve");
        assertEmployeeRow(values.get(3), 103, "Frank");
        assertEmployeeRow(values.get(4), 202, "Henry");
        assertEmployeeRow(values.get(5), 203, "Ivy");
        assertEmployeeRow(values.get(6), 302, "Kate");
        assertEmployeeRow(values.get(7), 303, "Leo");
        assertEmployeeRow(values.get(8), 402, "Nina");
        assertEmployeeRow(values.get(9), 403, "Oscar");
    }

    /**
     * Cross-backend {@code WHERE ... IN (subquery)}: the main query reads a CSV dataset on S3 while the
     * IN subquery reads a CSV dataset on GCS, so the IN join key is resolved across two distinct storage
     * backends in one statement. The S3 side carries {@code emp_no {1, 2, 3}}; the GCS filter side carries
     * {@code {2, 3, 4}}, so only the overlapping {@code {2, 3}} (Bob, Carol) survive the filter.
     */
    public void testInSubqueryMainS3FilterGcs() throws Exception {
        BackendFixture s3Backend = new S3BackendFixture(s3Fixture);
        BackendFixture gcsBackend = new GcsBackendFixture(gcsFixture);

        // Main CSV dataset → S3
        s3Backend.uploadBlob(IN_S3_BLOB_KEY, csvBytes("1,Alice,Anderson,50000", "2,Bob,Brown,60000", "3,Carol,Cox,55000"));
        putDataSource(IN_S3_DATA_SOURCE, s3Backend.dataSourceType(), s3Backend.dataSourceSettings());
        putDataset(IN_S3_DATASET, IN_S3_DATA_SOURCE, s3Backend.resourceUri(IN_S3_BLOB_KEY), Map.of());

        // Filter CSV dataset → GCS
        gcsBackend.uploadBlob(IN_GCS_BLOB_KEY, csvBytes("2,Bob,Brown,60000", "3,Carol,Cox,55000", "4,Dan,Dixon,45000"));
        putDataSource(IN_GCS_DATA_SOURCE, gcsBackend.dataSourceType(), gcsBackend.dataSourceSettings());
        putDataset(IN_GCS_DATASET, IN_GCS_DATA_SOURCE, gcsBackend.resourceUri(IN_GCS_BLOB_KEY), Map.of());

        String query = "FROM "
            + IN_S3_DATASET
            + " | WHERE emp_no IN (FROM "
            + IN_GCS_DATASET
            + " | KEEP emp_no)"
            + " | KEEP emp_no, first_name"
            + " | SORT emp_no";

        Map<String, Object> response = runQuery(query);
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) response.get("values");

        assertThat("only the overlapping emp_no {2, 3} survive the cross-backend IN filter", values, hasSize(2));
        assertEmployeeRow(values.get(0), 2, "Bob");
        assertEmployeeRow(values.get(1), 3, "Carol");
    }

    /**
     * Crosses a blob-backed external dataset with a local time-series index: the main query reads the CSV dataset
     * on S3 (a blob storage) while the IN subquery reads a {@code time_series}-mode index whose {@code name}
     * dimension carries {@code {Bob, Carol, Dan}}. Only the dataset rows whose {@code first_name} appears in the
     * time-series index survive — Bob (2) and Carol (3); Alice is dropped.
     */
    public void testInSubqueryMainDatasetSubqueryTimeSeriesIndex() throws Exception {
        BackendFixture s3Backend = new S3BackendFixture(s3Fixture);

        // External CSV dataset → S3 (Alice/Bob/Carol, emp_no 1..3)
        s3Backend.uploadBlob(S3_BLOB_KEY, csvBytes("1,Alice,Anderson,50000", "2,Bob,Brown,60000", "3,Carol,Cox,55000"));
        putDataSource(S3_DATA_SOURCE, s3Backend.dataSourceType(), s3Backend.dataSourceSettings());
        putDataset(S3_DATASET, S3_DATA_SOURCE, s3Backend.resourceUri(S3_BLOB_KEY), Map.of());

        createTimeSeriesIndex(TS_INDEX, "Bob", "Carol", "Dan");

        String query = "FROM "
            + S3_DATASET
            + " | WHERE first_name IN (TS "
            + TS_INDEX
            + " | KEEP name)"
            + " | KEEP emp_no, first_name"
            + " | SORT emp_no";

        Map<String, Object> response = runQuery(query);
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) response.get("values");

        assertThat("only Bob and Carol overlap the time-series index", values, hasSize(2));
        assertEmployeeRow(values.get(0), 2, "Bob");
        assertEmployeeRow(values.get(1), 3, "Carol");
    }

    /** Builds a typed-header CSV blob ({@code emp_no:integer,...}) from the supplied {@code emp_no,first,last,salary} rows. */
    private static byte[] csvBytes(String... dataRows) {
        StringBuilder sb = new StringBuilder("emp_no:integer,first_name:keyword,last_name:keyword,salary:integer\n");
        for (String row : dataRows) {
            sb.append(row).append('\n');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
}
