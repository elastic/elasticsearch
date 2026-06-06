/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.parquet;

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
 * Mixed-format, mixed-backend FROM-subquery REST IT. Builds a single ES|QL query whose three
 * subqueries straddle <em>different</em> storage providers and <em>different</em> file formats so
 * the planner, file-source factory and per-backend storage providers are all exercised within one
 * statement:
 *
 * <ul>
 *   <li>Subquery A reads a CSV blob from S3 (via the in-process {@link DataSourcesS3HttpFixture})
 *       and is wired through the {@code esql-datasource-s3} + {@code esql-datasource-csv} plugins.</li>
 *   <li>Subquery B reads a Parquet blob from GCS (via the in-process {@link DataSourcesGcsHttpFixture})
 *       and is wired through the {@code esql-datasource-gcs} + {@code esql-datasource-parquet}
 *       plugins.</li>
 *   <li>Subquery C reads an NDJSON blob from Azure Blob Storage (via the in-process
 *       {@link DataSourcesAzureHttpFixture}) and is wired through the {@code esql-datasource-azure}
 *       + {@code esql-datasource-ndjson} plugins.</li>
 * </ul>
 *
 * <p>All three payloads share the {@code employees(emp_no, first_name, last_name, salary)} schema
 * but use disjoint {@code emp_no} ranges (1..3 vs 101..103 vs 201..203) so the multi-subquery
 * UNION assertion can unambiguously attribute every output row to one specific
 * (format, backend) origin in the {@code FROM (A), (B), (C)} construct.
 *
 * <p>Iceberg is intentionally <em>not</em> included here: see {@code BackendFixture}'s class-level
 * javadoc — iceberg has no production {@code DataSourceValidator} and so cannot today be reached
 * via {@code FROM <dataset>}. Adding iceberg coverage requires either a core-side validator
 * implementation or a separate test that targets the {@code EXTERNAL "s3://..." WITH
 * { "format": "iceberg" }} command shape.
 *
 * <p>Backend-specific wiring (auth shape, URI scheme, blob upload) is delegated to
 * {@link S3BackendFixture}, {@link GcsBackendFixture} and {@link AzureBackendFixture}; this class
 * is left to express only the cross-format / cross-backend intent.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class MultiBackendSubqueryRestIT extends AbstractFromDatasetSubqueryRestTestCase {

    // S3 / CSV side: emp_no 1..3 (Alice, Bob, Carol)
    private static final String CSV_DATA_SOURCE = "mixed_csv_s3_ds";
    private static final String CSV_DATASET = "mixed_csv_s3_employees";
    private static final String CSV_BLOB_KEY = WAREHOUSE + "/standalone/multi_backend_subquery.csv";

    // GCS / Parquet side: emp_no 101..103 (Dave, Eve, Frank)
    private static final String PARQUET_DATA_SOURCE = "mixed_parquet_gcs_ds";
    private static final String PARQUET_DATASET = "mixed_parquet_gcs_employees";
    private static final String PARQUET_BLOB_KEY = WAREHOUSE + "/standalone/multi_backend_subquery.parquet";

    // Azure / NDJSON side: emp_no 201..203 (Gina, Henry, Ivy)
    private static final String NDJSON_DATA_SOURCE = "mixed_ndjson_azure_ds";
    private static final String NDJSON_DATASET = "mixed_ndjson_azure_employees";
    private static final String NDJSON_BLOB_KEY = WAREHOUSE + "/standalone/multi_backend_subquery.ndjson";

    public static DataSourcesS3HttpFixture s3Fixture = new DataSourcesS3HttpFixture();
    public static DataSourcesGcsHttpFixture gcsFixture = new DataSourcesGcsHttpFixture();
    public static DataSourcesAzureHttpFixture azureFixture = new DataSourcesAzureHttpFixture();
    public static ElasticsearchCluster cluster = Clusters.testCluster(() -> s3Fixture.getAddress());

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(gcsFixture).around(azureFixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @AfterClass
    public static void cleanupRegistry() throws IOException {
        // Cluster is shared across the suite (see Clusters.testCluster); explicit deletes keep state
        // from leaking into sibling REST ITs reusing the same cluster.
        deleteIgnoringMissing("/_query/dataset/" + CSV_DATASET);
        deleteIgnoringMissing("/_query/data_source/" + CSV_DATA_SOURCE);
        deleteIgnoringMissing("/_query/dataset/" + PARQUET_DATASET);
        deleteIgnoringMissing("/_query/data_source/" + PARQUET_DATA_SOURCE);
        deleteIgnoringMissing("/_query/dataset/" + NDJSON_DATASET);
        deleteIgnoringMissing("/_query/data_source/" + NDJSON_DATA_SOURCE);
    }

    /**
     * End-to-end: register a CSV-on-S3 dataset, a Parquet-on-GCS dataset and an NDJSON-on-Azure
     * dataset, then run a single {@code FROM (sub-A), (sub-B), (sub-C)} query whose three
     * subqueries straddle all three production storage backends and three different format readers.
     *
     * <p>Sub-A keeps {@code emp_no IN {2, 3}} from the CSV/S3 side (Bob, Carol). Sub-B keeps
     * {@code emp_no >= 102} from the Parquet/GCS side (Eve, Frank). Sub-C keeps
     * {@code emp_no >= 202} from the NDJSON/Azure side (Henry, Ivy). Combined output, sorted by
     * {@code emp_no}, is exactly the six-row interleave asserted below — any other shape means one
     * of the three subqueries silently dropped, returned the wrong rows, or got planned against the
     * wrong (backend, format) pair.
     */
    public void testThreeSubqueriesAcrossCsvParquetNdjson() throws Exception {
        BackendFixture s3Backend = new S3BackendFixture(s3Fixture);
        BackendFixture gcsBackend = new GcsBackendFixture(gcsFixture);
        BackendFixture azureBackend = new AzureBackendFixture(azureFixture);

        // CSV → S3
        s3Backend.uploadBlob(CSV_BLOB_KEY, sampleCsvBytes());
        putDataSource(CSV_DATA_SOURCE, s3Backend.dataSourceType(), s3Backend.dataSourceSettings());
        putDataset(CSV_DATASET, CSV_DATA_SOURCE, s3Backend.resourceUri(CSV_BLOB_KEY), Map.of("format", "csv"));

        // Parquet → GCS
        gcsBackend.uploadBlob(PARQUET_BLOB_KEY, EmployeesParquetGenerator.alternateEmployeesParquetBytes());
        putDataSource(PARQUET_DATA_SOURCE, gcsBackend.dataSourceType(), gcsBackend.dataSourceSettings());
        putDataset(PARQUET_DATASET, PARQUET_DATA_SOURCE, gcsBackend.resourceUri(PARQUET_BLOB_KEY), Map.of("format", "parquet"));

        // NDJSON → Azure
        azureBackend.uploadBlob(NDJSON_BLOB_KEY, sampleNdjsonBytes());
        putDataSource(NDJSON_DATA_SOURCE, azureBackend.dataSourceType(), azureBackend.dataSourceSettings());
        putDataset(NDJSON_DATASET, NDJSON_DATA_SOURCE, azureBackend.resourceUri(NDJSON_BLOB_KEY), Map.of("format", "ndjson"));

        String query = "FROM (FROM "
            + CSV_DATASET
            + " | WHERE emp_no >= 2 AND emp_no <= 3), (FROM "
            + PARQUET_DATASET
            + " | WHERE emp_no >= 102), (FROM "
            + NDJSON_DATASET
            + " | WHERE emp_no >= 202)"
            + " | KEEP emp_no, first_name"
            + " | SORT emp_no";

        Map<String, Object> response = runQuery(query);
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) response.get("values");

        assertThat("two rows per backend (CSV/S3 + Parquet/GCS + NDJSON/Azure) expected", values, hasSize(6));
        assertEmployeeRow(values.get(0), 2, "Bob");
        assertEmployeeRow(values.get(1), 3, "Carol");
        assertEmployeeRow(values.get(2), 102, "Eve");
        assertEmployeeRow(values.get(3), 103, "Frank");
        assertEmployeeRow(values.get(4), 202, "Henry");
        assertEmployeeRow(values.get(5), 203, "Ivy");
    }

    private static byte[] sampleCsvBytes() {
        return ("""
            emp_no:integer,first_name:keyword,last_name:keyword,salary:integer
            1,Alice,Anderson,50000
            2,Bob,Brown,60000
            3,Carol,Cox,55000
            """).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * NDJSON sibling of {@link #sampleCsvBytes} / {@code EmployeesParquetGenerator.alternateEmployeesParquetBytes()}.
     * Disjoint {@code emp_no} range (201..203) and disjoint first-name set (Gina/Henry/Ivy) so each
     * row in the combined query output is unambiguously attributable to this subquery.
     */
    private static byte[] sampleNdjsonBytes() {
        return ("""
            {"emp_no":201,"first_name":"Gina","last_name":"Green","salary":70000}
            {"emp_no":202,"first_name":"Henry","last_name":"Hill","salary":75000}
            {"emp_no":203,"first_name":"Ivy","last_name":"Ito","salary":80000}
            """).getBytes(StandardCharsets.UTF_8);
    }
}
