/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.csv;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
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
 * Parameterized REST-driven {@code FROM (FROM <dataset> | ...)} IT for the TSV format. TSV is
 * registered by the same {@code esql-datasource-csv} plugin as CSV (just a different file
 * extension + delimiter), but the {@code .tsv} extension takes its own production code path
 * through {@link org.elasticsearch.xpack.esql.datasource.csv.CsvFormatReader} configured with
 * {@code CsvFormatOptions.TSV}, so it warrants an explicit suite to keep that path covered.
 *
 * <p>Runs against each of the three production cloud-storage backends (S3, GCS, Azure) via the
 * in-process HTTP fixtures, sharing the same canonical assertion shape as
 * {@link FromDatasetSubqueryRestIT}.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class TsvFromDatasetSubqueryRestIT extends AbstractFromDatasetSubqueryRestTestCase {

    public static DataSourcesS3HttpFixture s3Fixture = new DataSourcesS3HttpFixture();
    public static DataSourcesGcsHttpFixture gcsFixture = new DataSourcesGcsHttpFixture();
    public static DataSourcesAzureHttpFixture azureFixture = new DataSourcesAzureHttpFixture();
    public static ElasticsearchCluster cluster = Clusters.testCluster(() -> s3Fixture.getAddress());

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(gcsFixture).around(azureFixture).around(cluster);

    private final String backendId;

    public TsvFromDatasetSubqueryRestIT(String backendId) {
        this.backendId = backendId;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @ParametersFactory(argumentFormatting = "%1$s")
    public static List<Object[]> backends() {
        return List.of(new Object[] { "s3" }, new Object[] { "gcs" }, new Object[] { "azure" });
    }

    @AfterClass
    public static void cleanupRegistry() throws IOException {
        for (String b : List.of("s3", "gcs", "azure")) {
            deleteIgnoringMissing("/_query/dataset/" + datasetName(b));
            deleteIgnoringMissing("/_query/data_source/" + dataSourceName(b));
        }
    }

    public void testSubqueryWithFilter() throws Exception {
        BackendFixture backend = resolveBackend();
        String dataSource = dataSourceName(backend.name());
        String dataset = datasetName(backend.name());
        String blobKey = WAREHOUSE + "/standalone/from_dataset_subquery_tsv_" + backend.name() + ".tsv";

        backend.uploadBlob(blobKey, sampleTsvBytes());
        putDataSource(dataSource, backend.dataSourceType(), backend.dataSourceSettings());
        putDataset(dataset, dataSource, backend.resourceUri(blobKey), Map.of("format", "tsv"));

        Map<String, Object> response = runQuery("FROM (FROM " + dataset + " | WHERE emp_no > 1) | KEEP emp_no, first_name | SORT emp_no");
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) response.get("values");
        assertThat("subquery WHERE emp_no > 1 keeps rows 2 and 3", values, hasSize(2));
        assertEmployeeRow(values.get(0), 2, "Bob");
        assertEmployeeRow(values.get(1), 3, "Carol");
    }

    private BackendFixture resolveBackend() {
        return switch (backendId) {
            case "s3" -> new S3BackendFixture(s3Fixture);
            case "gcs" -> new GcsBackendFixture(gcsFixture);
            case "azure" -> new AzureBackendFixture(azureFixture);
            default -> throw new AssertionError("unknown backend [" + backendId + "]");
        };
    }

    private static String dataSourceName(String backend) {
        return backend + "_tsv_ds";
    }

    private static String datasetName(String backend) {
        return backend + "_tsv_employees";
    }

    /**
     * Three-row employees payload with tab delimiters. Same header-typing convention as the CSV
     * payload (so {@link AbstractFromDatasetSubqueryRestTestCase#assertEmployeeRow} can apply
     * unchanged) — only the field separator differs.
     */
    private static byte[] sampleTsvBytes() {
        return ("emp_no:integer\tfirst_name:keyword\tlast_name:keyword\tsalary:integer\n"
            + "1\tAlice\tAnderson\t50000\n"
            + "2\tBob\tBrown\t60000\n"
            + "3\tCarol\tCox\t55000\n").getBytes(StandardCharsets.UTF_8);
    }
}
