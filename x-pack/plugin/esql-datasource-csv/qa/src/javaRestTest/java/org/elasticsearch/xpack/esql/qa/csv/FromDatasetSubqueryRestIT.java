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
 * Parameterized REST-driven counterpart to {@code FromDatasetSubqueryIT} for the CSV format.
 * Collapses the previous per-backend trio ({@code FromDatasetSubqueryS3RestIT},
 * {@code FromDatasetSubqueryGcsRestIT}, {@code FromDatasetSubqueryAzureRestIT}) into a single
 * class that runs the same {@code FROM (FROM <dataset> | WHERE ...)} contract against each of the
 * three production cloud-storage backends (S3, GCS, Azure) via the in-process HTTP fixtures.
 *
 * <p>Each parameterized invocation uses backend-disjoint dataset/data-source names so the shared
 * {@code @ClassRule} cluster + fixtures can host all three back-to-back without state leaking
 * between runs; an {@code @AfterClass} sweep deletes all three regardless.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class FromDatasetSubqueryRestIT extends AbstractFromDatasetSubqueryRestTestCase {

    public static DataSourcesS3HttpFixture s3Fixture = new DataSourcesS3HttpFixture();
    public static DataSourcesGcsHttpFixture gcsFixture = new DataSourcesGcsHttpFixture();
    public static DataSourcesAzureHttpFixture azureFixture = new DataSourcesAzureHttpFixture();
    public static ElasticsearchCluster cluster = Clusters.testCluster(() -> s3Fixture.getAddress());

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(gcsFixture).around(azureFixture).around(cluster);

    private final String backendId;

    public FromDatasetSubqueryRestIT(String backendId) {
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
        // Cluster is shared across the suite; sweep all three backend slots regardless of which
        // parameter runs failed so state never leaks into sibling REST ITs reusing the same cluster.
        for (String b : List.of("s3", "gcs", "azure")) {
            deleteIgnoringMissing("/_query/dataset/" + datasetName(b));
            deleteIgnoringMissing("/_query/data_source/" + dataSourceName(b));
        }
    }

    /**
     * End-to-end: register a {@code <backend>}-typed data source pointing at the local fixture
     * endpoint, register a CSV dataset on top, then run {@code FROM (FROM <dataset> | WHERE ...)}
     * and assert the rows the subquery filters in are exactly those produced by the dataset
     * rewriter when the FROM source is rewritten to an external relation.
     */
    public void testSubqueryWithFilter() throws Exception {
        BackendFixture backend = resolveBackend();
        String dataSource = dataSourceName(backend.name());
        String dataset = datasetName(backend.name());
        String blobKey = WAREHOUSE + "/standalone/from_dataset_subquery_csv_" + backend.name() + ".csv";

        backend.uploadBlob(blobKey, sampleCsvBytes());
        putDataSource(dataSource, backend.dataSourceType(), backend.dataSourceSettings());
        putDataset(dataset, dataSource, backend.resourceUri(blobKey), Map.of("format", "csv"));

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
        return backend + "_csv_ds";
    }

    private static String datasetName(String backend) {
        return backend + "_csv_employees";
    }

    private static byte[] sampleCsvBytes() {
        return ("""
            emp_no:integer,first_name:keyword,last_name:keyword,salary:integer
            1,Alice,Anderson,50000
            2,Bob,Brown,60000
            3,Carol,Cox,55000
            """).getBytes(StandardCharsets.UTF_8);
    }
}
