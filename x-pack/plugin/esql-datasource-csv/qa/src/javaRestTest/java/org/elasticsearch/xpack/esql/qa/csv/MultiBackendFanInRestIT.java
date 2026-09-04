/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.csv;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.ResponseException;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.datasources.AbstractFromDatasetSubqueryRestTestCase;
import org.elasticsearch.xpack.esql.datasources.AzureBackendFixture;
import org.elasticsearch.xpack.esql.datasources.AzureFixtureUtils.DataSourcesAzureHttpFixture;
import org.elasticsearch.xpack.esql.datasources.BackendFixture;
import org.elasticsearch.xpack.esql.datasources.FixtureUtils;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.ACCESS_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.BUCKET;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.SECOND_ACCESS_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.SECOND_SECRET_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.SECRET_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.WAREHOUSE;
import static org.hamcrest.Matchers.hasSize;

/**
 * Flat-{@code FROM} counterpart of {@link MultiBackendSubqueryRestIT}: instead of wrapping each source in its
 * own subquery, all seven sources are listed side by side in a single {@code FROM a, b, c, ...}, so they are
 * planned as sibling producers of one source fan-in rather than as independent subquery branches.
 *
 * <p>The seven sources deliberately disagree on every axis a producer can differ on:
 * <ul>
 *   <li>a local index, so an index and external datasets fan in together;</li>
 *   <li>two S3 datasets whose data sources carry <em>different</em> credentials and point at
 *       <em>different</em> S3 fixtures;</li>
 *   <li>a GCS dataset (service-account JSON auth) and an Azure dataset (shared-key auth);</li>
 *   <li>an unauthenticated HTTP dataset and a local {@code file://} dataset, neither of which carries
 *       credentials at all.</li>
 * </ul>
 *
 * <p>Because every producer is resolved and read independently, a fan-in that leaked one producer's
 * configuration into another would send the wrong credentials, the wrong endpoint, or the wrong storage
 * scheme to at least one source. The two S3 fixtures make that failure observable rather than silent: each
 * accepts only its own access key and answers a request signed with the other's key with a 403, so the query
 * can only return all fourteen rows if each dataset was read with its own data source's credentials.
 * {@link #testS3DatasetCarryingTheOtherFixturesKeyIsRejected} pins that the fixtures really do reject a
 * mismatched key, without which the main assertion would prove nothing.
 *
 * <p>Every source contributes two rows over the shared {@code employees(emp_no, first_name)} schema in a
 * disjoint {@code emp_no} decade, so each output row is attributable to exactly one source.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class MultiBackendFanInRestIT extends AbstractFromDatasetSubqueryRestTestCase {

    // Local index: emp_no 1..2 (Ada, Ben)
    private static final String INDEX = "fan_in_csv_index_employees";

    // S3 on fixture A, credentials ACCESS_KEY/SECRET_KEY: emp_no 11..12 (Cara, Dan)
    private static final String S3_A_DATA_SOURCE = "fan_in_csv_s3_a_ds";
    private static final String S3_A_DATASET = "fan_in_csv_s3_a_employees";
    private static final String S3_A_BLOB_KEY = WAREHOUSE + "/standalone/fan_in_s3_a.csv";

    // S3 on fixture B, credentials SECOND_ACCESS_KEY/SECOND_SECRET_KEY: emp_no 21..22 (Elle, Finn)
    private static final String S3_B_DATA_SOURCE = "fan_in_csv_s3_b_ds";
    private static final String S3_B_DATASET = "fan_in_csv_s3_b_employees";
    private static final String S3_B_BLOB_KEY = WAREHOUSE + "/standalone/fan_in_s3_b.csv";

    // GCS: emp_no 31..32 (Gil, Hana)
    private static final String GCS_DATA_SOURCE = "fan_in_csv_gcs_ds";
    private static final String GCS_DATASET = "fan_in_csv_gcs_employees";
    private static final String GCS_BLOB_KEY = WAREHOUSE + "/standalone/fan_in_gcs.csv";

    // Azure: emp_no 41..42 (Iris, Jonas)
    private static final String AZURE_DATA_SOURCE = "fan_in_csv_azure_ds";
    private static final String AZURE_DATASET = "fan_in_csv_azure_employees";
    private static final String AZURE_BLOB_KEY = WAREHOUSE + "/standalone/fan_in_azure.csv";

    // Anonymous HTTP, served by fixture A over a plain unsigned GET: emp_no 51..52 (Kai, Lena)
    private static final String HTTP_DATA_SOURCE = "fan_in_csv_http_ds";
    private static final String HTTP_DATASET = "fan_in_csv_http_employees";
    private static final String HTTP_BLOB_KEY = WAREHOUSE + "/standalone/fan_in_http.csv";

    // Local file under the node's esql.external.local_allowed_paths root: emp_no 61..62 (Mo, Nadia)
    private static final String LOCAL_DATA_SOURCE = "fan_in_csv_local_ds";
    private static final String LOCAL_DATASET = "fan_in_csv_local_employees";
    private static final String LOCAL_FILE_NAME = "fan_in_local.csv";

    // Negative control: fixture B's endpoint paired with fixture A's key.
    private static final String WRONG_KEY_DATA_SOURCE = "fan_in_csv_wrong_key_ds";
    private static final String WRONG_KEY_DATASET = "fan_in_csv_wrong_key_employees";

    public static DataSourcesS3HttpFixture s3FixtureA = new DataSourcesS3HttpFixture();
    public static DataSourcesS3HttpFixture s3FixtureB = new DataSourcesS3HttpFixture(SECOND_ACCESS_KEY, SECOND_SECRET_KEY);
    public static DataSourcesGcsHttpFixture gcsFixture = new DataSourcesGcsHttpFixture();
    public static DataSourcesAzureHttpFixture azureFixture = new DataSourcesAzureHttpFixture();
    public static ElasticsearchCluster cluster = Clusters.testClusterWithEncryption(() -> s3FixtureA.getAddress());

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3FixtureA)
        .around(s3FixtureB)
        .around(gcsFixture)
        .around(azureFixture)
        .around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @AfterClass
    public static void cleanupRegistry() throws IOException {
        // Cluster is shared across the suite; explicit deletes keep state from leaking into sibling REST ITs.
        for (String dataset : List.of(
            S3_A_DATASET,
            S3_B_DATASET,
            GCS_DATASET,
            AZURE_DATASET,
            HTTP_DATASET,
            LOCAL_DATASET,
            WRONG_KEY_DATASET
        )) {
            deleteIgnoringMissing("/_query/dataset/" + dataset);
        }
        for (String dataSource : List.of(
            S3_A_DATA_SOURCE,
            S3_B_DATA_SOURCE,
            GCS_DATA_SOURCE,
            AZURE_DATA_SOURCE,
            HTTP_DATA_SOURCE,
            LOCAL_DATA_SOURCE,
            WRONG_KEY_DATA_SOURCE
        )) {
            deleteIgnoringMissing("/_query/data_source/" + dataSource);
        }
        deleteIgnoringMissing("/" + INDEX);
    }

    /**
     * Registers one local index and six datasets spanning five storage schemes and four distinct credential
     * shapes (two S3 key pairs, GCS service-account JSON, Azure shared key, and twice no credentials at all),
     * then reads all seven in one flat {@code FROM}. Each source owns a disjoint {@code emp_no} decade, so the
     * fourteen sorted rows below are only produced when every producer read its own resource through its own
     * data source's configuration.
     */
    public void testFlatFanInAcrossIndexTwoS3CredentialsGcsAzureHttpAndLocalFile() throws Exception {
        BackendFixture s3A = new S3BackendFixture(s3FixtureA);
        BackendFixture s3B = new S3BackendFixture(s3FixtureB);
        BackendFixture gcs = new GcsBackendFixture(gcsFixture);
        BackendFixture azure = new AzureBackendFixture(azureFixture);

        createEmployeeIndex(INDEX, Map.of(1, "Ada", 2, "Ben"));

        s3A.uploadBlob(S3_A_BLOB_KEY, csvBytes("11,Cara", "12,Dan"));
        putDataSource(S3_A_DATA_SOURCE, s3A.dataSourceType(), s3A.dataSourceSettings());
        putDataset(S3_A_DATASET, S3_A_DATA_SOURCE, s3A.resourceUri(S3_A_BLOB_KEY), Map.of());

        s3B.uploadBlob(S3_B_BLOB_KEY, csvBytes("21,Elle", "22,Finn"));
        putDataSource(S3_B_DATA_SOURCE, s3B.dataSourceType(), s3B.dataSourceSettings());
        putDataset(S3_B_DATASET, S3_B_DATA_SOURCE, s3B.resourceUri(S3_B_BLOB_KEY), Map.of());

        gcs.uploadBlob(GCS_BLOB_KEY, csvBytes("31,Gil", "32,Hana"));
        putDataSource(GCS_DATA_SOURCE, gcs.dataSourceType(), gcs.dataSourceSettings());
        putDataset(GCS_DATASET, GCS_DATA_SOURCE, gcs.resourceUri(GCS_BLOB_KEY), Map.of());

        azure.uploadBlob(AZURE_BLOB_KEY, csvBytes("41,Iris", "42,Jonas"));
        putDataSource(AZURE_DATA_SOURCE, azure.dataSourceType(), azure.dataSourceSettings());
        putDataset(AZURE_DATASET, AZURE_DATA_SOURCE, azure.resourceUri(AZURE_BLOB_KEY), Map.of());

        // The HTTP provider sends no Authorization header, so fixture A serves this blob as a plain web server
        // would. The bytes live in the same fixture as the S3-A dataset but are reached through a different
        // scheme and a data source that carries no credentials.
        s3A.uploadBlob(HTTP_BLOB_KEY, csvBytes("51,Kai", "52,Lena"));
        putDataSource(HTTP_DATA_SOURCE, "http", Map.of("auth", "anonymous"));
        putDataset(HTTP_DATASET, HTTP_DATA_SOURCE, s3FixtureA.getAddress() + "/" + BUCKET + "/" + HTTP_BLOB_KEY, Map.of());

        Path localFile = writeLocalCsv(csvBytes("61,Mo", "62,Nadia"));
        putDataSource(LOCAL_DATA_SOURCE, "local", Map.of("auth", "anonymous"));
        putDataset(LOCAL_DATASET, LOCAL_DATA_SOURCE, "file://" + localFile, Map.of());

        String query = "FROM "
            + String.join(", ", INDEX, S3_A_DATASET, S3_B_DATASET, GCS_DATASET, AZURE_DATASET, HTTP_DATASET, LOCAL_DATASET)
            + " | KEEP emp_no, first_name"
            + " | SORT emp_no";

        Map<String, Object> response = runQuery(query);
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) response.get("values");

        assertThat("two rows from each of the seven sources", values, hasSize(14));
        assertEmployeeRow(values.get(0), 1, "Ada");
        assertEmployeeRow(values.get(1), 2, "Ben");
        assertEmployeeRow(values.get(2), 11, "Cara");
        assertEmployeeRow(values.get(3), 12, "Dan");
        assertEmployeeRow(values.get(4), 21, "Elle");
        assertEmployeeRow(values.get(5), 22, "Finn");
        assertEmployeeRow(values.get(6), 31, "Gil");
        assertEmployeeRow(values.get(7), 32, "Hana");
        assertEmployeeRow(values.get(8), 41, "Iris");
        assertEmployeeRow(values.get(9), 42, "Jonas");
        assertEmployeeRow(values.get(10), 51, "Kai");
        assertEmployeeRow(values.get(11), 52, "Lena");
        assertEmployeeRow(values.get(12), 61, "Mo");
        assertEmployeeRow(values.get(13), 62, "Nadia");
    }

    /**
     * Negative control for the credential half of the fan-in assertion. A dataset on fixture B is registered
     * through a data source carrying fixture A's key pair; reading it must fail. Without this, the fan-in test
     * above would pass just as happily against fixtures that accept any credentials.
     */
    public void testS3DatasetCarryingTheOtherFixturesKeyIsRejected() throws Exception {
        BackendFixture s3B = new S3BackendFixture(s3FixtureB);
        s3B.uploadBlob(S3_B_BLOB_KEY, csvBytes("21,Elle", "22,Finn"));

        putDataSource(
            WRONG_KEY_DATA_SOURCE,
            "s3",
            Map.of("endpoint", s3FixtureB.getAddress(), "access_key", ACCESS_KEY, "secret_key", SECRET_KEY)
        );
        putDataset(WRONG_KEY_DATASET, WRONG_KEY_DATA_SOURCE, s3B.resourceUri(S3_B_BLOB_KEY), Map.of());

        expectThrows(ResponseException.class, () -> runQuery("FROM " + WRONG_KEY_DATASET + " | KEEP emp_no, first_name"));
    }

    /**
     * Writes the local branch's CSV under the {@code esql.external.local_allowed_paths} root the cluster was
     * booted with, which is the only place a {@code file://} dataset is permitted to read from.
     */
    private static Path writeLocalCsv(byte[] content) throws IOException {
        Path root = PathUtils.get(FixtureUtils.pathRepoRootForIcebergFixtures(Clusters.class));
        Files.createDirectories(root);
        Path file = root.resolve(LOCAL_FILE_NAME);
        Files.write(file, content);
        return file.toAbsolutePath();
    }

    /** Builds a typed-header CSV blob from the supplied {@code emp_no,first_name} rows. */
    private static byte[] csvBytes(String... dataRows) {
        StringBuilder sb = new StringBuilder("emp_no:integer,first_name:keyword\n");
        for (String row : dataRows) {
            sb.append(row).append('\n');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
}
