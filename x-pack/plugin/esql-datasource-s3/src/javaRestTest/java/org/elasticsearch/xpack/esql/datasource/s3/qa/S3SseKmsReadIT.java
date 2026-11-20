/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3.qa;

import fixture.aws.AwsCredentialsUtils;
import fixture.aws.DynamicRegionSupplier;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;

/**
 * Reads of SSE-KMS objects, with and without S3 Bucket Keys, return the same rows as the unencrypted copy.
 *
 * <p>S3 decrypts server-side on a plain GET, so a correct read sends nothing for encryption. This test pins that contract
 * against the in-process fixture, which answers the KMS objects with {@code x-amz-server-side-encryption*} response headers.
 * It cannot prove KMS itself (the fixture stores and serves plain bytes), but it fails if the read path starts sending
 * encryption headers (SSE-C style) or fails on an SSE-KMS response.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class S3SseKmsReadIT extends ESRestTestCase {

    private static final String BUCKET = "sse-kms-bucket";
    private static final String ACCESS_KEY = "sse_kms_access_key";
    private static final String SECRET_KEY = "sse_kms_secret_key";
    private static final String ENCRYPTION_PASSWORD_ID = "test";

    private static final String PLAIN_CSV = "data/plain.csv";
    private static final String KMS_CSV = "data/kms.csv";
    private static final String KMS_BUCKET_KEY_CSV = "data/kms_bucket_key.csv";
    private static final String CSV_CONTENT = "id,city\n1,Vienna\n2,Berlin\n";

    private static final DynamicRegionSupplier regionSupplier = new DynamicRegionSupplier();

    private static final SeedingS3HttpFixture s3HttpFixture = new SeedingS3HttpFixture(
        BUCKET,
        AwsCredentialsUtils.fixedAccessKey(ACCESS_KEY, regionSupplier, "s3")
    );

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting(S3FixtureUtils.ALLOWED_ENDPOINT_HOSTS_SETTING, S3FixtureUtils.LOOPBACK_ENDPOINT_HOSTS)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
        .keystore("cluster.state.encryption.password." + ENCRYPTION_PASSWORD_ID, "esql-sse-kms-encryption-password")
        .keystore("cluster.state.encryption.active_password_id", ENCRYPTION_PASSWORD_ID)
        .environment("AWS_REGION", regionSupplier)
        .environment("AWS_CONFIG_FILE", "/dev/null/aws/config")
        .environment("AWS_SHARED_CREDENTIALS_FILE", "/dev/null/aws/credentials")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3HttpFixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @BeforeClass
    public static void disableForReleaseBuilds() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    @BeforeClass
    public static void seedFixture() {
        for (String key : new String[] { PLAIN_CSV, KMS_CSV, KMS_BUCKET_KEY_CSV }) {
            s3HttpFixture.seedBlob(key, CSV_CONTENT.getBytes(StandardCharsets.UTF_8));
        }
        s3HttpFixture.markKmsEncrypted(KMS_CSV, false);
        s3HttpFixture.markKmsEncrypted(KMS_BUCKET_KEY_CSV, true);
    }

    public void testSseKmsObjectsReadLikeTheUnencryptedCopy() throws IOException {
        putDataSource("sse_kms_ds");
        putDataset("plain_rows", PLAIN_CSV);
        putDataset("kms_rows", KMS_CSV);
        putDataset("kms_bucket_key_rows", KMS_BUCKET_KEY_CSV);

        Object expected = query("plain_rows").get("values");
        assertThat(expected, equalTo(List.of(List.of(1, "Vienna"), List.of(2, "Berlin"))));
        assertThat(query("kms_rows").get("values"), equalTo(expected));
        assertThat(query("kms_bucket_key_rows").get("values"), equalTo(expected));

        assertThat(
            "the fixture must have answered the KMS objects with SSE-KMS headers, and the plain one without",
            s3HttpFixture.kmsResponses(),
            allOf(
                hasItems(
                    "GET /" + BUCKET + "/" + KMS_CSV + " bucket-key=false",
                    "GET /" + BUCKET + "/" + KMS_BUCKET_KEY_CSV + " bucket-key=true"
                ),
                not(hasItem(containsString(PLAIN_CSV)))
            )
        );
        assertThat(
            "a read must not ask S3 for encryption; S3 decrypts SSE-KMS objects server-side",
            s3HttpFixture.serverSideEncryptionRequestHeaders(),
            empty()
        );
    }

    private static Map<String, Object> query(String dataset) throws IOException {
        Request req = new Request("POST", "/_query");
        req.setJsonEntity("{\"query\":\"FROM " + dataset + " | SORT id | KEEP id, city | LIMIT 10\"}");
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
        return entityAsMap(r);
    }

    private static void putDataSource(String name) throws IOException {
        Request req = new Request("PUT", "/_query/data_source/" + name);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject()
                .field("type", "s3")
                .field("settings", Map.of("access_key", ACCESS_KEY, "secret_key", SECRET_KEY, "endpoint", s3HttpFixture.getAddress()))
                .endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        assertThat(client().performRequest(req).getStatusLine().getStatusCode(), equalTo(200));
    }

    private static void putDataset(String name, String key) throws IOException {
        Request req = new Request("PUT", "/_query/dataset/" + name);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject()
                .field("data_source", "sse_kms_ds")
                .field("resource", "s3://" + BUCKET + "/" + key)
                .field("settings", Map.of("region", regionSupplier.get()))
                .endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        assertThat(client().performRequest(req).getStatusLine().getStatusCode(), equalTo(200));
    }
}
