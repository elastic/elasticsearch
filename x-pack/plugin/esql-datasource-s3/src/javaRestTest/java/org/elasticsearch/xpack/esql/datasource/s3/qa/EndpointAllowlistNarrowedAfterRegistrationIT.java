/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3.qa;

import fixture.aws.AwsCredentialsUtils;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * A data source registered while {@code esql.external.allowed_endpoint_hosts} admitted its endpoint must not be read
 * once the operator removes that host from the list and restarts the node.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class EndpointAllowlistNarrowedAfterRegistrationIT extends ESRestTestCase {

    private static final String BUCKET = "allowlist-bucket";
    private static final String ACCESS_KEY = "allowlist_ak";
    private static final String SECRET_KEY = "allowlist_sk";
    private static final String DATA_SOURCE = "allowlist_ds";
    private static final String DATASET = "allowlist_data";

    /** Read on every (re)start, so the restart below picks up the narrowed list. */
    private static final AtomicReference<String> allowedHosts = new AtomicReference<>(S3FixtureUtils.LOOPBACK_ENDPOINT_HOSTS);

    private static final SeedingS3HttpFixture s3HttpFixture = new SeedingS3HttpFixture(
        BUCKET,
        AwsCredentialsUtils.fixedAccessKey(ACCESS_KEY, () -> "us-east-1", "s3")
    );

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting(S3FixtureUtils.ALLOWED_ENDPOINT_HOSTS_SETTING, allowedHosts::get)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
        .keystore("cluster.state.encryption.password.test", "allowlist-enc-password")
        .keystore("cluster.state.encryption.active_password_id", "test")
        .environment("AWS_CONFIG_FILE", "/dev/null/aws/config")
        .environment("AWS_SHARED_CREDENTIALS_FILE", "/dev/null/aws/credentials")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3HttpFixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // The cluster is restarted mid-test and is ephemeral for this class.
        return true;
    }

    @BeforeClass
    public static void skipForReleaseBuilds() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    @BeforeClass
    public static void seedFixture() {
        s3HttpFixture.seedBlob("data/test.csv", "id,city\n1,Vienna\n2,Berlin\n".getBytes(StandardCharsets.UTF_8));
    }

    public void testReadIsRefusedOnceTheHostLeavesTheAllowlist() throws IOException {
        putDataSource(DATA_SOURCE);
        putDataset();

        // While the list names the fixture's host, the read succeeds: the setup itself works.
        Response before = client().performRequest(query());
        assertThat(before.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(EntityUtils.toString(before.getEntity()), containsString("Vienna"));

        // The operator drops the loopback hosts from the list and restarts the node.
        allowedHosts.set("minio.internal.example:9000");
        cluster.restart(false);
        closeClients();
        initClient();

        // The narrowed list is in force: registering the same endpoint again is refused.
        ResponseException put = expectThrows(ResponseException.class, () -> putDataSource("allowlist_ds_again"));
        assertThat(put.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(EntityUtils.toString(put.getResponse().getEntity()), containsString("must use https"));

        // The stored data source must now also be refused on read.
        ResponseException e = expectThrows(
            ResponseException.class,
            "the stored endpoint is no longer admitted by the node's allowlist, but the query still read from it",
            () -> client().performRequest(query())
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(EntityUtils.toString(e.getResponse().getEntity()), containsString("must use https"));
    }

    private static Request query() {
        Request req = new Request("POST", "/_query");
        req.setJsonEntity("{\"query\":\"FROM " + DATASET + " | SORT id | LIMIT 10\"}");
        req.setOptions(req.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        return req;
    }

    private static void putDataSource(String name) throws IOException {
        Request req = new Request("PUT", "/_query/data_source/" + name);
        req.setOptions(req.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        req.setJsonEntity(
            Strings.format(
                """
                    {"type":"s3","settings":{"access_key":"%s","secret_key":"%s","endpoint":"%s"}}""",
                ACCESS_KEY,
                SECRET_KEY,
                s3HttpFixture.getAddress()
            )
        );
        assertThat(client().performRequest(req).getStatusLine().getStatusCode(), equalTo(200));
    }

    private static void putDataset() throws IOException {
        Request req = new Request("PUT", "/_query/dataset/" + DATASET);
        req.setOptions(req.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        req.setJsonEntity(Strings.format("""
            {"data_source":"%s","resource":"s3://%s/data/*.csv"}""", DATA_SOURCE, BUCKET));
        assertThat(client().performRequest(req).getStatusLine().getStatusCode(), equalTo(200));
    }
}
