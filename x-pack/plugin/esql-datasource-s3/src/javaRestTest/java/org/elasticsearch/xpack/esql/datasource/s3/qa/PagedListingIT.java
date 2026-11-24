/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3.qa;

import fixture.aws.AwsCredentialsUtils;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.sun.net.httpserver.HttpHandler;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * A glob dataset must read every file of its prefix even when the store's listing contains a page that is marked
 * truncated but carries no keys. ListObjectsV2 bounds a page's key count only from above, so such a page is
 * permitted; continuation is decided by the continuation token.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class PagedListingIT extends ESRestTestCase {

    private static final String BUCKET = "paged-listing-bucket";
    private static final String ACCESS_KEY = "paged_listing_ak";
    private static final String SECRET_KEY = "paged_listing_sk";
    private static final String REGION = "us-east-1";
    private static final String DATA_SOURCE = "paged_ds";
    private static final String DATASET = "paged_rows";

    private static final String FIRST = "first";
    private static final Map<String, String> PAGES = Map.of(
        FIRST,
        page(List.of("data/a.csv"), "token-1"),
        "token-1",
        page(List.of(), "token-2"),
        "token-2",
        page(List.of("data/b.csv"), null)
    );

    private static final PagedListingFixture s3HttpFixture = new PagedListingFixture();

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting(S3FixtureUtils.ALLOWED_ENDPOINT_HOSTS_SETTING, S3FixtureUtils.LOOPBACK_ENDPOINT_HOSTS)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
        .keystore("cluster.state.encryption.password.test", "paged-listing-enc-password")
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

    @BeforeClass
    public static void skipForReleaseBuilds() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    @BeforeClass
    public static void seedObjects() {
        s3HttpFixture.seedBlob("data/a.csv", "id,city\n1,Vienna\n".getBytes(StandardCharsets.UTF_8));
        s3HttpFixture.seedBlob("data/b.csv", "id,city\n2,Berlin\n".getBytes(StandardCharsets.UTF_8));
    }

    @After
    public void cleanup() {
        for (String path : new String[] { "/_query/dataset/" + DATASET, "/_query/data_source/" + DATA_SOURCE }) {
            Request delete = new Request("DELETE", path);
            delete.setOptions(delete.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
            try {
                client().performRequest(delete);
            } catch (Exception ignored) {}
        }
    }

    public void testGlobReadsFilesAfterEmptyTruncatedPage() throws IOException {
        Request dataSource = new Request("PUT", "/_query/data_source/" + DATA_SOURCE);
        dataSource.setOptions(dataSource.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        dataSource.setJsonEntity(
            Strings.format(
                """
                    {"type":"s3","settings":{"access_key":"%s","secret_key":"%s","region":"%s","endpoint":"%s"}}""",
                ACCESS_KEY,
                SECRET_KEY,
                REGION,
                s3HttpFixture.getAddress()
            )
        );
        assertThat(client().performRequest(dataSource).getStatusLine().getStatusCode(), equalTo(200));

        Request dataset = new Request("PUT", "/_query/dataset/" + DATASET);
        dataset.setOptions(dataset.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        dataset.setJsonEntity(Strings.format("""
            {"data_source":"%s","resource":"s3://%s/data/*.csv"}""", DATA_SOURCE, BUCKET));
        assertThat(client().performRequest(dataset).getStatusLine().getStatusCode(), equalTo(200));

        Request query = new Request("POST", "/_query");
        query.setOptions(query.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        query.setJsonEntity("{\"query\":\"FROM " + DATASET + " | STATS c = COUNT(*)\"}");
        String body = EntityUtils.toString(client().performRequest(query).getEntity());

        assertThat(body, containsString("\"values\":[[2]]"));
    }

    private static String page(List<String> keys, String nextToken) {
        StringBuilder xml = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        xml.append("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
        xml.append("<Name>").append(BUCKET).append("</Name><Prefix>data/</Prefix>");
        xml.append("<KeyCount>").append(keys.size()).append("</KeyCount><MaxKeys>1000</MaxKeys>");
        xml.append("<IsTruncated>").append(nextToken != null).append("</IsTruncated>");
        for (String key : keys) {
            xml.append("<Contents><Key>").append(key).append("</Key>");
            xml.append("<LastModified>2026-01-01T00:00:00.000Z</LastModified><Size>17</Size>");
            xml.append("<StorageClass>STANDARD</StorageClass></Contents>");
        }
        if (nextToken != null) {
            xml.append("<NextContinuationToken>").append(nextToken).append("</NextContinuationToken>");
        }
        return xml.append("</ListBucketResult>").toString();
    }

    /** Serves the seeded objects normally and answers the {@code data/} listing from the scripted pages. */
    @SuppressForbidden(reason = "overrides S3HttpFixture.createHandler which returns com.sun.net.httpserver.HttpHandler")
    private static final class PagedListingFixture extends SeedingS3HttpFixture {
        PagedListingFixture() {
            super(BUCKET, AwsCredentialsUtils.fixedAccessKey(ACCESS_KEY, () -> REGION, "s3"));
        }

        @Override
        protected HttpHandler createHandler() {
            HttpHandler delegate = super.createHandler();
            return exchange -> {
                String query = exchange.getRequestURI().getRawQuery();
                if ("GET".equals(exchange.getRequestMethod()) && query != null && query.contains("list-type=2")) {
                    String token = FIRST;
                    for (String param : query.split("&")) {
                        if (param.startsWith("continuation-token=")) {
                            token = java.net.URLDecoder.decode(param.substring("continuation-token=".length()), StandardCharsets.UTF_8);
                        }
                    }
                    String body = PAGES.get(token);
                    if (body != null) {
                        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders().add("Content-Type", "application/xml");
                        exchange.sendResponseHeaders(200, bytes.length);
                        exchange.getResponseBody().write(bytes);
                        exchange.close();
                        return;
                    }
                }
                delegate.handle(exchange);
            };
        }
    }
}
