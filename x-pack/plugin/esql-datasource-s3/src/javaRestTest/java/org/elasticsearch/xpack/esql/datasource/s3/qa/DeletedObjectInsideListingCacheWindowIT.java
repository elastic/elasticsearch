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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheSettings;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * A file deleted from the object store after a dataset's file listing was cached must not fail queries over the
 * rest of that dataset until the listing TTL elapses.
 * <p>
 * Two datasets share one prefix and differ in sort, so they cache under different listing keys. After a
 * delete-and-add, a query that still holds the stale set may fail once; that failure drops the listing, so the
 * next query lists the store. Either way, a query inside the TTL window reads the surviving file and the file
 * added after the delete, not the deleted one. The added file is what tells a re-list apart from a silent skip.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class DeletedObjectInsideListingCacheWindowIT extends ESRestTestCase {

    private static final String BUCKET = "listing-cache-bucket";
    private static final String ACCESS_KEY = "listing_cache_access_key";
    private static final String SECRET_KEY = "listing_cache_secret_key";
    private static final String DATA_SOURCE = "listing_cache_ds";

    /** Default first-file-wins listing order. */
    private static final String DATASET_DEFAULT = "listing_cache_default";
    /** {@code file_sort_by: name} is a different listing key over the same prefix. */
    private static final String DATASET_BY_NAME = "listing_cache_by_name";

    private static final String SURVIVING_CSV = "glob/a.csv";
    private static final String DELETED_CSV = "glob/b.csv";
    /** Seeded only after the listings were cached: its rows are the proof of whether a query re-listed. */
    private static final String ADDED_CSV = "glob/c.csv";

    private static final DynamicRegionSupplier regionSupplier = new DynamicRegionSupplier();

    private static final SeedingS3HttpFixture s3HttpFixture = new SeedingS3HttpFixture(
        BUCKET,
        AwsCredentialsUtils.fixedAccessKey(ACCESS_KEY, regionSupplier, "s3")
    );

    private static final String ENCRYPTION_PASSWORD_ID = "test";

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting(S3FixtureUtils.ALLOWED_ENDPOINT_HOSTS_SETTING, S3FixtureUtils.LOOPBACK_ENDPOINT_HOSTS)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
        .setting(ExternalSourceCacheSettings.CACHE_ENABLED.getKey(), "true")
        .setting(ExternalSourceCacheSettings.LISTING_TTL.getKey(), "1h")
        .keystore("cluster.state.encryption.password." + ENCRYPTION_PASSWORD_ID, "esql-listing-cache-encryption-password")
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
        assumeFalse("plain HTTP S3 fixture", inFipsJvm());
    }

    @BeforeClass
    public static void seedFixture() {
        s3HttpFixture.seedBlob(SURVIVING_CSV, "id,city\n1,Vienna\n".getBytes(StandardCharsets.UTF_8));
        s3HttpFixture.seedBlob(DELETED_CSV, "id,city\n2,Berlin\n".getBytes(StandardCharsets.UTF_8));
    }

    private record Outcome(int status, String body, List<String> warnings) {}

    public void testDeletedObjectInsideTheCachedListingWindow() throws Exception {
        putDataSource();
        putDataset(DATASET_DEFAULT, Map.of());
        putDataset(DATASET_BY_NAME, Map.of("file_sort_by", "name"));

        for (String dataset : List.of(DATASET_DEFAULT, DATASET_BY_NAME)) {
            Outcome before = run(dataset);
            assertThat(before.status(), equalTo(200));
            assertThat(before.body(), containsString("Vienna"));
            assertThat(before.body(), containsString("Berlin"));
        }

        s3HttpFixture.removeBlob(DELETED_CSV);
        s3HttpFixture.seedBlob(ADDED_CSV, "id,city\n3,Prague\n".getBytes(StandardCharsets.UTF_8));

        for (String dataset : List.of(DATASET_DEFAULT, DATASET_BY_NAME)) {
            assertCurrentStore(runUntilCurrentStore(dataset).body());
        }
    }

    /**
     * A stale cached listing can 404 the deleted object on the first query; that failure drops the listing.
     * The next query lists the store and must succeed inside the TTL.
     */
    private static Outcome runUntilCurrentStore(String dataset) throws IOException {
        Outcome outcome = run(dataset);
        if (outcome.status() != 200) {
            assertThat(outcome.body(), containsString("not found"));
            outcome = run(dataset);
        }
        assertThat(outcome.status(), equalTo(200));
        return outcome;
    }

    private static void assertCurrentStore(String body) {
        assertThat(body, containsString("Vienna"));
        assertThat("the query re-listed, so the file added after the delete is visible", body, containsString("Prague"));
        assertThat(body, not(containsString("Berlin")));
    }

    private static Outcome run(String dataset) throws IOException {
        Request req = new Request("POST", "/_query");
        req.setJsonEntity("{\"query\":\"FROM " + dataset + " | SORT id | LIMIT 10\"}");
        req.setOptions(req.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        try {
            Response r = client().performRequest(req);
            return new Outcome(
                r.getStatusLine().getStatusCode(),
                EntityUtils.toString(r.getEntity(), StandardCharsets.UTF_8),
                r.getWarnings()
            );
        } catch (ResponseException e) {
            return new Outcome(
                e.getResponse().getStatusLine().getStatusCode(),
                EntityUtils.toString(e.getResponse().getEntity(), StandardCharsets.UTF_8),
                e.getResponse().getWarnings()
            );
        }
    }

    private static void putDataSource() throws IOException {
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject()
                .field("type", "s3")
                .field(
                    "settings",
                    Map.of(
                        "access_key",
                        ACCESS_KEY,
                        "secret_key",
                        SECRET_KEY,
                        "region",
                        regionSupplier.get(),
                        "endpoint",
                        s3HttpFixture.getAddress()
                    )
                )
                .endObject();
            Request req = new Request("PUT", "/_query/data_source/" + DATA_SOURCE);
            req.setJsonEntity(Strings.toString(b));
            req.setOptions(req.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
            assertThat(client().performRequest(req).getStatusLine().getStatusCode(), equalTo(200));
        }
    }

    private static void putDataset(String name, Map<String, Object> extraSettings) throws IOException {
        Map<String, Object> settings = new LinkedHashMap<>();
        settings.put("region", regionSupplier.get());
        settings.putAll(extraSettings);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject()
                .field("data_source", DATA_SOURCE)
                .field("resource", "s3://" + BUCKET + "/glob/*.csv")
                .field("settings", settings)
                .endObject();
            Request req = new Request("PUT", "/_query/dataset/" + name);
            req.setJsonEntity(Strings.toString(b));
            req.setOptions(req.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
            assertThat(client().performRequest(req).getStatusLine().getStatusCode(), equalTo(200));
        }
    }
}
