/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.s3;

import fixture.s3.S3HttpFixture;
import io.netty.handler.codec.http.HttpMethod;

import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.allOf;

public class S3SearchableSnapshotsCredentialsReloadIT extends ESRestTestCase {

    private static final String BUCKET = "S3SearchableSnapshotsCredentialsReloadIT-bucket";
    private static final String BASE_PATH = "S3SearchableSnapshotsCredentialsReloadIT-base-path";

    public static final S3HttpFixture s3Fixture = new S3HttpFixture(true, BUCKET, BASE_PATH, "ignored");

    private static final MutableSettingsProvider keystoreSettings = new MutableSettingsProvider();

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .keystore(keystoreSettings)
        .setting("s3.client.default.endpoint", s3Fixture::getAddress)
        .setting("xpack.searchable.snapshot.shared_cache.size", "4kB")
        .setting("xpack.searchable.snapshot.shared_cache.region_size", "4kB")
        .setting("xpack.searchable_snapshots.cache_fetch_async_thread_pool.keep_alive", "0ms")
        .setting("xpack.security.enabled", "false")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testReloadCredentialsFromKeystore() throws IOException {

        // Create an index, large enough that its data is not all captured in the file headers
        final var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        try (var bodyStream = new ByteArrayOutputStream()) {
            for (int i = 0; i < 1024; i++) {
                try (var bodyLineBuilder = new XContentBuilder(XContentType.JSON.xContent(), bodyStream)) {
                    bodyLineBuilder.startObject().startObject("index").endObject().endObject();
                }
                bodyStream.write(0x0a);
                try (var bodyLineBuilder = new XContentBuilder(XContentType.JSON.xContent(), bodyStream)) {
                    bodyLineBuilder.startObject().field("foo", "bar").endObject();
                }
                bodyStream.write(0x0a);
            }
            bodyStream.flush();
            final var request = new Request("PUT", indexName + "/_bulk");
            request.setEntity(new ByteArrayEntity(bodyStream.toByteArray(), ContentType.APPLICATION_JSON));
            client().performRequest(request);
        }

        // Register repository (?verify=false because we don't have access to the blob store yet)
        final var repositoryName = randomIdentifier();
        registerRepository(repositoryName, "s3", false, Settings.builder().put("bucket", BUCKET).put("base_path", BASE_PATH).build());
        final var verifyRequest = new Request("POST", "/_snapshot/" + repositoryName + "/_verify");

        // Set up initial credentials
        final var accessKey1 = randomIdentifier();
        s3Fixture.setAccessKey(accessKey1);
        keystoreSettings.put("s3.client.default.access_key", accessKey1);
        keystoreSettings.put("s3.client.default.secret_key", randomIdentifier());
        cluster.updateStoredSecureSettings();
        assertOK(client().performRequest(new Request("POST", "/_nodes/reload_secure_settings")));

        // Take a snapshot and delete the original index
        final var snapshotName = randomIdentifier();
        createSnapshot(repositoryName, snapshotName, true);
        deleteIndex(indexName);

        // Mount the snapshotted index as a searchable snapshot
        final var mountedIndexName = randomValueOtherThan(indexName, ESTestCase::randomIdentifier);
        final var mountRequest = newXContentRequest(
            HttpMethod.POST,
            "/_snapshot/" + repositoryName + "/" + snapshotName + "/_mount",
            (b, p) -> b.field("index", indexName).field("renamed_index", mountedIndexName)
        );
        mountRequest.addParameter("wait_for_completion", "true");
        mountRequest.addParameter("storage", "shared_cache");
        assertOK(client().performRequest(mountRequest));
        ensureGreen(mountedIndexName);

        // Verify searchable snapshot functionality
        assertEquals(
            "bar",
            ObjectPath.createFromResponse(assertOK(client().performRequest(new Request("GET", mountedIndexName + "/_search"))))
                .evaluate("hits.hits.0._source.foo")
        );

        // Rotate credentials in blob store
        final var accessKey2 = randomValueOtherThan(accessKey1, ESTestCase::randomIdentifier);
        s3Fixture.setAccessKey(accessKey2);

        // Ensure searchable snapshot now does not work due to invalid credentials
        assertOK(client().performRequest(new Request("POST", "/_searchable_snapshots/cache/clear")));
        assertThat(
            expectThrows(ResponseException.class, () -> client().performRequest(new Request("GET", mountedIndexName + "/_search")))
                .getMessage(),
            allOf(
                containsString("Bad access key"),
                containsString("Status Code: 403"),
                containsString("Error Code: AccessDenied"),
                containsString("failed to read data from cache")
            )
        );

        // Set up refreshed credentials
        keystoreSettings.put("s3.client.default.access_key", accessKey2);
        cluster.updateStoredSecureSettings();
        assertOK(client().performRequest(new Request("POST", "/_nodes/reload_secure_settings")));

        // Check access using refreshed credentials
        assertEquals(
            "bar",
            ObjectPath.createFromResponse(assertOK(client().performRequest(new Request("GET", mountedIndexName + "/_search"))))
                .evaluate("hits.hits.0._source.foo")
        );
    }

}
