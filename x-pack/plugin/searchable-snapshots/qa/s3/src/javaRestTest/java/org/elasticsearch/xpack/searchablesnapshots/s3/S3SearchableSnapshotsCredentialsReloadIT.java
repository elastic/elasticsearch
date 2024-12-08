/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.s3;

import fixture.s3.S3HttpFixture;
import io.netty.handler.codec.http.HttpMethod;

import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.function.UnaryOperator;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.allOf;

public class S3SearchableSnapshotsCredentialsReloadIT extends ESRestTestCase {

    private static final String BUCKET = "S3SearchableSnapshotsCredentialsReloadIT-bucket";
    private static final String BASE_PATH = "S3SearchableSnapshotsCredentialsReloadIT-base-path";

    private static volatile String repositoryAccessKey;

    public static final S3HttpFixture s3Fixture = new S3HttpFixture(
        true,
        BUCKET,
        BASE_PATH,
        S3HttpFixture.mutableAccessKey(() -> repositoryAccessKey)
    );

    private static final MutableSettingsProvider keystoreSettings = new MutableSettingsProvider();

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .keystore(keystoreSettings)
        .setting("xpack.searchable.snapshot.shared_cache.size", "4kB")
        .setting("xpack.searchable.snapshot.shared_cache.region_size", "4kB")
        .setting("xpack.searchable_snapshots.cache_fetch_async_thread_pool.keep_alive", "0ms")
        .setting("xpack.security.enabled", "false")
        .systemProperty("es.allow_insecure_settings", "true")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void skipFips() {
        assumeFalse("getting these tests to run in a FIPS JVM is kinda fiddly and we don't really need the extra coverage", inFipsJvm());
    }

    public void testReloadCredentialsFromKeystore() throws IOException {
        final TestHarness testHarness = new TestHarness();
        testHarness.putRepository();

        // Set up initial credentials
        final String accessKey1 = randomIdentifier();
        repositoryAccessKey = accessKey1;
        keystoreSettings.put("s3.client.default.access_key", accessKey1);
        keystoreSettings.put("s3.client.default.secret_key", randomIdentifier());
        cluster.updateStoredSecureSettings();
        assertOK(client().performRequest(new Request("POST", "/_nodes/reload_secure_settings")));

        testHarness.createFrozenSearchableSnapshotIndex();

        // Verify searchable snapshot functionality
        testHarness.ensureSearchSuccess();

        // Rotate credentials in blob store
        logger.info("--> rotate credentials");
        final String accessKey2 = randomValueOtherThan(accessKey1, ESTestCase::randomIdentifier);
        repositoryAccessKey = accessKey2;

        // Ensure searchable snapshot now does not work due to invalid credentials
        logger.info("--> expect failure");
        testHarness.ensureSearchFailure();

        // Set up refreshed credentials
        logger.info("--> update keystore contents");
        keystoreSettings.put("s3.client.default.access_key", accessKey2);
        cluster.updateStoredSecureSettings();
        assertOK(client().performRequest(new Request("POST", "/_nodes/reload_secure_settings")));

        // Check access using refreshed credentials
        logger.info("--> expect success");
        testHarness.ensureSearchSuccess();
    }

    public void testReloadCredentialsFromAlternativeClient() throws IOException {
        final TestHarness testHarness = new TestHarness();
        testHarness.putRepository();

        // Set up credentials
        final String accessKey1 = randomIdentifier();
        final String accessKey2 = randomValueOtherThan(accessKey1, ESTestCase::randomIdentifier);
        final String alternativeClient = randomValueOtherThan("default", ESTestCase::randomIdentifier);

        repositoryAccessKey = accessKey1;
        keystoreSettings.put("s3.client.default.access_key", accessKey1);
        keystoreSettings.put("s3.client.default.secret_key", randomIdentifier());
        keystoreSettings.put("s3.client." + alternativeClient + ".access_key", accessKey2);
        keystoreSettings.put("s3.client." + alternativeClient + ".secret_key", randomIdentifier());
        cluster.updateStoredSecureSettings();
        assertOK(client().performRequest(new Request("POST", "/_nodes/reload_secure_settings")));

        testHarness.createFrozenSearchableSnapshotIndex();

        // Verify searchable snapshot functionality
        testHarness.ensureSearchSuccess();

        // Rotate credentials in blob store
        logger.info("--> rotate credentials");
        repositoryAccessKey = accessKey2;

        // Ensure searchable snapshot now does not work due to invalid credentials
        logger.info("--> expect failure");
        testHarness.ensureSearchFailure();

        // Adjust repository to use new client
        logger.info("--> update repository metadata");
        testHarness.putRepository(b -> b.put("client", alternativeClient));

        // Check access using refreshed credentials
        logger.info("--> expect success");
        testHarness.ensureSearchSuccess();
    }

    public void testReloadCredentialsFromMetadata() throws IOException {
        final TestHarness testHarness = new TestHarness();
        testHarness.warningsHandler = WarningsHandler.PERMISSIVE;

        // Set up credentials
        final String accessKey1 = randomIdentifier();
        final String accessKey2 = randomValueOtherThan(accessKey1, ESTestCase::randomIdentifier);

        testHarness.putRepository(b -> b.put("access_key", accessKey1).put("secret_key", randomIdentifier()));
        repositoryAccessKey = accessKey1;

        testHarness.createFrozenSearchableSnapshotIndex();

        // Verify searchable snapshot functionality
        testHarness.ensureSearchSuccess();

        // Rotate credentials in blob store
        logger.info("--> rotate credentials");
        repositoryAccessKey = accessKey2;

        // Ensure searchable snapshot now does not work due to invalid credentials
        logger.info("--> expect failure");
        testHarness.ensureSearchFailure();

        // Adjust repository to use new client
        logger.info("--> update repository metadata");
        testHarness.putRepository(b -> b.put("access_key", accessKey2).put("secret_key", randomIdentifier()));

        // Check access using refreshed credentials
        logger.info("--> expect success");
        testHarness.ensureSearchSuccess();
    }

    private class TestHarness {
        private final String mountedIndexName = randomIdentifier();
        private final String repositoryName = randomIdentifier();

        @Nullable // to use the default
        WarningsHandler warningsHandler;

        void putRepository() throws IOException {
            putRepository(UnaryOperator.identity());
        }

        void putRepository(UnaryOperator<Settings.Builder> settingsOperator) throws IOException {
            // Register repository
            final Request request = newXContentRequest(
                HttpMethod.PUT,
                "/_snapshot/" + repositoryName,
                (b, p) -> b.field("type", "s3")
                    .startObject("settings")
                    .value(
                        settingsOperator.apply(
                            Settings.builder().put("bucket", BUCKET).put("base_path", BASE_PATH).put("endpoint", s3Fixture.getAddress())
                        ).build()
                    )
                    .endObject()
            );
            request.addParameter("verify", "false"); // because we don't have access to the blob store yet
            request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(warningsHandler));
            assertOK(client().performRequest(request));
        }

        void createFrozenSearchableSnapshotIndex() throws IOException {
            // Create an index, large enough that its data is not all captured in the file headers
            final String indexName = randomValueOtherThan(mountedIndexName, ESTestCase::randomIdentifier);
            createIndex(indexName, indexSettings(1, 0).build());
            try (var bodyStream = new ByteArrayOutputStream()) {
                for (int i = 0; i < 1024; i++) {
                    try (XContentBuilder bodyLineBuilder = new XContentBuilder(XContentType.JSON.xContent(), bodyStream)) {
                        bodyLineBuilder.startObject().startObject("index").endObject().endObject();
                    }
                    bodyStream.write(0x0a);
                    try (XContentBuilder bodyLineBuilder = new XContentBuilder(XContentType.JSON.xContent(), bodyStream)) {
                        bodyLineBuilder.startObject().field("foo", "bar").endObject();
                    }
                    bodyStream.write(0x0a);
                }
                bodyStream.flush();
                final Request request = new Request("PUT", indexName + "/_bulk");
                request.setEntity(new ByteArrayEntity(bodyStream.toByteArray(), ContentType.APPLICATION_JSON));
                client().performRequest(request);
            }

            // Take a snapshot and delete the original index
            final String snapshotName = randomIdentifier();
            final Request createSnapshotRequest = new Request(HttpPut.METHOD_NAME, "_snapshot/" + repositoryName + '/' + snapshotName);
            createSnapshotRequest.addParameter("wait_for_completion", "true");
            createSnapshotRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(warningsHandler));
            assertOK(client().performRequest(createSnapshotRequest));

            deleteIndex(indexName);

            // Mount the snapshotted index as a searchable snapshot
            final Request mountRequest = newXContentRequest(
                HttpMethod.POST,
                "/_snapshot/" + repositoryName + "/" + snapshotName + "/_mount",
                (b, p) -> b.field("index", indexName).field("renamed_index", mountedIndexName)
            );
            mountRequest.addParameter("wait_for_completion", "true");
            mountRequest.addParameter("storage", "shared_cache");
            assertOK(client().performRequest(mountRequest));
            ensureGreen(mountedIndexName);
        }

        void ensureSearchSuccess() throws IOException {
            final Request searchRequest = new Request("GET", mountedIndexName + "/_search");
            searchRequest.addParameter("size", "10000");
            assertEquals(
                "bar",
                ObjectPath.createFromResponse(assertOK(client().performRequest(searchRequest))).evaluate("hits.hits.0._source.foo")
            );
        }

        void ensureSearchFailure() throws IOException {
            assertOK(client().performRequest(new Request("POST", "/_searchable_snapshots/cache/clear")));
            final Request searchRequest = new Request("GET", mountedIndexName + "/_search");
            searchRequest.addParameter("size", "10000");
            assertThat(
                expectThrows(ResponseException.class, () -> client().performRequest(searchRequest)).getMessage(),
                allOf(
                    containsString("Access denied"),
                    containsString("Status Code: 403"),
                    containsString("Error Code: AccessDenied"),
                    containsString("failed to read data from cache")
                )
            );
        }
    }

}
