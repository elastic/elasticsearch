/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import io.netty.handler.codec.http.HttpMethod;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractRepositoryS3RestTestCase extends ESRestTestCase {

    public record TestRepository(String repositoryName, String clientName, String bucketName, String basePath) {

        public Closeable register() throws IOException {
            return register(UnaryOperator.identity());
        }

        public Closeable register(UnaryOperator<Settings> settingsUnaryOperator) throws IOException {
            assertOK(client().performRequest(getRegisterRequest(settingsUnaryOperator)));
            return () -> assertOK(client().performRequest(new Request("DELETE", "/_snapshot/" + repositoryName())));
        }

        private Request getRegisterRequest(UnaryOperator<Settings> settingsUnaryOperator) throws IOException {
            return newXContentRequest(
                HttpMethod.PUT,
                "/_snapshot/" + repositoryName(),
                (b, p) -> b.field("type", S3Repository.TYPE)
                    .startObject("settings")
                    .value(
                        settingsUnaryOperator.apply(
                            Settings.builder()
                                .put("bucket", bucketName())
                                .put("base_path", basePath())
                                .put("client", clientName())
                                .put("canned_acl", "private")
                                .put("storage_class", "standard")
                                .put("disable_chunked_encoding", randomBoolean())
                                .build()
                        )
                    )
                    .endObject()
            );
        }
    }

    protected abstract String getBucketName();

    protected abstract String getBasePath();

    protected abstract String getClientName();

    protected static String getIdentifierPrefix(String testSuiteName) {
        return testSuiteName + "-" + Integer.toString(Murmur3HashFunction.hash(testSuiteName + System.getProperty("tests.seed")), 16) + "-";
    }

    private TestRepository newTestRepository() {
        return new TestRepository(randomIdentifier(), getClientName(), getBucketName(), getBasePath());
    }

    private static UnaryOperator<Settings> readonlyOperator(Boolean readonly) {
        return readonly == null
            ? UnaryOperator.identity()
            : s -> Settings.builder().put(s).put(BlobStoreRepository.READONLY_SETTING_KEY, readonly).build();
    }

    public void testGetRepository() throws IOException {
        testGetRepository(null);
    }

    public void testGetRepositoryReadonlyTrue() throws IOException {
        testGetRepository(Boolean.TRUE);
    }

    public void testGetRepositoryReadonlyFalse() throws IOException {
        testGetRepository(Boolean.FALSE);
    }

    private void testGetRepository(Boolean readonly) throws IOException {
        final var repository = newTestRepository();
        try (var ignored = repository.register(readonlyOperator(readonly))) {
            final var repositoryName = repository.repositoryName();
            final var responseObjectPath = assertOKAndCreateObjectPath(
                client().performRequest(new Request("GET", "/_snapshot/" + repositoryName))
            );

            assertEquals("s3", responseObjectPath.evaluate(repositoryName + ".type"));
            assertNotNull(responseObjectPath.evaluate(repositoryName + ".settings"));
            assertEquals(repository.bucketName(), responseObjectPath.evaluate(repositoryName + ".settings.bucket"));
            assertEquals(repository.clientName(), responseObjectPath.evaluate(repositoryName + ".settings.client"));
            assertEquals(repository.basePath(), responseObjectPath.evaluate(repositoryName + ".settings.base_path"));
            assertEquals("private", responseObjectPath.evaluate(repositoryName + ".settings.canned_acl"));
            assertEquals("standard", responseObjectPath.evaluate(repositoryName + ".settings.storage_class"));
            assertNull(responseObjectPath.evaluate(repositoryName + ".settings.access_key"));
            assertNull(responseObjectPath.evaluate(repositoryName + ".settings.secret_key"));
            assertNull(responseObjectPath.evaluate(repositoryName + ".settings.session_token"));

            if (readonly == null) {
                assertNull(responseObjectPath.evaluate(repositoryName + ".settings." + BlobStoreRepository.READONLY_SETTING_KEY));
            } else {
                assertEquals(
                    Boolean.toString(readonly),
                    responseObjectPath.evaluate(repositoryName + ".settings." + BlobStoreRepository.READONLY_SETTING_KEY)
                );
            }
        }
    }

    public void testNonexistentBucket() throws Exception {
        testNonexistentBucket(null);
    }

    public void testNonexistentBucketReadonlyTrue() throws Exception {
        testNonexistentBucket(Boolean.TRUE);
    }

    public void testNonexistentBucketReadonlyFalse() throws Exception {
        testNonexistentBucket(Boolean.FALSE);
    }

    private void testNonexistentBucket(Boolean readonly) throws Exception {
        final var repository = new TestRepository(
            randomIdentifier(),
            getClientName(),
            randomValueOtherThan(getBucketName(), ESTestCase::randomIdentifier),
            getBasePath()
        );
        final var registerRequest = repository.getRegisterRequest(readonlyOperator(readonly));

        final var responseException = expectThrows(ResponseException.class, () -> client().performRequest(registerRequest));
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR.getStatus(), responseException.getResponse().getStatusLine().getStatusCode());
        assertThat(
            responseException.getMessage(),
            allOf(containsString("repository_verification_exception"), containsString("is not accessible on master node"))
        );
    }

    public void testNonexistentClient() throws Exception {
        testNonexistentClient(null);
    }

    public void testNonexistentClientReadonlyTrue() throws Exception {
        testNonexistentClient(Boolean.TRUE);
    }

    public void testNonexistentClientReadonlyFalse() throws Exception {
        testNonexistentClient(Boolean.FALSE);
    }

    private void testNonexistentClient(Boolean readonly) throws Exception {
        final var repository = new TestRepository(
            randomIdentifier(),
            randomValueOtherThanMany(c -> c.equals(getClientName()) || c.equals("default"), ESTestCase::randomIdentifier),
            getBucketName(),
            getBasePath()
        );
        final var registerRequest = repository.getRegisterRequest(readonlyOperator(readonly));

        final var responseException = expectThrows(ResponseException.class, () -> client().performRequest(registerRequest));
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR.getStatus(), responseException.getResponse().getStatusLine().getStatusCode());
        assertThat(
            responseException.getMessage(),
            allOf(
                containsString("repository_verification_exception"),
                containsString("is not accessible on master node"),
                containsString("illegal_argument_exception"),
                containsString("Unknown s3 client name")
            )
        );
    }

    public void testNonexistentSnapshot() throws Exception {
        testNonexistentSnapshot(null);
    }

    public void testNonexistentSnapshotReadonlyTrue() throws Exception {
        testNonexistentSnapshot(Boolean.TRUE);
    }

    public void testNonexistentSnapshotReadonlyFalse() throws Exception {
        testNonexistentSnapshot(Boolean.FALSE);
    }

    private void testNonexistentSnapshot(Boolean readonly) throws Exception {
        final var repository = newTestRepository();
        try (var ignored = repository.register(readonlyOperator(readonly))) {
            final var repositoryName = repository.repositoryName();

            final var getSnapshotRequest = new Request("GET", "/_snapshot/" + repositoryName + "/" + randomIdentifier());
            final var getSnapshotException = expectThrows(ResponseException.class, () -> client().performRequest(getSnapshotRequest));
            assertEquals(RestStatus.NOT_FOUND.getStatus(), getSnapshotException.getResponse().getStatusLine().getStatusCode());
            assertThat(getSnapshotException.getMessage(), containsString("snapshot_missing_exception"));

            final var restoreRequest = new Request("POST", "/_snapshot/" + repositoryName + "/" + randomIdentifier() + "/_restore");
            if (randomBoolean()) {
                restoreRequest.addParameter("wait_for_completion", Boolean.toString(randomBoolean()));
            }
            final var restoreException = expectThrows(ResponseException.class, () -> client().performRequest(restoreRequest));
            assertEquals(RestStatus.INTERNAL_SERVER_ERROR.getStatus(), restoreException.getResponse().getStatusLine().getStatusCode());
            assertThat(restoreException.getMessage(), containsString("snapshot_restore_exception"));

            if (readonly != Boolean.TRUE) {
                final var deleteRequest = new Request("DELETE", "/_snapshot/" + repositoryName + "/" + randomIdentifier());
                final var deleteException = expectThrows(ResponseException.class, () -> client().performRequest(deleteRequest));
                assertEquals(RestStatus.NOT_FOUND.getStatus(), deleteException.getResponse().getStatusLine().getStatusCode());
                assertThat(deleteException.getMessage(), containsString("snapshot_missing_exception"));
            }
        }
    }

    public void testUsageStats() throws Exception {
        testUsageStats(null);
    }

    public void testUsageStatsReadonlyTrue() throws Exception {
        testUsageStats(Boolean.TRUE);
    }

    public void testUsageStatsReadonlyFalse() throws Exception {
        testUsageStats(Boolean.FALSE);
    }

    private void testUsageStats(Boolean readonly) throws Exception {
        final var repository = newTestRepository();
        try (var ignored = repository.register(readonlyOperator(readonly))) {
            final var responseObjectPath = assertOKAndCreateObjectPath(client().performRequest(new Request("GET", "/_cluster/stats")));
            assertThat(responseObjectPath.evaluate("repositories.s3.count"), equalTo(1));

            if (readonly == Boolean.TRUE) {
                assertThat(responseObjectPath.evaluate("repositories.s3.read_only"), equalTo(1));
                assertNull(responseObjectPath.evaluate("repositories.s3.read_write"));
            } else {
                assertNull(responseObjectPath.evaluate("repositories.s3.read_only"));
                assertThat(responseObjectPath.evaluate("repositories.s3.read_write"), equalTo(1));
            }
        }
    }

    public void testSnapshotAndRestore() throws Exception {
        final var repository = newTestRepository();
        try (var ignored = repository.register()) {
            final var repositoryName = repository.repositoryName();
            final var indexName = randomIdentifier();
            final var snapshotsToDelete = new ArrayList<String>(2);

            try {
                indexDocuments(indexName, """
                    {"index":{"_id":"1"}}
                    {"snapshot":"one"}
                    {"index":{"_id":"2"}}
                    {"snapshot":"one"}
                    {"index":{"_id":"3"}}
                    {"snapshot":"one"}
                    """, 3);

                // create the first snapshot
                final var snapshot1Name = randomIdentifier();
                createSnapshot(repositoryName, snapshotsToDelete, snapshot1Name);

                // check the first snapshot's status
                {
                    final var snapshotStatusResponse = assertOKAndCreateObjectPath(
                        client().performRequest(new Request("GET", "/_snapshot/" + repositoryName + "/" + snapshot1Name + "/_status"))
                    );
                    assertEquals(snapshot1Name, snapshotStatusResponse.evaluate("snapshots.0.snapshot"));
                    assertEquals("SUCCESS", snapshotStatusResponse.evaluate("snapshots.0.state"));
                }

                // add more documents to the index
                indexDocuments(indexName, """
                    {"index":{"_id":"4"}}
                    {"snapshot":"one"}
                    {"index":{"_id":"5"}}
                    {"snapshot":"one"}
                    {"index":{"_id":"6"}}
                    {"snapshot":"one"}
                    {"index":{"_id":"7"}}
                    {"snapshot":"one"}
                    """, 7);

                // create the second snapshot
                final var snapshot2Name = randomValueOtherThan(snapshot1Name, ESTestCase::randomIdentifier);
                createSnapshot(repositoryName, snapshotsToDelete, snapshot2Name);

                // list the snapshots
                {
                    final var listSnapshotsResponse = assertOKAndCreateObjectPath(
                        client().performRequest(
                            new Request("GET", "/_snapshot/" + repositoryName + "/" + snapshot1Name + "," + snapshot2Name)
                        )
                    );
                    assertEquals(2, listSnapshotsResponse.evaluateArraySize("snapshots"));
                    assertEquals(
                        Set.of(snapshot1Name, snapshot2Name),
                        Set.of(
                            listSnapshotsResponse.evaluate("snapshots.0.snapshot"),
                            listSnapshotsResponse.evaluate("snapshots.1.snapshot")
                        )
                    );
                    assertEquals("SUCCESS", listSnapshotsResponse.evaluate("snapshots.0.state"));
                    assertEquals("SUCCESS", listSnapshotsResponse.evaluate("snapshots.1.state"));
                }

                // delete and restore the index from snapshot 2
                deleteAndRestoreIndex(indexName, repositoryName, snapshot2Name, 7);

                // delete and restore the index from snapshot 1
                deleteAndRestoreIndex(indexName, repositoryName, snapshot1Name, 3);
            } finally {
                if (snapshotsToDelete.isEmpty() == false) {
                    assertOK(
                        client().performRequest(
                            new Request(
                                "DELETE",
                                "/_snapshot/" + repositoryName + "/" + snapshotsToDelete.stream().collect(Collectors.joining(","))
                            )
                        )
                    );
                }
            }
        }
    }

    private static void deleteAndRestoreIndex(String indexName, String repositoryName, String snapshot2Name, int expectedDocCount)
        throws IOException {
        assertOK(client().performRequest(new Request("DELETE", "/" + indexName)));
        final var restoreRequest = new Request("POST", "/_snapshot/" + repositoryName + "/" + snapshot2Name + "/_restore");
        restoreRequest.addParameter("wait_for_completion", "true");
        assertOK(client().performRequest(restoreRequest));
        assertIndexDocCount(indexName, expectedDocCount);
    }

    private static void indexDocuments(String indexName, String body, int expectedDocCount) throws IOException {
        // create and populate an index
        final var indexDocsRequest = new Request("POST", "/" + indexName + "/_bulk");
        indexDocsRequest.addParameter("refresh", "true");
        indexDocsRequest.setJsonEntity(body);
        assertFalse(assertOKAndCreateObjectPath(client().performRequest(indexDocsRequest)).evaluate("errors"));

        // check the index contents
        assertIndexDocCount(indexName, expectedDocCount);
    }

    private static void createSnapshot(String repositoryName, ArrayList<String> snapshotsToDelete, String snapshotName) throws IOException {
        final var createSnapshotRequest = new Request("POST", "/_snapshot/" + repositoryName + "/" + snapshotName);
        createSnapshotRequest.addParameter("wait_for_completion", "true");
        final var createSnapshotResponse = assertOKAndCreateObjectPath(client().performRequest(createSnapshotRequest));
        snapshotsToDelete.add(snapshotName);
        assertEquals(snapshotName, createSnapshotResponse.evaluate("snapshot.snapshot"));
        assertEquals("SUCCESS", createSnapshotResponse.evaluate("snapshot.state"));
        assertThat(createSnapshotResponse.evaluate("snapshot.shards.failed"), equalTo(0));
    }

    private static void assertIndexDocCount(String indexName, int expectedCount) throws IOException {
        assertThat(
            assertOKAndCreateObjectPath(client().performRequest(new Request("GET", "/" + indexName + "/_count"))).evaluate("count"),
            equalTo(expectedCount)
        );
    }
}
