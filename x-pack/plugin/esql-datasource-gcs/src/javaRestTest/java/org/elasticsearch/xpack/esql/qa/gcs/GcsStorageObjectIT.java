/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.gcs;

import fixture.gcs.GoogleCloudStorageHttpFixture;
import fixture.gcs.TestUtils;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.gcs.GcsStorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 * Integration tests for GcsStorageObject using GoogleCloudStorageHttpFixture.
 * Tests reading objects, range reads, metadata retrieval, and existence checks
 * against a mock GCS server.
 */
public class GcsStorageObjectIT extends ESTestCase {

    private static final String BUCKET = "test-bucket";
    private static final String TOKEN = "test-token";

    @ClassRule
    public static GoogleCloudStorageHttpFixture gcsFixture = new GoogleCloudStorageHttpFixture(true, BUCKET, TOKEN);

    private static GcsStorageProvider provider;
    private static Storage storage;

    private static final String TEST_KEY = "test-data/sample.txt";
    private static final String TEST_CONTENT = "Hello, Google Cloud Storage! This is test content for integration testing.";

    @BeforeClass
    public static void setupProvider() throws Exception {
        String endpoint = gcsFixture.getAddress();
        storage = buildTestStorageClient(endpoint);

        // Upload test content
        BlobInfo blobInfo = BlobInfo.newBuilder(BUCKET, TEST_KEY).build();
        storage.create(blobInfo, TEST_CONTENT.getBytes(StandardCharsets.UTF_8));

        // Build a GcsStorageProvider that uses the same storage client
        provider = new GcsStorageProvider(storage);
    }

    private static Storage buildTestStorageClient(String endpoint) throws Exception {
        byte[] serviceAccountJson = TestUtils.createServiceAccount(random());
        ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(new ByteArrayInputStream(serviceAccountJson))
            .toBuilder()
            .setTokenServerUri(URI.create(endpoint + "/" + TOKEN))
            .build();

        return StorageOptions.newBuilder().setCredentials(credentials).setProjectId("test-project").setHost(endpoint).build().getService();
    }

    @AfterClass
    public static void cleanupProvider() throws Exception {
        if (provider != null) {
            provider.close();
            provider = null;
        }
        if (storage != null) {
            storage.close();
            storage = null;
        }
    }

    public void testFullRead() throws IOException {
        StoragePath path = StoragePath.of("gs://" + BUCKET + "/" + TEST_KEY);
        StorageObject obj = provider.newObject(path);

        try (InputStream stream = obj.newStream()) {
            String content = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(TEST_CONTENT, content);
        }
    }

    public void testRangeRead() throws IOException {
        StoragePath path = StoragePath.of("gs://" + BUCKET + "/" + TEST_KEY);
        StorageObject obj = provider.newObject(path);

        // Read "Hello" (first 5 bytes)
        try (InputStream stream = obj.newStream(0, 5)) {
            String content = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            assertEquals("Hello", content);
        }
    }

    public void testObjectLength() throws IOException {
        StoragePath path = StoragePath.of("gs://" + BUCKET + "/" + TEST_KEY);
        StorageObject obj = provider.newObject(path);

        long length = obj.length();
        assertEquals(TEST_CONTENT.getBytes(StandardCharsets.UTF_8).length, length);
    }

    public void testObjectExists() throws IOException {
        StoragePath path = StoragePath.of("gs://" + BUCKET + "/" + TEST_KEY);
        StorageObject obj = provider.newObject(path);

        assertTrue(obj.exists());
    }

    public void testObjectNotExists() throws IOException {
        StoragePath path = StoragePath.of("gs://" + BUCKET + "/nonexistent/file.txt");
        StorageObject obj = provider.newObject(path);

        assertFalse(obj.exists());
    }

    public void testObjectLastModified() throws IOException {
        StoragePath path = StoragePath.of("gs://" + BUCKET + "/" + TEST_KEY);
        StorageObject obj = provider.newObject(path);

        // lastModified may be null for the fixture, but should not throw
        obj.lastModified();
    }

    public void testObjectPath() {
        StoragePath path = StoragePath.of("gs://" + BUCKET + "/" + TEST_KEY);
        StorageObject obj = provider.newObject(path);

        assertEquals(path, obj.path());
    }

    public void testNewObjectWithPreknownLength() throws IOException {
        StoragePath path = StoragePath.of("gs://" + BUCKET + "/" + TEST_KEY);
        long expectedLength = TEST_CONTENT.getBytes(StandardCharsets.UTF_8).length;
        StorageObject obj = provider.newObject(path, expectedLength);

        // The pre-known length should be returned without a HEAD request
        assertEquals(expectedLength, obj.length());
    }

    public void testProviderExists() throws IOException {
        StoragePath path = StoragePath.of("gs://" + BUCKET + "/" + TEST_KEY);
        assertTrue(provider.exists(path));
    }

    public void testProviderExistsNonexistent() throws IOException {
        StoragePath path = StoragePath.of("gs://" + BUCKET + "/nonexistent/file.txt");
        assertFalse(provider.exists(path));
    }

    public void testProviderSupportedSchemes() {
        assertEquals(java.util.List.of("gs"), provider.supportedSchemes());
    }
}
