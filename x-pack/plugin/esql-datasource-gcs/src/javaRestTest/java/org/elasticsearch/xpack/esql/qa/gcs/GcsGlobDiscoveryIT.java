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
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * GCS discovery tests using GoogleCloudStorageHttpFixture with empty blobs.
 * Validates that GcsStorageProvider.listObjects() returns correct entries
 * and that glob-style filtering works against GCS listings.
 * <p>
 * Mirrors the S3GlobDiscoveryIT test pattern from the Parquet QA module.
 */
public class GcsGlobDiscoveryIT extends ESTestCase {

    private static final String BUCKET = "test-bucket";
    private static final String TOKEN = "test-token";

    @ClassRule
    public static GoogleCloudStorageHttpFixture gcsFixture = new GoogleCloudStorageHttpFixture(true, BUCKET, TOKEN);

    private static GcsStorageProvider provider;
    private static Storage storage;

    private static final String DISCOVER_PREFIX = "warehouse/discover";

    @BeforeClass
    public static void setupProvider() throws Exception {
        String endpoint = gcsFixture.getAddress();
        storage = buildTestStorageClient(endpoint);

        // Upload empty blobs for discovery via the GCS client
        uploadTestBlob(DISCOVER_PREFIX + "/flat/a.parquet", new byte[0]);
        uploadTestBlob(DISCOVER_PREFIX + "/flat/b.parquet", new byte[0]);
        uploadTestBlob(DISCOVER_PREFIX + "/flat/c.csv", new byte[0]);
        uploadTestBlob(DISCOVER_PREFIX + "/nested/x/d.parquet", new byte[0]);
        uploadTestBlob(DISCOVER_PREFIX + "/nested/y/e.parquet", new byte[0]);

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

    private static void uploadTestBlob(String key, byte[] content) {
        BlobInfo blobInfo = BlobInfo.newBuilder(BUCKET, key).build();
        storage.create(blobInfo, content);
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

    public void testGcsFlatListing() throws IOException {
        StoragePath prefix = StoragePath.of("gs://" + BUCKET + "/" + DISCOVER_PREFIX + "/flat");
        List<StorageEntry> entries = collectAll(provider.listObjects(prefix, true));

        List<String> names = entries.stream().map(e -> e.path().objectName()).sorted().toList();
        assertEquals(List.of("a.parquet", "b.parquet", "c.csv"), names);
    }

    public void testGcsFlatGlobFiltering() throws IOException {
        StoragePath prefix = StoragePath.of("gs://" + BUCKET + "/" + DISCOVER_PREFIX + "/flat");
        List<StorageEntry> entries = collectAll(provider.listObjects(prefix, true));

        // Simulate *.parquet glob filtering
        Pattern parquetPattern = Pattern.compile("[^/]*\\.parquet");
        List<StorageEntry> matched = new ArrayList<>();
        for (StorageEntry e : entries) {
            if (parquetPattern.matcher(e.path().objectName()).matches()) {
                matched.add(e);
            }
        }

        assertEquals(2, matched.size());
    }

    public void testGcsRecursiveGlobFiltering() throws IOException {
        StoragePath prefix = StoragePath.of("gs://" + BUCKET + "/" + DISCOVER_PREFIX);
        List<StorageEntry> entries = collectAll(provider.listObjects(prefix, true));

        // Simulate **/*.parquet: match any .parquet file at any depth
        String prefixStr = "gs://" + BUCKET + "/" + DISCOVER_PREFIX + "/";
        List<StorageEntry> matched = new ArrayList<>();
        for (StorageEntry e : entries) {
            String fullPath = e.path().toString();
            String relativePath = fullPath.startsWith(prefixStr) ? fullPath.substring(prefixStr.length()) : e.path().objectName();
            if (relativePath.endsWith(".parquet")) {
                matched.add(e);
            }
        }

        assertEquals(4, matched.size());
    }

    public void testGcsNoMatchReturnsEmpty() throws IOException {
        StoragePath prefix = StoragePath.of("gs://" + BUCKET + "/" + DISCOVER_PREFIX + "/flat");
        List<StorageEntry> entries = collectAll(provider.listObjects(prefix, true));

        // Simulate *.json glob filtering — no matches expected
        Pattern jsonPattern = Pattern.compile("[^/]*\\.json");
        List<StorageEntry> matched = new ArrayList<>();
        for (StorageEntry e : entries) {
            if (jsonPattern.matcher(e.path().objectName()).matches()) {
                matched.add(e);
            }
        }

        assertEquals(0, matched.size());
    }

    public void testGcsBraceAlternativesFiltering() throws IOException {
        StoragePath prefix = StoragePath.of("gs://" + BUCKET + "/" + DISCOVER_PREFIX + "/flat");
        List<StorageEntry> entries = collectAll(provider.listObjects(prefix, true));

        // Simulate *.{parquet,csv} glob filtering
        Pattern bracePattern = Pattern.compile("[^/]*\\.(?:parquet|csv)");
        List<StorageEntry> matched = new ArrayList<>();
        for (StorageEntry e : entries) {
            if (bracePattern.matcher(e.path().objectName()).matches()) {
                matched.add(e);
            }
        }

        assertEquals(3, matched.size());
    }

    public void testGcsNonRecursiveListing() throws IOException {
        StoragePath prefix = StoragePath.of("gs://" + BUCKET + "/" + DISCOVER_PREFIX + "/flat");
        // Non-recursive listing should use delimiter to only show immediate children
        List<StorageEntry> entries = collectAll(provider.listObjects(prefix, false));

        // In a flat listing, we should get the 3 files directly under /flat/
        List<String> names = entries.stream().map(e -> e.path().objectName()).sorted().toList();
        assertEquals(List.of("a.parquet", "b.parquet", "c.csv"), names);
    }

    private static List<StorageEntry> collectAll(StorageIterator iterator) throws IOException {
        List<StorageEntry> entries = new ArrayList<>();
        try (iterator) {
            while (iterator.hasNext()) {
                entries.add(iterator.next());
            }
        }
        return entries;
    }
}
