/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import fixture.azure.AzureHttpFixture;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Set;

/**
 * Shared utilities for Azure fixture-based integration tests.
 * Provides fixture infrastructure for testing ESQL external data sources with Azure Blob Storage.
 */
public final class AzureFixtureUtils {

    private static final Logger logger = LogManager.getLogger(AzureFixtureUtils.class);

    /** Default Azure account name for test fixtures */
    public static final String ACCOUNT = "testazureaccount";

    /** Default Azure storage key for test fixtures (base64-encoded, valid format for SharedKey auth) */
    public static final String KEY = "dGVzdGtleTEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNA==";

    /** Container name - matches S3 BUCKET for consistent path structure */
    public static final String CONTAINER = S3FixtureUtils.BUCKET;

    /** Resource path for test fixtures */
    private static final String FIXTURES_RESOURCE_PATH = "/iceberg-fixtures";

    private AzureFixtureUtils() {}

    /**
     * Create a BlobServiceClient configured to use the Azure HTTP fixture.
     *
     * @param fixtureAddress the fixture address (e.g. "http://localhost:port/account")
     * @return a BlobServiceClient configured for the fixture
     */
    public static BlobServiceClient createBlobServiceClient(String fixtureAddress) {
        return new BlobServiceClientBuilder().endpoint(fixtureAddress)
            .credential(new StorageSharedKeyCredential(ACCOUNT, KEY))
            .buildClient();
    }

    /**
     * Upload a blob to the Azure fixture.
     *
     * @param fixtureAddress the fixture address (e.g. "http://localhost:port/account")
     * @param key the blob key (e.g. "warehouse/standalone/employees.ndjson.bz2")
     * @param content the blob content
     */
    public static void addBlobToFixture(String fixtureAddress, String key, byte[] content) {
        try {
            BlobServiceClient blobServiceClient = createBlobServiceClient(fixtureAddress);
            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(CONTAINER);
            BlobClient blobClient = containerClient.getBlobClient(key);
            blobClient.upload(new java.io.ByteArrayInputStream(content), content.length, true);
        } catch (Exception e) {
            logger.error("Failed to add blob [{}] to Azure fixture", key, e);
            throw e;
        }
    }

    /**
     * Load test fixtures from the classpath resources into the Azure fixture.
     *
     * @param fixtureAddress the fixture address (e.g. "http://localhost:port/account")
     */
    public static void loadFixturesFromResources(String fixtureAddress) {
        try {
            var resourceUrl = AzureFixtureUtils.class.getResource(FIXTURES_RESOURCE_PATH);
            assert resourceUrl != null : "Fixtures resource path not found: " + FIXTURES_RESOURCE_PATH;
            assert resourceUrl.getProtocol().equals("file") : "Fixtures resource path must be a file: " + resourceUrl;

            Path fixturesPath = Paths.get(resourceUrl.toURI());
            loadFixturesFromPath(fixtureAddress, fixturesPath);
        } catch (Exception e) {
            logger.error("Failed to load fixtures from resources", e);
            throw new RuntimeException(e);
        }
    }

    private static void loadFixturesFromPath(String fixtureAddress, Path fixturesPath) throws IOException, URISyntaxException {
        if (Files.exists(fixturesPath) == false) {
            logger.warn("Fixtures path does not exist: {}", fixturesPath);
            return;
        }

        BlobServiceClient blobServiceClient = createBlobServiceClient(fixtureAddress);
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(CONTAINER);

        Set<String> loadedFiles = new HashSet<>();

        Files.walkFileTree(fixturesPath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                String relativePath = fixturesPath.relativize(file).toString();
                String blobName = S3FixtureUtils.WAREHOUSE + "/" + relativePath;

                byte[] content = Files.readAllBytes(file);
                BlobClient blobClient = containerClient.getBlobClient(blobName);
                blobClient.upload(new java.io.ByteArrayInputStream(content), content.length, true);

                loadedFiles.add(blobName);
                return FileVisitResult.CONTINUE;
            }
        });

        logger.info("Loaded {} fixture files into Azure fixture", loadedFiles.size());
    }

    /**
     * Extended AzureHttpFixture that automatically loads test fixtures from resources.
     */
    public static class DataSourcesAzureHttpFixture extends AzureHttpFixture {

        public DataSourcesAzureHttpFixture() {
            super(
                AzureHttpFixture.Protocol.HTTP,
                ACCOUNT,
                CONTAINER,
                null,
                null,
                AzureHttpFixture.sharedKeyForAccountPredicate(ACCOUNT),
                (currentLeaseId, requestLeaseId) -> false
            );
        }

        /**
         * Load test fixtures from the classpath resources into the Azure fixture.
         * Must be called after the fixture has started (e.g. in @BeforeClass).
         */
        public void loadFixturesFromResources() {
            AzureFixtureUtils.loadFixturesFromResources(getAddress());
        }
    }
}
