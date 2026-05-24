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
     * Supports both filesystem paths and JAR-packaged resources.
     *
     * @param fixtureAddress the fixture address (e.g. "http://localhost:port/account")
     */
    public static void loadFixturesFromResources(String fixtureAddress) {
        try {
            Set<String> loadedKeys = new HashSet<>();
            FixtureUtils.forEachFixtureEntryMergingAllClasspathRoots(AzureFixtureUtils.class.getClassLoader(), (relativePath, content) -> {
                String key = S3FixtureUtils.WAREHOUSE + "/" + relativePath;
                addBlobToFixture(fixtureAddress, key, content);
                loadedKeys.add(key);
            });
            logger.info("Loaded {} fixture file(s) into Azure fixture: {}", loadedKeys.size(), String.join(", ", loadedKeys));
        } catch (Exception e) {
            logger.error("Failed to load fixtures from resources", e);
            throw new RuntimeException(e);
        }
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

        /**
         * Inject Azure endpoint and credentials into the query.
         *
         * @param query the ESQL query containing an EXTERNAL command
         * @return the query with Azure parameters injected
         */
        public String injectParams(String query) {
            String trimmed = query.trim();
            int pipeIndex = FixtureUtils.findFirstPipeAfterExternal(trimmed);

            String externalPart;
            String restOfQuery;

            if (pipeIndex == -1) {
                externalPart = trimmed;
                restOfQuery = "";
            } else {
                externalPart = trimmed.substring(0, pipeIndex).trim();
                restOfQuery = " " + trimmed.substring(pipeIndex);
            }

            StringBuilder entries = new StringBuilder();
            entries.append("\"endpoint\": \"").append(getAddress()).append("\", ");
            entries.append("\"account\": \"").append(ACCOUNT).append("\", ");
            entries.append("\"key\": \"").append(KEY).append("\"");

            return FixtureUtils.injectWithEntries(externalPart, entries.toString()) + restOfQuery;
        }
    }
}
