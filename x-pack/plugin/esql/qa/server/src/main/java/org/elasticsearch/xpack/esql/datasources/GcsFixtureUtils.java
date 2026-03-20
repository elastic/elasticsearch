/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import fixture.gcs.GoogleCloudStorageHttpFixture;
import fixture.gcs.GoogleCloudStorageHttpHandler;
import fixture.gcs.TestUtils;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

/**
 * Shared utilities for GCS fixture-based integration tests.
 * Provides fixture infrastructure for testing ESQL external data sources with Google Cloud Storage.
 */
public final class GcsFixtureUtils {

    private static final Logger logger = LogManager.getLogger(GcsFixtureUtils.class);

    /** Default GCS bucket name for test fixtures */
    public static final String BUCKET = "test-gcs-bucket";

    /** GCS OAuth2 token path used by the GCS fixture */
    public static final String TOKEN = "o/oauth2/token";

    private GcsFixtureUtils() {}

    /**
     * Add a blob to the GCS fixture.
     *
     * @param handler the GCS HTTP handler
     * @param key the blob key (e.g. "warehouse/standalone/employees.ndjson.bz2")
     * @param content the blob content
     */
    public static void addBlobToFixture(GoogleCloudStorageHttpHandler handler, String key, byte[] content) {
        handler.putBlob(key, new BytesArray(content));
    }

    /**
     * Extended GoogleCloudStorageHttpFixture that automatically loads test fixtures from resources.
     */
    public static class DataSourcesGcsHttpFixture extends GoogleCloudStorageHttpFixture {

        private String gcsServiceAccountJson;

        public DataSourcesGcsHttpFixture() {
            super(true, BUCKET, TOKEN);
        }

        /**
         * Load test fixtures from the classpath resources into the GCS fixture.
         * Must be called after the fixture has started (e.g. in @BeforeClass).
         */
        public void loadFixturesFromResources() {
            try {
                byte[] serviceAccountBytes = TestUtils.createServiceAccount(Randomness.get());
                gcsServiceAccountJson = new String(serviceAccountBytes, java.nio.charset.StandardCharsets.UTF_8);

                int[] count = { 0 };
                FixtureUtils.forEachFixtureEntry(getClass(), (relativePath, content) -> {
                    String key = S3FixtureUtils.WAREHOUSE + "/" + relativePath;
                    getHandler().putBlob(key, new BytesArray(content));
                    count[0]++;
                });

                logger.info("Loaded {} fixture files into GCS fixture", count[0]);
            } catch (Exception e) {
                logger.error("Failed to load GCS fixtures", e);
                throw new RuntimeException(e);
            }
        }

        /**
         * Inject GCS endpoint, credentials, and project_id into the query.
         *
         * @param query the ESQL query containing an EXTERNAL command
         * @return the query with GCS parameters injected
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

            // Escape the service account JSON for embedding inside the WITH clause.
            // The JSON is embedded as a string value, so internal double-quotes must be escaped.
            String escapedCredentials = gcsServiceAccountJson.replace("\\", "\\\\").replace("\"", "\\\"");

            String tokenUri = getAddress() + "/" + TOKEN;

            StringBuilder params = new StringBuilder();
            params.append(" WITH { ");
            params.append("\"endpoint\": \"").append(getAddress()).append("\", ");
            params.append("\"credentials\": \"").append(escapedCredentials).append("\", ");
            params.append("\"project_id\": \"test\", ");
            params.append("\"token_uri\": \"").append(tokenUri).append("\"");
            params.append(" }");

            return externalPart + params + restOfQuery;
        }
    }
}
