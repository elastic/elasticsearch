/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.datalake.iceberg;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.datasources.datalake.S3FixtureUtils.BUCKET;
import static org.elasticsearch.xpack.esql.datasources.datalake.S3FixtureUtils.WAREHOUSE;

/**
 * Tests to verify that unsupported S3 operations are properly detected and rejected.
 * This ensures that any gaps in S3HttpHandler support will cause tests to fail immediately
 * with clear error messages, rather than silently passing with UNKNOWN request types.
 */
public class UnsupportedRequestDetectionTests extends org.elasticsearch.xpack.esql.datasources.datalake.AbstractS3HttpFixtureTest {

    /**
     * Test that an unsupported S3 operation (e.g., POST to bucket root without proper query params)
     * causes the test to fail with an AssertionError in the @After method.
     *
     * This test simulates a request that doesn't match any of the known S3 operation patterns
     * in S3HttpHandler, which should trigger our gap detection mechanism.
     */
    public void testUnsupportedOperationThrowsException() {
        // Clear any previous logs
        clearRequestLogs();

        // Try to make a POST request to an invalid path
        // This should be classified as UNKNOWN since it doesn't match any known S3 operation
        String url = s3Fixture.getAddress() + "/" + BUCKET + "/invalid-path-without-warehouse";

        try {
            var conn = createS3Connection(url, "POST");
            // This will trigger UNKNOWN request logging
            // The connection might fail, but we don't care - we just want to trigger the logging
            conn.getResponseCode();
        } catch (Exception e) {
            // Expected - the server may reject the request or connection may fail
        }

        // Manually trigger the check to verify it works (normally done in @After)
        // This should throw AssertionError due to the UNKNOWN request
        AssertionError error = expectThrows(AssertionError.class, this::checkForUnsupportedOperations);

        // Verify the exception message contains useful information
        String message = error.getMessage();
        assertNotNull("Exception message should not be null", message);
        assertTrue("Exception message should mention unsupported operation", message.contains("unsupported S3 operation"));
        assertTrue("Exception message should mention gap in S3HttpHandler", message.contains("gap in S3HttpHandler support"));
        assertTrue("Exception message should include the request method", message.contains("POST"));
        assertTrue("Exception message should include the request path", message.contains("/iceberg-test/invalid-path-without-warehouse"));

        // Clear the logs so the @After method doesn't fail the test again
        clearRequestLogs();
    }

    /**
     * Test that supported S3 operations do NOT throw exceptions.
     * This verifies that our gap detection doesn't create false positives.
     */
    public void testSupportedOperationsDoNotThrowException() throws IOException, InterruptedException {
        // Clear any previous logs
        clearRequestLogs();

        // Pre-populate a test file
        String key = "test/supported-op.txt";
        addBlobToFixture(key, "test content");

        // Make a supported GET request - this should NOT throw an exception
        String url = s3Fixture.getAddress() + "/" + BUCKET + "/" + WAREHOUSE + "/" + key;
        var conn = createS3Connection(url, "GET");
        int responseCode = conn.getResponseCode();
        // This should succeed without throwing UnsupportedOperationException
        assertEquals("Supported operation should return 200 OK", 200, responseCode);

        // Verify no UNKNOWN requests were logged
        assertFalse("Should not have unknown requests for supported operations", hasUnknownRequests());
    }
}
