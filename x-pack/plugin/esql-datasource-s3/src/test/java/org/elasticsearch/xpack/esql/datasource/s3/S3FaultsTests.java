/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.TransientStorageException;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import static org.hamcrest.Matchers.instanceOf;

/**
 * Verifies that {@link S3Faults} maps real S3/SDK exceptions to the SPI's transient/throttle/non-transient
 * vocabulary — exercising the actual classification path rather than injecting a pre-typed marker.
 */
public class S3FaultsTests extends ESTestCase {

    private static final StoragePath PATH = StoragePath.of("s3://bucket/key");

    private static S3Exception s3(int statusCode) {
        return (S3Exception) S3Exception.builder().statusCode(statusCode).message("status " + statusCode).build();
    }

    public void test503IsThrottling() {
        IOException result = S3Faults.classify(s3(503), PATH, "op");
        assertThat(result, instanceOf(TransientStorageException.class));
        assertTrue("503 must be flagged throttling", ((TransientStorageException) result).throttling());
    }

    public void test429IsThrottling() {
        IOException result = S3Faults.classify(s3(429), PATH, "op");
        assertThat(result, instanceOf(TransientStorageException.class));
        assertTrue("429 must be flagged throttling", ((TransientStorageException) result).throttling());
    }

    public void test500IsTransientButNotThrottling() {
        IOException result = S3Faults.classify(s3(500), PATH, "op");
        assertThat(result, instanceOf(TransientStorageException.class));
        assertFalse("500 is transient but not throttling", ((TransientStorageException) result).throttling());
    }

    public void test403IsNotTransient() {
        IOException result = S3Faults.classify(s3(403), PATH, "op");
        assertFalse("403 must not be transient", result instanceof TransientStorageException);
    }

    public void testNoSuchKeyIsNotTransient() {
        IOException result = S3Faults.classify(NoSuchKeyException.builder().message("missing").build(), PATH, "op");
        assertFalse("a missing object must not be transient", result instanceof TransientStorageException);
        assertTrue(result.getMessage().contains("Object not found"));
    }

    public void testSocketExceptionIsTransient() {
        IOException result = S3Faults.classify(new SocketException("Connection reset"), PATH, "op");
        assertThat(result, instanceOf(TransientStorageException.class));
        assertFalse("a transport reset is transient but not throttling", ((TransientStorageException) result).throttling());
    }

    public void testSocketTimeoutIsTransient() {
        IOException result = S3Faults.classify(new SocketTimeoutException("Read timed out"), PATH, "op");
        assertThat(result, instanceOf(TransientStorageException.class));
    }

    public void testGenericIOExceptionPassesThrough() {
        IOException original = new IOException("some non-transient io");
        IOException result = S3Faults.classify(original, PATH, "op");
        assertSame("an existing IOException passes through unchanged", original, result);
        assertFalse(result instanceof TransientStorageException);
    }
}
