/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

/**
 * Unit tests for AzureStorageObject.
 * Tests constructor validation.
 * Note: Tests that require mocking BlobClient are excluded as it is a final class
 * and cannot be mocked with standard Mockito.
 */
public class AzureStorageObjectTests extends ESTestCase {

    public void testConstructorNullBlobClientThrows() {
        StoragePath path = StoragePath.of("wasbs://account.blob.core.windows.net/container/key");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new AzureStorageObject(null, "container", "key", path)
        );
        assertEquals("blobClient cannot be null", e.getMessage());
    }
}
