/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.lance.storage;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for {@link RealLanceDataset}.
 * <p>
 * These tests verify the behavior of the real Lance SDK integration.
 * Note: Tests that require actual Lance datasets or native libraries
 * are skipped if the lance-java native library cannot be loaded.
 */
public class RealLanceDatasetTests extends ESTestCase {

    private static boolean isLanceNativeAvailable() {
        try {
            // Try to load the lance-java native library by creating an allocator
            // This will fail if Arrow memory or Lance JNI isn't available
            org.apache.arrow.memory.RootAllocator allocator = new org.apache.arrow.memory.RootAllocator(1024);
            allocator.close();
            Class.forName("com.lancedb.lance.Dataset");
            return true;
        } catch (ClassNotFoundException | UnsatisfiedLinkError | ExceptionInInitializerError | NoClassDefFoundError e) {
            return false;
        }
    }

    // ========== Tests that don't require native libraries ==========

    public void testLanceDatasetConfigDefaults() {
        LanceDatasetConfig config = LanceDatasetConfig.defaults();

        assertThat(config.idColumn(), equalTo("_id"));
        assertThat(config.vectorColumn(), equalTo("vector"));
        assertThat(config.expectedDims(), equalTo(0));
    }

    public void testLanceDatasetConfigWithDims() {
        LanceDatasetConfig config = LanceDatasetConfig.withDims(768);

        assertThat(config.idColumn(), equalTo("_id"));
        assertThat(config.vectorColumn(), equalTo("vector"));
        assertThat(config.expectedDims(), equalTo(768));
    }

    public void testLanceDatasetConfigCustom() {
        LanceDatasetConfig config = new LanceDatasetConfig("doc_id", "embeddings", 1536, null, null, null);

        assertThat(config.idColumn(), equalTo("doc_id"));
        assertThat(config.vectorColumn(), equalTo("embeddings"));
        assertThat(config.expectedDims(), equalTo(1536));
    }

    public void testLanceDatasetRegistryIsLanceFormat() {
        // Test the .lance format detection
        assertTrue(LanceDatasetRegistry.isLanceFormat("/path/to/data.lance"));
        assertTrue(LanceDatasetRegistry.isLanceFormat("file:///path/to/data.lance"));
        assertTrue(LanceDatasetRegistry.isLanceFormat("oss://bucket/path/to/data.lance"));
        assertTrue(LanceDatasetRegistry.isLanceFormat("/path/to/data.lance/"));
        assertTrue(LanceDatasetRegistry.isLanceFormat("/path/to/data.lance/version"));

        // Test non-lance formats
        assertFalse(LanceDatasetRegistry.isLanceFormat("/path/to/data.json"));
        assertFalse(LanceDatasetRegistry.isLanceFormat("embedded:data"));
        assertFalse(LanceDatasetRegistry.isLanceFormat("/path/to/data.parquet"));
    }

    public void testLanceDatasetRegistryClearWorks() {
        // Just verify that clear doesn't throw
        LanceDatasetRegistry.clear();
        assertThat(LanceDatasetRegistry.size(), equalTo(0));
    }

    public void testLanceDatasetRegistryInvalidateNonexistent() {
        // Invalidating a non-existent key should not throw
        LanceDatasetRegistry.invalidate("nonexistent://uri");
        // Should complete without error
    }

    // ========== Tests that require native libraries ==========

    public void testOpenNonExistentDatasetThrowsIOException() throws Exception {
        assumeTrue("Lance native library not available", isLanceNativeAvailable());

        // Opening a non-existent dataset should throw IOException
        LanceDatasetConfig config = LanceDatasetConfig.defaults();

        IOException ex = expectThrows(IOException.class, () -> RealLanceDataset.open("file:///nonexistent/path/to/dataset.lance", config));

        // Verify the exception message mentions the path
        assertThat(ex.getMessage(), containsString("nonexistent"));
    }

    public void testOpenWithInvalidUriThrowsIOException() throws Exception {
        assumeTrue("Lance native library not available", isLanceNativeAvailable());

        LanceDatasetConfig config = LanceDatasetConfig.defaults();

        // Various invalid URIs should all result in IOException
        IOException ex = expectThrows(IOException.class, () -> RealLanceDataset.open("/invalid/path/without/lance/extension", config));

        assertNotNull(ex);
    }

    /**
     * Integration test that requires an actual Lance dataset.
     * Skipped if lance-java native library is not available.
     */
    public void testOpenRealDatasetIfAvailable() throws Exception {
        assumeTrue("Lance native library not available", isLanceNativeAvailable());

        // This test would require an actual Lance dataset file
        // For CI/CD, we'd need to create a test dataset or use a fixture
        // For now, we just verify the method signature works
        Path tempDir = createTempDir();
        Path fakeLancePath = tempDir.resolve("test.lance");

        // Create a directory to simulate a Lance dataset structure
        Files.createDirectories(fakeLancePath);

        LanceDatasetConfig config = LanceDatasetConfig.defaults();

        // This will fail because it's not a real Lance dataset,
        // but it exercises the code path
        IOException ex = expectThrows(IOException.class, () -> RealLanceDataset.open(fakeLancePath.toString(), config));

        // The error should come from the Lance SDK trying to read invalid data
        assertNotNull(ex);
    }
}
