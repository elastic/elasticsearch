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
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit tests for {@link FakeLanceDatasetRegistry}.
 */
public class FakeLanceDatasetRegistryTests extends ESTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Clear registry before each test to ensure isolation
        FakeLanceDatasetRegistry.clear();
    }

    @Override
    public void tearDown() throws Exception {
        // Clear registry after each test
        FakeLanceDatasetRegistry.clear();
        super.tearDown();
    }

    public void testGetLoadsDataset() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] }
            ]
            """);

        String uri = "file://" + tempFile.toString();
        FakeLanceDataset dataset = FakeLanceDatasetRegistry.get(uri, 3, () -> {
            try {
                return FakeLanceDataset.load(uri, 3);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertNotNull(dataset);
        assertThat(dataset.dims(), equalTo(3));
    }

    public void testGetCachesDataset() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] }
            ]
            """);

        String uri = "file://" + tempFile.toString();
        AtomicInteger loadCount = new AtomicInteger(0);

        FakeLanceDataset dataset1 = FakeLanceDatasetRegistry.get(uri, 3, () -> {
            loadCount.incrementAndGet();
            try {
                return FakeLanceDataset.load(uri, 3);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        FakeLanceDataset dataset2 = FakeLanceDatasetRegistry.get(uri, 3, () -> {
            loadCount.incrementAndGet();
            try {
                return FakeLanceDataset.load(uri, 3);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // Loader should only be called once due to caching
        assertThat(loadCount.get(), equalTo(1));
        // Should return the same instance
        assertThat(dataset1, sameInstance(dataset2));
    }

    public void testGetDifferentUrisAreCachedSeparately() throws Exception {
        Path tempFile1 = createTempFile();
        Files.writeString(tempFile1, """
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] }
            ]
            """);

        Path tempFile2 = createTempFile();
        Files.writeString(tempFile2, """
            [
              { "id": "doc2", "vector": [0.0, 1.0, 0.0] }
            ]
            """);

        String uri1 = "file://" + tempFile1.toString();
        String uri2 = "file://" + tempFile2.toString();

        AtomicInteger loadCount = new AtomicInteger(0);

        FakeLanceDataset dataset1 = FakeLanceDatasetRegistry.get(uri1, 3, () -> {
            loadCount.incrementAndGet();
            try {
                return FakeLanceDataset.load(uri1, 3);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        FakeLanceDataset dataset2 = FakeLanceDatasetRegistry.get(uri2, 3, () -> {
            loadCount.incrementAndGet();
            try {
                return FakeLanceDataset.load(uri2, 3);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // Each URI should trigger a load
        assertThat(loadCount.get(), equalTo(2));
        // Datasets should be different instances
        assertNotSame(dataset1, dataset2);
    }

    public void testGetPropagatesIOException() throws Exception {
        // Use a unique URI to avoid cache interference
        String uri = "uri://io-error-test-" + randomAlphaOfLength(8);
        IOException expectedException = expectThrows(IOException.class, () -> {
            FakeLanceDatasetRegistry.get(uri, 3, () -> { throw new RuntimeException(new IOException("Test IO error")); });
        });

        assertThat(expectedException.getMessage(), equalTo("Test IO error"));
    }

    public void testGetPropagatesRuntimeException() throws Exception {
        // Use a unique URI to avoid cache interference
        String uri = "uri://runtime-error-test-" + randomAlphaOfLength(8);
        RuntimeException ex = expectThrows(RuntimeException.class, () -> {
            FakeLanceDatasetRegistry.get(uri, 3, () -> { throw new RuntimeException("Non-IO error"); });
        });

        // The exception gets wrapped by computeIfAbsent, so check the cause chain
        Throwable cause = ex.getCause();
        assertNotNull(cause);
        assertThat(cause.getMessage(), equalTo("Non-IO error"));
    }

    public void testClearRemovesAllEntries() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] }
            ]
            """);

        String uri = "file://" + tempFile.toString();
        AtomicInteger loadCount = new AtomicInteger(0);

        // First load
        FakeLanceDatasetRegistry.get(uri, 3, () -> {
            loadCount.incrementAndGet();
            try {
                return FakeLanceDataset.load(uri, 3);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertThat(loadCount.get(), equalTo(1));

        // Clear the cache
        FakeLanceDatasetRegistry.clear();

        // Should load again after clear
        FakeLanceDatasetRegistry.get(uri, 3, () -> {
            loadCount.incrementAndGet();
            try {
                return FakeLanceDataset.load(uri, 3);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertThat(loadCount.get(), equalTo(2));
    }

    public void testGetWithNullLoaderResult() throws Exception {
        // Use a unique URI to avoid cache interference
        String uri = "uri://null-test-" + randomAlphaOfLength(8);

        // ConcurrentHashMap.computeIfAbsent may return null if the function returns null
        // In that case, subsequent calls will invoke the loader again
        FakeLanceDataset result = FakeLanceDatasetRegistry.get(uri, 3, () -> null);
        assertNull(result);
    }
}
