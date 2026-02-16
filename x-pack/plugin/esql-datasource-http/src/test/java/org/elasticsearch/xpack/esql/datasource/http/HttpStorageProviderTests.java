/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.time.Duration;
import java.util.Map;

/**
 * Tests for HttpStorageProvider configuration and basic functionality.
 * Note: Tests avoid creating real HttpClient instances to prevent thread leaks.
 */
public class HttpStorageProviderTests extends ESTestCase {

    public void testConfigurationDefaults() {
        HttpConfiguration config = HttpConfiguration.defaults();

        assertEquals(Duration.ofSeconds(30), config.connectTimeout());
        assertEquals(Duration.ofMinutes(5), config.requestTimeout());
        assertTrue(config.followRedirects());
        assertTrue(config.customHeaders().isEmpty());
        assertEquals(3, config.maxRetries());
    }

    public void testConfigurationBuilder() {
        HttpConfiguration config = HttpConfiguration.builder()
            .connectTimeout(Duration.ofSeconds(15))
            .requestTimeout(Duration.ofMinutes(3))
            .followRedirects(false)
            .customHeaders(Map.of("Authorization", "Bearer token"))
            .maxRetries(2)
            .build();

        assertEquals(Duration.ofSeconds(15), config.connectTimeout());
        assertEquals(Duration.ofMinutes(3), config.requestTimeout());
        assertFalse(config.followRedirects());
        assertEquals("Bearer token", config.customHeaders().get("Authorization"));
        assertEquals(2, config.maxRetries());
    }

    public void testConfigurationBuilderValidation() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> { HttpConfiguration.builder().maxRetries(-1).build(); }
        );
        assertTrue(e.getMessage().contains("non-negative"));
    }

    public void testConfigurationBuilderNullConnectTimeout() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> { HttpConfiguration.builder().connectTimeout(null); }
        );
        assertTrue(e.getMessage().contains("connectTimeout"));
    }

    public void testConfigurationBuilderNullRequestTimeout() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> { HttpConfiguration.builder().requestTimeout(null); }
        );
        assertTrue(e.getMessage().contains("requestTimeout"));
    }

    public void testConfigurationBuilderNullCustomHeaders() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> { HttpConfiguration.builder().customHeaders(null); }
        );
        assertTrue(e.getMessage().contains("customHeaders"));
    }

    public void testStoragePathParsing() {
        StoragePath path = StoragePath.of("https://example.com:8080/data/file.csv");

        assertEquals("https", path.scheme());
        assertEquals("example.com", path.host());
        assertEquals(8080, path.port());
        assertEquals("/data/file.csv", path.path());
        assertEquals("file.csv", path.objectName());
    }

    public void testStoragePathWithoutPort() {
        StoragePath path = StoragePath.of("https://example.com/data/file.csv");

        assertEquals("https", path.scheme());
        assertEquals("example.com", path.host());
        assertEquals(-1, path.port());
        assertEquals("/data/file.csv", path.path());
    }

    public void testListObjectsThrowsUnsupportedOperation() {
        HttpStorageProvider provider = new HttpStorageProvider(HttpConfiguration.defaults(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        try {
            StoragePath prefix = StoragePath.of("https://example.com/data/");
            expectThrows(UnsupportedOperationException.class, () -> provider.listObjects(prefix, false));
            expectThrows(UnsupportedOperationException.class, () -> provider.listObjects(prefix, true));
        } finally {
            provider.close();
        }
    }
}
