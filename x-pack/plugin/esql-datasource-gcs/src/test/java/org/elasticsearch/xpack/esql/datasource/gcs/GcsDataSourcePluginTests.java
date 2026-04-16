/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.util.Map;

/**
 * Unit tests for GcsDataSourcePlugin.
 * Tests that the plugin correctly registers storage provider factories for the gs:// scheme.
 */
public class GcsDataSourcePluginTests extends ESTestCase {

    public void testStorageProvidersRegistersGsScheme() {
        GcsDataSourcePlugin plugin = new GcsDataSourcePlugin();
        Map<String, StorageProviderFactory> providers = plugin.storageProviders(Settings.EMPTY);

        assertTrue("Should register gs scheme", providers.containsKey("gs"));
        assertEquals("Should register exactly 1 scheme", 1, providers.size());
    }

    public void testStorageProviderFactoryCreateWithNullConfigDelegatesToDefault() {
        GcsDataSourcePlugin plugin = new GcsDataSourcePlugin();
        Map<String, StorageProviderFactory> providers = plugin.storageProviders(Settings.EMPTY);

        StorageProviderFactory factory = providers.get("gs");
        assertNotNull("gs factory should not be null", factory);

        // create(settings, null) should delegate to create(settings) without throwing.
        // The GCS client is lazily initialized, so no ADC error at construction time.
        var provider = factory.create(Settings.EMPTY, null);
        assertNotNull(provider);
    }

    public void testStorageProviderFactoryCreateWithEmptyConfigDelegatesToDefault() {
        GcsDataSourcePlugin plugin = new GcsDataSourcePlugin();
        Map<String, StorageProviderFactory> providers = plugin.storageProviders(Settings.EMPTY);

        StorageProviderFactory factory = providers.get("gs");
        assertNotNull("gs factory should not be null", factory);

        // create(settings, emptyMap) should delegate to create(settings).
        // The GCS client is lazily initialized, so no ADC error at construction time.
        var provider = factory.create(Settings.EMPTY, Map.of());
        assertNotNull(provider);
    }

    public void testStorageProviderFactoryCreateWithConfigParsesFields() {
        GcsDataSourcePlugin plugin = new GcsDataSourcePlugin();
        Map<String, StorageProviderFactory> providers = plugin.storageProviders(Settings.EMPTY);

        StorageProviderFactory factory = providers.get("gs");
        assertNotNull("gs factory should not be null", factory);

        Map<String, Object> config = Map.of(
            "credentials",
            "{\"type\":\"service_account\",\"project_id\":\"test\",\"private_key_id\":\"key\"}",
            "project_id",
            "my-project",
            "endpoint",
            "http://localhost:4443"
        );

        // The factory should parse the config into a GcsConfiguration and attempt to build a client.
        // It will fail because the credentials JSON is not a valid service account key,
        // but we verify the config parsing path is exercised (not an ADC error).
        try {
            factory.create(Settings.EMPTY, config);
        } catch (IllegalStateException e) {
            assertTrue(
                "Should fail during client build, not config parsing",
                e.getMessage().contains("Failed to build Google Cloud Storage client")
            );
        }
    }

    public void testGsSchemeSameFactory() {
        GcsDataSourcePlugin plugin = new GcsDataSourcePlugin();
        Map<String, StorageProviderFactory> providers = plugin.storageProviders(Settings.EMPTY);

        // Only gs:// is supported (unlike S3 which has s3, s3a, s3n)
        assertNotNull(providers.get("gs"));
        assertNull(providers.get("gcs"));
    }
}
