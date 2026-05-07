/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.util.Map;

/**
 * Unit tests for AzureDataSourcePlugin.
 * Tests that the plugin correctly registers storage provider factories for wasbs:// and wasb:// schemes.
 */
public class AzureDataSourcePluginTests extends ESTestCase {

    public void testStorageProvidersRegistersWasbsAndWasbSchemes() {
        AzureDataSourcePlugin plugin = new AzureDataSourcePlugin();
        Map<String, StorageProviderFactory> providers = plugin.storageProviders(Settings.EMPTY);

        assertTrue("Should register wasbs scheme", providers.containsKey("wasbs"));
        assertTrue("Should register wasb scheme", providers.containsKey("wasb"));
        assertEquals("Should register exactly 2 schemes", 2, providers.size());
    }

    public void testSupportedSchemes() {
        AzureDataSourcePlugin plugin = new AzureDataSourcePlugin();
        assertEquals(2, plugin.supportedSchemes().size());
        assertTrue(plugin.supportedSchemes().contains("wasbs"));
        assertTrue(plugin.supportedSchemes().contains("wasb"));
    }

    public void testStorageProviderFactoryCreateWithNullConfigDelegatesToDefault() {
        AzureDataSourcePlugin plugin = new AzureDataSourcePlugin();
        Map<String, StorageProviderFactory> providers = plugin.storageProviders(Settings.EMPTY);

        StorageProviderFactory factory = providers.get("wasbs");
        assertNotNull("wasbs factory should not be null", factory);

        var provider = factory.create(Settings.EMPTY, null);
        assertNotNull(provider);
    }

    public void testStorageProviderFactoryCreateWithEmptyConfigDelegatesToDefault() {
        AzureDataSourcePlugin plugin = new AzureDataSourcePlugin();
        Map<String, StorageProviderFactory> providers = plugin.storageProviders(Settings.EMPTY);

        StorageProviderFactory factory = providers.get("wasbs");
        assertNotNull("wasbs factory should not be null", factory);

        var provider = factory.create(Settings.EMPTY, Map.of());
        assertNotNull(provider);
    }

    public void testWasbsAndWasbShareSameFactory() {
        AzureDataSourcePlugin plugin = new AzureDataSourcePlugin();
        Map<String, StorageProviderFactory> providers = plugin.storageProviders(Settings.EMPTY);

        StorageProviderFactory wasbsFactory = providers.get("wasbs");
        StorageProviderFactory wasbFactory = providers.get("wasb");
        assertNotNull(wasbsFactory);
        assertNotNull(wasbFactory);
        assertEquals(wasbsFactory, wasbFactory);
    }
}
