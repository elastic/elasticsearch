/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderServices;

import java.util.Map;

/**
 * Unit tests for AzureDataSourcePlugin.
 * Tests that the plugin correctly registers storage provider factories for wasbs:// and wasb:// schemes.
 * <p>
 * Azure registration is gated on the {@code esql_external_azure} sub-flag (snapshot-on, release-off).
 * The provider-shape tests below assume the gate is on; a dedicated test asserts nothing is
 * registered when it is off. Across the snapshot and {@code elasticsearch.esql-release} build
 * variants both branches get exercised.
 */
public class AzureDataSourcePluginTests extends ESTestCase {

    private static boolean azureEnabled() {
        return AzureDataSourcePlugin.ESQL_EXTERNAL_AZURE_FEATURE_FLAG.isEnabled();
    }

    public void testStorageProvidersRegistersWasbsAndWasbSchemes() {
        assumeTrue("requires Azure feature flag", azureEnabled());
        AzureDataSourcePlugin plugin = new AzureDataSourcePlugin();
        Map<String, StorageProviderFactory> providers = plugin.storageProviders(
            new StorageProviderServices(Settings.EMPTY, EsExecutors.DIRECT_EXECUTOR_SERVICE, null, null)
        );

        assertTrue("Should register wasbs scheme", providers.containsKey("wasbs"));
        assertTrue("Should register wasb scheme", providers.containsKey("wasb"));
        assertEquals("Should register exactly 2 schemes", 2, providers.size());
    }

    public void testSupportedSchemes() {
        assumeTrue("requires Azure feature flag", azureEnabled());
        AzureDataSourcePlugin plugin = new AzureDataSourcePlugin();
        assertEquals(2, plugin.supportedSchemes().size());
        assertTrue(plugin.supportedSchemes().contains("wasbs"));
        assertTrue(plugin.supportedSchemes().contains("wasb"));
    }

    public void testDatasourceValidatorRegisteredWhenEnabled() {
        assumeTrue("requires Azure feature flag", azureEnabled());
        AzureDataSourcePlugin plugin = new AzureDataSourcePlugin();
        Map<String, DataSourceValidator> validators = plugin.datasourceValidators(Settings.EMPTY);

        assertTrue("should register the azure validator", validators.containsKey("azure"));
        assertEquals("should register exactly 1 validator", 1, validators.size());
    }

    public void testDisabledWhenFeatureFlagOff() {
        assumeFalse("only when Azure feature flag is off", azureEnabled());
        AzureDataSourcePlugin plugin = new AzureDataSourcePlugin();
        assertTrue("no schemes when disabled", plugin.supportedSchemes().isEmpty());
        assertTrue(
            "no storage providers when disabled",
            plugin.storageProviders(new StorageProviderServices(Settings.EMPTY, EsExecutors.DIRECT_EXECUTOR_SERVICE, null, null)).isEmpty()
        );
        assertTrue("no datasource validators when disabled", plugin.datasourceValidators(Settings.EMPTY).isEmpty());
    }

    public void testStorageProviderFactoryCreateWithNullConfigDelegatesToDefault() {
        assumeTrue("requires Azure feature flag", azureEnabled());
        AzureDataSourcePlugin plugin = new AzureDataSourcePlugin();
        Map<String, StorageProviderFactory> providers = plugin.storageProviders(
            new StorageProviderServices(Settings.EMPTY, EsExecutors.DIRECT_EXECUTOR_SERVICE, null, null)
        );

        StorageProviderFactory factory = providers.get("wasbs");
        assertNotNull("wasbs factory should not be null", factory);

        var provider = factory.create(Settings.EMPTY, null);
        assertNotNull(provider);
    }

    public void testStorageProviderFactoryCreateWithEmptyConfigDelegatesToDefault() {
        assumeTrue("requires Azure feature flag", azureEnabled());
        AzureDataSourcePlugin plugin = new AzureDataSourcePlugin();
        Map<String, StorageProviderFactory> providers = plugin.storageProviders(
            new StorageProviderServices(Settings.EMPTY, EsExecutors.DIRECT_EXECUTOR_SERVICE, null, null)
        );

        StorageProviderFactory factory = providers.get("wasbs");
        assertNotNull("wasbs factory should not be null", factory);

        var provider = factory.create(Settings.EMPTY, Map.of());
        assertNotNull(provider);
    }

    public void testWasbsAndWasbShareSameFactory() {
        assumeTrue("requires Azure feature flag", azureEnabled());
        AzureDataSourcePlugin plugin = new AzureDataSourcePlugin();
        Map<String, StorageProviderFactory> providers = plugin.storageProviders(
            new StorageProviderServices(Settings.EMPTY, EsExecutors.DIRECT_EXECUTOR_SERVICE, null, null)
        );

        StorageProviderFactory wasbsFactory = providers.get("wasbs");
        StorageProviderFactory wasbFactory = providers.get("wasb");
        assertNotNull(wasbsFactory);
        assertNotNull(wasbFactory);
        assertEquals(wasbsFactory, wasbFactory);
    }
}
