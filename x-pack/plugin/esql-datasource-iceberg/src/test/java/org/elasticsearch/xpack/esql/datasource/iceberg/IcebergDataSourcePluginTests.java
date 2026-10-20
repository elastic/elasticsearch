/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.iceberg;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.DataSourceCapabilities;

import java.util.List;

/**
 * Unit tests for {@link IcebergDataSourcePlugin}'s feature-flag gating.
 * <p>
 * Iceberg catalog registration is gated on the {@code esql_external_iceberg} sub-flag
 * (snapshot-on, release-off). Across the snapshot and {@code elasticsearch.esql-release} build
 * variants both the enabled and disabled branches get exercised.
 */
public class IcebergDataSourcePluginTests extends ESTestCase {

    private static boolean icebergEnabled() {
        return IcebergDataSourcePlugin.ESQL_EXTERNAL_ICEBERG_FEATURE_FLAG.isEnabled();
    }

    public void testRegistersIcebergCatalogWhenEnabled() {
        assumeTrue("requires Iceberg feature flag", icebergEnabled());
        IcebergDataSourcePlugin plugin = new IcebergDataSourcePlugin();

        assertTrue("should register iceberg catalog", plugin.supportedCatalogs().contains("iceberg"));
        assertEquals("should register exactly 1 catalog", 1, plugin.supportedCatalogs().size());
        assertTrue("should register iceberg table catalog factory", plugin.tableCatalogs(Settings.EMPTY).containsKey("iceberg"));
        assertEquals("should register exactly 1 table catalog factory", 1, plugin.tableCatalogs(Settings.EMPTY).size());
    }

    public void testDisabledWhenFeatureFlagOff() {
        assumeFalse("only when Iceberg feature flag is off", icebergEnabled());
        IcebergDataSourcePlugin plugin = new IcebergDataSourcePlugin();

        assertTrue("no supported catalogs when disabled", plugin.supportedCatalogs().isEmpty());
        assertTrue("no table catalogs when disabled", plugin.tableCatalogs(Settings.EMPTY).isEmpty());
    }

    /**
     * With the flag off the node's data source capabilities must not advertise an {@code iceberg}
     * catalog — the catalog would otherwise claim extensionless S3 object paths in the resolver.
     */
    public void testNoCatalogInCapabilitiesWhenFlagOff() {
        assumeFalse("only when Iceberg feature flag is off", icebergEnabled());
        DataSourceCapabilities caps = DataSourceCapabilities.build(List.of(new IcebergDataSourcePlugin()));
        assertFalse("iceberg must not appear in capabilities when flag is off", caps.supportsCatalog("iceberg"));
    }
}
