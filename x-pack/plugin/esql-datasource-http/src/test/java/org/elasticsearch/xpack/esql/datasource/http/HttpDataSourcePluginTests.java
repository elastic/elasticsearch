/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.DecompressionCodecRegistry;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_HTTP_FEATURE_FLAG;
import static org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpDataSourcePluginTests extends ESTestCase {

    private final HttpDataSourcePlugin plugin = new HttpDataSourcePlugin();

    private static boolean httpEnabled() {
        return ESQL_EXTERNAL_DATASOURCES_HTTP_FEATURE_FLAG.isEnabled();
    }

    private static boolean localEnabled() {
        return ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled();
    }

    public void testHttpValidatorRegisteredWhenFlagEnabled() {
        assumeTrue("requires http datasource feature flag", httpEnabled());
        DataSourceValidator http = plugin.datasourceValidators(Settings.EMPTY).get("http");
        assertNotNull("http validator should be registered when the flag is enabled", http);
        assertEquals("http", http.type());
    }

    public void testLocalValidatorRegisteredWhenFlagEnabled() {
        assumeTrue("requires external datasources feature flag", localEnabled());
        DataSourceValidator local = plugin.datasourceValidators(Settings.EMPTY).get("local");
        assertNotNull("local validator should be registered when the flag is enabled", local);
        assertEquals("local", local.type());
    }

    public void testHttpValidatorAcceptsHttpAndHttpsSchemes() {
        assumeTrue("requires http datasource feature flag", httpEnabled());
        DataSourceValidator http = plugin.datasourceValidators(Settings.EMPTY).get("http");
        // No dataset settings supplied, so the validated settings come back empty for both schemes.
        assertTrue(http.validateDataset(Map.of(), "http://example.org/data.csv", Map.of()).isEmpty());
        assertTrue(http.validateDataset(Map.of(), "https://example.org/data.csv", Map.of()).isEmpty());
    }

    public void testHttpValidatorRejectsNonHttpScheme() {
        assumeTrue("requires http datasource feature flag", httpEnabled());
        DataSourceValidator http = plugin.datasourceValidators(Settings.EMPTY).get("http");
        expectThrows(ValidationException.class, () -> http.validateDataset(Map.of(), "file:///tmp/data.csv", Map.of()));
        expectThrows(ValidationException.class, () -> http.validateDataset(Map.of(), "s3://bucket/data.csv", Map.of()));
    }

    public void testLocalValidatorAcceptsFileScheme() {
        assumeTrue("requires external datasources feature flag", localEnabled());
        DataSourceValidator local = plugin.datasourceValidators(Settings.EMPTY).get("local");
        assertNotNull(local.validateDataset(Map.of(), "file:///tmp/data.csv", Map.of()));
    }

    public void testLocalValidatorRejectsNonFileScheme() {
        assumeTrue("requires external datasources feature flag", localEnabled());
        DataSourceValidator local = plugin.datasourceValidators(Settings.EMPTY).get("local");
        expectThrows(ValidationException.class, () -> local.validateDataset(Map.of(), "http://example.org/data.csv", Map.of()));
    }

    public void testEmptyDatasourceSettingsAccepted() {
        assumeTrue("requires http datasource feature flag", httpEnabled());
        DataSourceValidator http = plugin.datasourceValidators(Settings.EMPTY).get("http");
        assertTrue(http.validateDatasource(Map.of()).isEmpty());
    }

    public void testAuthAnonymousDatasourceSettingAccepted() {
        assumeTrue("requires http datasource feature flag", httpEnabled());
        DataSourceValidator http = plugin.datasourceValidators(Settings.EMPTY).get("http");
        assertTrue(http.validateDatasource(Map.of("auth", "anonymous")).containsKey("auth"));
    }

    public void testDatasourceSettingsRejected() {
        assumeTrue("requires http datasource feature flag", httpEnabled());
        DataSourceValidator http = plugin.datasourceValidators(Settings.EMPTY).get("http");
        expectThrows(ValidationException.class, () -> http.validateDatasource(Map.of("region", "us-east-1")));
    }

    public void testLocalEnabledWhenOnlyHttpFlagOff() {
        // Local does not require ESQL_EXTERNAL_DATASOURCES_HTTP_FEATURE_FLAG — the two flags are independent.
        // This test verifies local is available even when the http sub-flag is off (local flag on).
        assumeTrue("requires local flag", ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled());
        assumeFalse("only when http flag is off", ESQL_EXTERNAL_DATASOURCES_HTTP_FEATURE_FLAG.isEnabled());
        assertNotNull("local validator should be present", plugin.datasourceValidators(Settings.EMPTY).get("local"));
        assertTrue("file scheme should be registered", plugin.supportedSchemes().contains("file"));
    }

    // Format-aware HTTP(S): pins that '#'-stripping is preserved for http/https schemes.
    // '#' is not a glob metacharacter (StoragePath.GLOB_METACHARACTERS), so URLs with fragments
    // are queryable — unlike '?', which routes any HTTP URL through the glob/listing path.
    private static final FileDataSourceValidator.FormatConfigKeyResolver CSV_RESOLVER = FileDataSourceValidator.FormatConfigKeyResolver.of(
        Map.of("csv", Set.of("delimiter")),
        Map.of(".csv", "csv")
    );

    public void testFormatAwareHttpFragmentWithDottedSuffixDoesNotConfuseExtension() {
        // '#' is stripped from the object name before the extension lookup (HTTP branch). A dot inside
        // the fragment ('#frag.xyz') must not win the last-dot scan — the correct result is '.csv'.
        assumeTrue("requires http datasource feature flag", httpEnabled());
        FileDataSourceValidator httpBase = (FileDataSourceValidator) plugin.datasourceValidators(Settings.EMPTY).get("http");
        var formatAwareHttp = httpBase.withFormatConfigKeyResolver(CSV_RESOLVER).withFormatReaderRegistry(csvRegistry());
        var result = formatAwareHttp.validateDataset(Map.of(), "https://host/data.csv#frag.xyz", Map.of("delimiter", ";"));
        assertEquals(";", result.get("delimiter"));
    }

    /**
     * Mockito stub: {@link FormatReader#formatName()}, {@link FormatReader#fileExtensions()}, and
     * {@link FormatReader#supportsWholeFileCompression()} are the only methods consulted.
     */
    private static FormatReaderRegistry csvRegistry() {
        FormatReader csv = mock(FormatReader.class);
        when(csv.formatName()).thenReturn("csv");
        when(csv.fileExtensions()).thenReturn(List.of(".csv"));
        when(csv.supportsWholeFileCompression()).thenReturn(true);
        FormatReaderRegistry registry = new FormatReaderRegistry(new DecompressionCodecRegistry());
        registry.registerLazy("csv", (s, bf) -> csv, Settings.EMPTY, null);
        registry.registerExtension(".csv", "csv");
        return registry;
    }
}
