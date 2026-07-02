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
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.util.Map;

import static org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_HTTP_FEATURE_FLAG;
import static org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG;

public class HttpDataSourcePluginTests extends ESTestCase {

    private final HttpDataSourcePlugin plugin = new HttpDataSourcePlugin();

    public void testHttpValidatorRegisteredWhenFlagEnabled() {
        assumeTrue("requires http datasource feature flag", ESQL_EXTERNAL_DATASOURCES_HTTP_FEATURE_FLAG.isEnabled());
        DataSourceValidator http = plugin.datasourceValidators(Settings.EMPTY).get("http");
        assertNotNull("http validator should be registered when the flag is enabled", http);
        assertEquals("http", http.type());
    }

    public void testLocalValidatorRegisteredWhenFlagEnabled() {
        assumeTrue("requires local datasource feature flag", ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled());
        DataSourceValidator local = plugin.datasourceValidators(Settings.EMPTY).get("local");
        assertNotNull("local validator should be registered when the flag is enabled", local);
        assertEquals("local", local.type());
    }

    public void testHttpValidatorAcceptsHttpAndHttpsSchemes() {
        assumeTrue("requires http datasource feature flag", ESQL_EXTERNAL_DATASOURCES_HTTP_FEATURE_FLAG.isEnabled());
        DataSourceValidator http = plugin.datasourceValidators(Settings.EMPTY).get("http");
        // No dataset settings supplied, so the validated settings come back empty for both schemes.
        assertTrue(http.validateDataset(Map.of(), "http://example.org/data.csv", Map.of()).isEmpty());
        assertTrue(http.validateDataset(Map.of(), "https://example.org/data.csv", Map.of()).isEmpty());
    }

    public void testHttpValidatorRejectsNonHttpScheme() {
        assumeTrue("requires http datasource feature flag", ESQL_EXTERNAL_DATASOURCES_HTTP_FEATURE_FLAG.isEnabled());
        DataSourceValidator http = plugin.datasourceValidators(Settings.EMPTY).get("http");
        expectThrows(ValidationException.class, () -> http.validateDataset(Map.of(), "file:///tmp/data.csv", Map.of()));
        expectThrows(ValidationException.class, () -> http.validateDataset(Map.of(), "s3://bucket/data.csv", Map.of()));
    }

    public void testLocalValidatorAcceptsFileScheme() {
        assumeTrue("requires local datasource feature flag", ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled());
        DataSourceValidator local = plugin.datasourceValidators(Settings.EMPTY).get("local");
        assertNotNull(local.validateDataset(Map.of(), "file:///tmp/data.csv", Map.of()));
    }

    public void testLocalValidatorRejectsNonFileScheme() {
        assumeTrue("requires local datasource feature flag", ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled());
        DataSourceValidator local = plugin.datasourceValidators(Settings.EMPTY).get("local");
        expectThrows(ValidationException.class, () -> local.validateDataset(Map.of(), "http://example.org/data.csv", Map.of()));
    }

    public void testEmptyDatasourceSettingsAccepted() {
        assumeTrue("requires http datasource feature flag", ESQL_EXTERNAL_DATASOURCES_HTTP_FEATURE_FLAG.isEnabled());
        DataSourceValidator http = plugin.datasourceValidators(Settings.EMPTY).get("http");
        assertTrue(http.validateDatasource(Map.of()).isEmpty());
    }

    public void testAuthAnonymousDatasourceSettingAccepted() {
        assumeTrue("requires http datasource feature flag", ESQL_EXTERNAL_DATASOURCES_HTTP_FEATURE_FLAG.isEnabled());
        DataSourceValidator http = plugin.datasourceValidators(Settings.EMPTY).get("http");
        assertTrue(http.validateDatasource(Map.of("auth", "anonymous")).containsKey("auth"));
    }

    public void testDatasourceSettingsRejected() {
        assumeTrue("requires http datasource feature flag", ESQL_EXTERNAL_DATASOURCES_HTTP_FEATURE_FLAG.isEnabled());
        DataSourceValidator http = plugin.datasourceValidators(Settings.EMPTY).get("http");
        expectThrows(ValidationException.class, () -> http.validateDatasource(Map.of("region", "us-east-1")));
    }
}
