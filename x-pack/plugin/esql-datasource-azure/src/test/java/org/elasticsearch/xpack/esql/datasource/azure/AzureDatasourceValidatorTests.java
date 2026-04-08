/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ConfigSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DatasourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FileDatasourceValidator;

import java.util.Map;
import java.util.Set;

public class AzureDatasourceValidatorTests extends ESTestCase {

    private final DatasourceValidator validator = new FileDatasourceValidator(
        "azure_blob",
        AzureConfiguration::fromMap,
        Set.of("wasbs://", "wasb://")
    );

    public void testType() {
        assertEquals("azure_blob", validator.type());
    }

    public void testValidateDatasourceWithSharedKey() {
        var result = validator.validateDatasource(Map.of("account", "myaccount", "key", "mykey"));
        assertFalse(findKey(result, "account").isSecret());
        assertTrue(findKey(result, "key").isSecret());
    }

    public void testValidateDatasourceEmpty() {
        assertTrue(validator.validateDatasource(Map.of()).isEmpty());
    }

    public void testValidateDatasourceRejectsUnknown() {
        expectThrows(IllegalArgumentException.class, () -> validator.validateDatasource(Map.of("container", "x")));
    }

    public void testValidateDatasourceRejectsInvalidAuth() {
        expectThrows(IllegalArgumentException.class, () -> validator.validateDatasource(Map.of("auth", "managed_identity")));
    }

    public void testValidateDatasourceAnonymousConflictConnectionString() {
        expectThrows(
            IllegalArgumentException.class,
            () -> validator.validateDatasource(Map.of("auth", "none", "connection_string", "DefaultEndpointsProtocol=https"))
        );
    }

    public void testValidateDatasourceAnonymousConflictSasToken() {
        expectThrows(
            IllegalArgumentException.class,
            () -> validator.validateDatasource(Map.of("auth", "none", "sas_token", "?sv=2020-01-01"))
        );
    }

    public void testValidateDatasourceWithSasToken() {
        assertTrue(findKey(validator.validateDatasource(Map.of("sas_token", "?sv=2020")), "sas_token").isSecret());
    }

    public void testValidateDatasourceWithConnectionString() {
        assertTrue(
            findKey(
                validator.validateDatasource(Map.of("connection_string", "DefaultEndpointsProtocol=https;AccountName=x")),
                "connection_string"
            ).isSecret()
        );
    }

    public void testValidateDatasetValid() {
        var result = validator.validateDataset(Map.of(), "wasbs://c@a.blob.core.windows.net/p/*.parquet", Map.of("error_mode", "skip_row"));
        assertEquals("skip_row", findValue(result, "error_mode"));
    }

    public void testValidateDatasetBothSchemes() {
        for (String uri : new String[] { "wasbs://c@a.blob.core.windows.net/p", "wasb://c@a.blob.core.windows.net/p" }) {
            assertNotNull(validator.validateDataset(Map.of(), uri, Map.of()));
        }
    }

    public void testValidateDatasetRequiresResource() {
        expectThrows(IllegalArgumentException.class, () -> validator.validateDataset(Map.of(), null, Map.of()));
    }

    public void testValidateDatasetWrongScheme() {
        expectThrows(IllegalArgumentException.class, () -> validator.validateDataset(Map.of(), "s3://bucket/path", Map.of()));
    }

    public void testValidateDatasetRejectsUnknown() {
        expectThrows(
            IllegalArgumentException.class,
            () -> validator.validateDataset(Map.of(), "wasbs://c@a.blob.core.windows.net/p", Map.of("format", "parquet"))
        );
    }

    public void testValidateDatasetSchemaSampleSize() {
        assertEquals(
            "50",
            findValue(
                validator.validateDataset(Map.of(), "wasbs://c@a.blob.core.windows.net/p", Map.of("schema_sample_size", 50)),
                "schema_sample_size"
            )
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> validator.validateDataset(Map.of(), "wasbs://c@a.blob.core.windows.net/p", Map.of("schema_sample_size", 0))
        );
    }

    private static String findValue(Map<ConfigSetting, String> settings, String name) {
        return settings.entrySet().stream().filter(e -> e.getKey().name().equals(name)).map(Map.Entry::getValue).findFirst().orElseThrow();
    }

    private static ConfigSetting findKey(Map<ConfigSetting, String> settings, String name) {
        return settings.keySet().stream().filter(s -> s.name().equals(name)).findFirst().orElseThrow();
    }
}
