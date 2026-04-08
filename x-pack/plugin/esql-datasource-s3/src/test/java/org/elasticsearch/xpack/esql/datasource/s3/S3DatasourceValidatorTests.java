/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ConfigSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DatasourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FileDatasourceValidator;

import java.util.Map;
import java.util.Set;

public class S3DatasourceValidatorTests extends ESTestCase {

    private final DatasourceValidator validator = new FileDatasourceValidator(
        "s3",
        S3Configuration::fromMap,
        Set.of("s3://", "s3a://", "s3n://")
    );

    public void testType() {
        assertEquals("s3", validator.type());
    }

    public void testValidateDatasourceWithCredentials() {
        var result = validator.validateDatasource(Map.of("access_key", "AKIA123", "secret_key", "secret", "region", "us-east-1"));
        assertEquals("AKIA123", findValue(result, "access_key"));
        assertTrue(findKey(result, "access_key").isSecret());
        assertTrue(findKey(result, "secret_key").isSecret());
        assertEquals("us-east-1", findValue(result, "region"));
        assertFalse(findKey(result, "region").isSecret());
    }

    public void testValidateDatasourceEmpty() {
        assertTrue(validator.validateDatasource(Map.of()).isEmpty());
    }

    public void testValidateDatasourceRejectsUnknown() {
        expectThrows(IllegalArgumentException.class, () -> validator.validateDatasource(Map.of("bucket", "x")));
    }

    public void testValidateDatasourceRejectsInvalidAuth() {
        expectThrows(IllegalArgumentException.class, () -> validator.validateDatasource(Map.of("auth", "oauth2")));
    }

    public void testValidateDatasourceNormalizesAuth() {
        var result = validator.validateDatasource(Map.of("auth", "NONE"));
        assertEquals("none", findValue(result, "auth"));
        assertFalse(findKey(result, "auth").isSecret());
    }

    public void testValidateDatasourceAnonymousConflict() {
        expectThrows(
            IllegalArgumentException.class,
            () -> validator.validateDatasource(Map.of("auth", "none", "access_key", "AKIA123", "secret_key", "secret"))
        );
    }

    public void testValidateDatasourceNullSettings() {
        assertTrue(validator.validateDatasource(null).isEmpty());
    }

    public void testValidateDatasourceSkipsNullValues() {
        var settings = new java.util.HashMap<String, Object>();
        settings.put("region", "us-east-1");
        settings.put("endpoint", null);
        var result = validator.validateDatasource(settings);
        assertEquals("us-east-1", findValue(result, "region"));
        assertNull(findValueOrNull(result, "endpoint"));
    }

    public void testValidateDatasetValid() {
        var result = validator.validateDataset(Map.of(), "s3://bucket/path/*.parquet", Map.of("partition_detection", "hive"));
        assertEquals("hive", findValue(result, "partition_detection"));
    }

    public void testValidateDatasetRequiresResource() {
        expectThrows(IllegalArgumentException.class, () -> validator.validateDataset(Map.of(), null, Map.of()));
    }

    public void testValidateDatasetBlankResource() {
        expectThrows(IllegalArgumentException.class, () -> validator.validateDataset(Map.of(), "", Map.of()));
    }

    public void testValidateDatasetWrongScheme() {
        expectThrows(IllegalArgumentException.class, () -> validator.validateDataset(Map.of(), "gs://bucket/path", Map.of()));
    }

    public void testValidateDatasetAllSchemes() {
        for (String uri : new String[] { "s3://b/p", "s3a://b/p", "s3n://b/p" }) {
            assertNotNull(validator.validateDataset(Map.of(), uri, Map.of()));
        }
    }

    public void testValidateDatasetRejectsUnknown() {
        expectThrows(IllegalArgumentException.class, () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("format", "parquet")));
    }

    public void testValidateDatasetSchemaSampleSize() {
        assertEquals(
            "50",
            findValue(validator.validateDataset(Map.of(), "s3://b/p", Map.of("schema_sample_size", 50)), "schema_sample_size")
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("schema_sample_size", 0))
        );
    }

    public void testValidateDatasetSchemaSampleSizeNonNumber() {
        expectThrows(
            IllegalArgumentException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("schema_sample_size", "abc"))
        );
    }

    public void testValidateDatasetNullSettings() {
        assertTrue(validator.validateDataset(Map.of(), "s3://b/p", null).isEmpty());
    }

    private static String findValue(Map<ConfigSetting, String> settings, String name) {
        return settings.entrySet().stream().filter(e -> e.getKey().name().equals(name)).map(Map.Entry::getValue).findFirst().orElseThrow();
    }

    private static String findValueOrNull(Map<ConfigSetting, String> settings, String name) {
        return settings.entrySet().stream().filter(e -> e.getKey().name().equals(name)).map(Map.Entry::getValue).findFirst().orElse(null);
    }

    private static ConfigSetting findKey(Map<ConfigSetting, String> settings, String name) {
        return settings.keySet().stream().filter(s -> s.name().equals(name)).findFirst().orElseThrow();
    }
}
