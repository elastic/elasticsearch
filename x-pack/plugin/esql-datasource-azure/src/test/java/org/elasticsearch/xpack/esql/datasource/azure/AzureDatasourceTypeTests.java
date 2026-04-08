/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DatasourceType;
import org.elasticsearch.xpack.esql.datasources.spi.FileDatasourceType;

import java.util.Map;
import java.util.Set;

public class AzureDatasourceTypeTests extends ESTestCase {

    private final DatasourceType type = new FileDatasourceType("azure_blob", AzureConfiguration::fromMap, Set.of("wasbs://", "wasb://"));

    public void testType() {
        assertEquals("azure_blob", type.type());
    }

    public void testValidateDatasourceWithSharedKey() {
        var result = type.validateDatasource(Map.of("account", "myaccount", "key", "mykey"));
        assertEquals("myaccount", result.get("account").value());
        assertFalse(result.get("account").isSecret());
        assertEquals("mykey", result.get("key").value());
        assertTrue(result.get("key").isSecret());
    }

    public void testValidateDatasourceEmpty() {
        assertTrue(type.validateDatasource(Map.of()).isEmpty());
    }

    public void testValidateDatasourceRejectsUnknown() {
        expectThrows(IllegalArgumentException.class, () -> type.validateDatasource(Map.of("container", "x")));
    }

    public void testValidateDatasourceNormalizesAuth() {
        var result = type.validateDatasource(Map.of("auth", "NONE"));
        assertEquals("none", result.get("auth").value());
    }

    public void testValidateDatasetValid() {
        var result = type.validateDataset(Map.of(), "wasbs://c@a.blob.core.windows.net/p/*.parquet", Map.of("error_mode", "skip_row"));
        assertEquals("skip_row", result.get("error_mode").value());
    }

    public void testValidateDatasetBothSchemes() {
        for (String uri : new String[] { "wasbs://c@a.blob.core.windows.net/p", "wasb://c@a.blob.core.windows.net/p" }) {
            assertNotNull(type.validateDataset(Map.of(), uri, Map.of()));
        }
    }

    public void testValidateDatasetWrongScheme() {
        expectThrows(IllegalArgumentException.class, () -> type.validateDataset(Map.of(), "s3://bucket/path", Map.of()));
    }

    public void testValidateDatasetRejectsUnknown() {
        expectThrows(
            IllegalArgumentException.class,
            () -> type.validateDataset(Map.of(), "wasbs://c@a.blob.core.windows.net/p", Map.of("format", "parquet"))
        );
    }
}
