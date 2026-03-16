/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for AzureConfiguration.
 * Tests parsing Azure credentials and configuration from query parameters.
 */
public class AzureConfigurationTests extends ESTestCase {

    private static final Source SOURCE = Source.EMPTY;

    public void testFromParamsWithAllFields() {
        Map<String, Expression> params = new HashMap<>();
        params.put("connection_string", literal("DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key"));
        params.put("account", literal("myaccount"));
        params.put("key", literal("mykey"));
        params.put("sas_token", literal("?sv=2020-01-01"));
        params.put("endpoint", literal("https://myaccount.blob.core.windows.net"));

        AzureConfiguration config = AzureConfiguration.fromParams(params);

        assertNotNull(config);
        assertEquals("DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key", config.connectionString());
        assertEquals("myaccount", config.account());
        assertEquals("mykey", config.key());
        assertEquals("?sv=2020-01-01", config.sasToken());
        assertEquals("https://myaccount.blob.core.windows.net", config.endpoint());
        assertTrue(config.hasCredentials());
    }

    public void testFromParamsWithConnectionStringOnly() {
        Map<String, Expression> params = new HashMap<>();
        params.put("connection_string", literal("DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key"));

        AzureConfiguration config = AzureConfiguration.fromParams(params);

        assertNotNull(config);
        assertEquals("DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key", config.connectionString());
        assertTrue(config.hasCredentials());
    }

    public void testFromParamsWithAccountAndKey() {
        Map<String, Expression> params = new HashMap<>();
        params.put("account", literal("myaccount"));
        params.put("key", literal("mykey"));

        AzureConfiguration config = AzureConfiguration.fromParams(params);

        assertNotNull(config);
        assertEquals("myaccount", config.account());
        assertEquals("mykey", config.key());
        assertTrue(config.hasCredentials());
    }

    public void testFromParamsWithNullMapReturnsNull() {
        AzureConfiguration config = AzureConfiguration.fromParams(null);
        assertNull(config);
    }

    public void testFromParamsWithEmptyMapReturnsNull() {
        AzureConfiguration config = AzureConfiguration.fromParams(new HashMap<>());
        assertNull(config);
    }

    public void testFromParamsWithNoAzureParamsReturnsNull() {
        Map<String, Expression> params = new HashMap<>();
        params.put("other_param", literal("value"));

        AzureConfiguration config = AzureConfiguration.fromParams(params);

        assertNull(config);
    }

    public void testFromFieldsWithAllFields() {
        AzureConfiguration config = AzureConfiguration.fromFields("connstr", "account", "key", "sas", "https://endpoint");

        assertNotNull(config);
        assertEquals("connstr", config.connectionString());
        assertEquals("account", config.account());
        assertEquals("key", config.key());
        assertEquals("sas", config.sasToken());
        assertEquals("https://endpoint", config.endpoint());
        assertTrue(config.hasCredentials());
    }

    public void testFromFieldsWithAllNullReturnsNull() {
        AzureConfiguration config = AzureConfiguration.fromFields(null, null, null, null, null);
        assertNull(config);
    }

    public void testHasCredentialsWithConnectionString() {
        AzureConfiguration config = AzureConfiguration.fromFields("connstr", null, null, null, null);
        assertTrue(config.hasCredentials());
    }

    public void testHasCredentialsWithAccountAndKey() {
        AzureConfiguration config = AzureConfiguration.fromFields(null, "account", "key", null, null);
        assertTrue(config.hasCredentials());
    }

    public void testHasCredentialsWithSasToken() {
        AzureConfiguration config = AzureConfiguration.fromFields(null, "account", null, "sas", null);
        assertTrue(config.hasCredentials());
    }

    public void testHasCredentialsWithoutCredentials() {
        AzureConfiguration config = AzureConfiguration.fromFields(null, null, null, null, "https://endpoint");
        assertFalse(config.hasCredentials());
    }

    public void testEqualsAndHashCodeSameValues() {
        AzureConfiguration config1 = AzureConfiguration.fromFields("cs", "acc", "key", "sas", "ep");
        AzureConfiguration config2 = AzureConfiguration.fromFields("cs", "acc", "key", "sas", "ep");

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    public void testEqualsAndHashCodeDifferentValues() {
        AzureConfiguration config1 = AzureConfiguration.fromFields("cs1", "acc", "key", "sas", "ep");
        AzureConfiguration config2 = AzureConfiguration.fromFields("cs2", "acc", "key", "sas", "ep");

        assertNotEquals(config1, config2);
    }

    private Literal literal(Object value) {
        Object literalValue = value instanceof String s ? new BytesRef(s) : value;
        DataType dataType = value instanceof String ? DataType.KEYWORD : DataType.KEYWORD;
        return new Literal(SOURCE, literalValue, dataType);
    }
}
