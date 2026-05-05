/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * Unit tests for AzureConfiguration.
 * Tests parsing Azure credentials and configuration from query parameters.
 */
public class AzureConfigurationTests extends ESTestCase {

    public void testFromMapWithAllFields() {
        AzureConfiguration config = AzureConfiguration.fromMap(
            Map.of(
                "connection_string",
                "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key",
                "account",
                "myaccount",
                "key",
                "mykey",
                "sas_token",
                "?sv=2020-01-01",
                "endpoint",
                "https://myaccount.blob.core.windows.net"
            )
        );

        assertNotNull(config);
        assertEquals("DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key", config.connectionString());
        assertEquals("myaccount", config.account());
        assertEquals("mykey", config.key());
        assertEquals("?sv=2020-01-01", config.sasToken());
        assertEquals("https://myaccount.blob.core.windows.net", config.endpoint());
        assertTrue(config.hasCredentials());
    }

    public void testFromMapWithConnectionStringOnly() {
        AzureConfiguration config = AzureConfiguration.fromMap(
            Map.of("connection_string", "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key")
        );

        assertNotNull(config);
        assertEquals("DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key", config.connectionString());
        assertTrue(config.hasCredentials());
    }

    public void testFromMapWithAccountAndKey() {
        AzureConfiguration config = AzureConfiguration.fromMap(Map.of("account", "myaccount", "key", "mykey"));

        assertNotNull(config);
        assertEquals("myaccount", config.account());
        assertEquals("mykey", config.key());
        assertTrue(config.hasCredentials());
    }

    public void testFromMapWithNullMapReturnsNull() {
        assertNull(AzureConfiguration.fromMap(null));
    }

    public void testFromMapWithEmptyMapReturnsNull() {
        assertNull(AzureConfiguration.fromMap(new HashMap<>()));
    }

    public void testFromMapWithUnknownParamsThrows() {
        expectThrows(ValidationException.class, () -> AzureConfiguration.fromMap(Map.of("other_param", "value")));
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
        assertNull(AzureConfiguration.fromFields(null, null, null, null, null));
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

    public void testAuthNone() {
        AzureConfiguration config = AzureConfiguration.fromFields(null, null, null, null, "https://endpoint", "none");
        assertNotNull(config);
        assertTrue(config.isAnonymous());
        assertFalse(config.hasCredentials());
    }

    public void testAuthNoneCaseInsensitive() {
        AzureConfiguration config = AzureConfiguration.fromFields(null, null, null, null, "https://endpoint", "NONE");
        assertTrue(config.isAnonymous());
        assertEquals("none", config.auth());
    }

    public void testAuthNoneConflictsWithConnectionString() {
        expectThrows(ValidationException.class, () -> AzureConfiguration.fromFields("connstr", null, null, null, null, "none"));
    }

    public void testAuthNoneConflictsWithAccountKey() {
        expectThrows(ValidationException.class, () -> AzureConfiguration.fromFields(null, "acc", "key", null, null, "none"));
    }

    public void testAuthNoneConflictsWithSasToken() {
        expectThrows(ValidationException.class, () -> AzureConfiguration.fromFields(null, null, null, "sas", null, "none"));
    }

    public void testAuthNoneAllowsEndpoint() {
        AzureConfiguration config = AzureConfiguration.fromFields(null, null, null, null, "https://ep", "none");
        assertTrue(config.isAnonymous());
        assertEquals("https://ep", config.endpoint());
    }

    public void testUnsupportedAuthValueThrows() {
        expectThrows(ValidationException.class, () -> AzureConfiguration.fromFields(null, null, null, null, "https://ep", "unsupported"));
    }

    public void testFromMapRejectsUnknownKeys() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("account", "acc");
        raw.put("header_row", false);
        ValidationException e = expectThrows(ValidationException.class, () -> AzureConfiguration.fromMap(raw));
        assertThat(e.getMessage(), containsString("unknown setting [header_row]"));
    }

    public void testFromQueryConfigDropsUnknownKeys() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("account", "myaccount");
        raw.put("key", "mykey");
        raw.put("endpoint", "https://ep");
        raw.put("header_row", false);
        raw.put("column_prefix", "f");

        AzureConfiguration config = AzureConfiguration.fromQueryConfig(raw);
        assertNotNull(config);
        assertEquals("myaccount", config.account());
        assertEquals("mykey", config.key());
        assertEquals("https://ep", config.endpoint());
    }

    public void testFromQueryConfigStillEnforcesAuthConflict() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("auth", "none");
        raw.put("connection_string", "connstr");
        raw.put("header_row", false);
        ValidationException e = expectThrows(ValidationException.class, () -> AzureConfiguration.fromQueryConfig(raw));
        assertThat(e.getMessage(), containsString("auth=none cannot be combined with explicit credentials"));
    }

    public void testFromQueryConfigWithOnlyUnknownKeysReturnsNull() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("header_row", false);
        raw.put("column_prefix", "f");
        assertNull(AzureConfiguration.fromQueryConfig(raw));
    }

    public void testFromQueryConfigWithNullReturnsNull() {
        assertNull(AzureConfiguration.fromQueryConfig(null));
    }
}
