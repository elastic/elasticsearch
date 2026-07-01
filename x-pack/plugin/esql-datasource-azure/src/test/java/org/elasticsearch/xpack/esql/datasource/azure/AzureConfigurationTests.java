/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
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

    public void testSasTokenWithoutAccountRejectedAtCreate() {
        // sas_token alone is not a complete static credential — the provider needs account+sas. Requiring account
        // means auto has nothing to resolve, so it is rejected at create rather than resolving to static and failing
        // at query time. (Explicit auth=static_credentials with sas-alone is rejected the same way.)
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> AzureConfiguration.fromFields(null, null, null, "?sv=2020-01-01", null)
        );
        assertTrue(e.getMessage().contains("requires credentials"));

        ValidationException explicit = expectThrows(
            ValidationException.class,
            () -> AzureConfiguration.fromFields(null, null, null, "?sv=2020-01-01", null, "static_credentials")
        );
        assertTrue(explicit.getMessage().contains("requires complete explicit credentials"));
    }

    public void testWithoutCredentialsRejectedAtCreate() {
        // Only an endpoint, no credential — auto has nothing to resolve, so construction is rejected at create.
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> AzureConfiguration.fromFields(null, null, null, null, "https://endpoint")
        );
        assertTrue(e.getMessage().contains("requires credentials"));
    }

    public void testWhitespaceSasTokenIsAbsentRejectedAtCreate() {
        // A whitespace-only SAS token is treated as absent (consistent with S3/GCS short-lived tokens), so it does
        // not count as credentials — leaving auto unresolvable, which is rejected at create.
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> AzureConfiguration.fromFields(null, "account", null, "   ", null)
        );
        assertTrue(e.getMessage().contains("requires credentials"));
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

    public void testNotEqualsWithDifferentSasToken() {
        AzureConfiguration config1 = AzureConfiguration.fromFields(null, "acc", null, "sas1", "ep");
        AzureConfiguration config2 = AzureConfiguration.fromFields(null, "acc", null, "sas2", "ep");
        assertNotEquals(config1, config2);
    }

    public void testSasTokenAbsentByDefault() {
        AzureConfiguration config = AzureConfiguration.fromFields(null, "account", "key", null, null);
        assertNotNull(config);
        assertNull(config.sasToken());
    }

    public void testAuthAnonymous() {
        AzureConfiguration config = AzureConfiguration.fromFields(null, null, null, null, "https://endpoint", "anonymous");
        assertNotNull(config);
        assertTrue(config.isAnonymous());
        assertFalse(config.hasCredentials());
    }

    public void testAuthAnonymousCaseInsensitive() {
        AzureConfiguration config = AzureConfiguration.fromFields(null, null, null, null, "https://endpoint", "ANONYMOUS");
        assertTrue(config.isAnonymous());
        assertEquals("anonymous", config.auth());
    }

    public void testAuthAnonymousConflictsWithConnectionString() {
        expectThrows(ValidationException.class, () -> AzureConfiguration.fromFields("connstr", null, null, null, null, "anonymous"));
    }

    public void testAuthAnonymousConflictsWithAccountKey() {
        expectThrows(ValidationException.class, () -> AzureConfiguration.fromFields(null, "acc", "key", null, null, "anonymous"));
    }

    public void testAuthAnonymousConflictsWithSasToken() {
        expectThrows(ValidationException.class, () -> AzureConfiguration.fromFields(null, null, null, "sas", null, "anonymous"));
    }

    public void testAuthAnonymousAllowsEndpoint() {
        AzureConfiguration config = AzureConfiguration.fromFields(null, null, null, null, "https://ep", "anonymous");
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

        Configured<AzureConfiguration> result = AzureConfiguration.fromQueryConfig(raw);
        AzureConfiguration config = result.value();
        assertNotNull(config);
        assertEquals("myaccount", config.account());
        assertEquals("mykey", config.key());
        assertEquals("https://ep", config.endpoint());
        assertThat(result.consumedKeys(), containsInAnyOrder("account", "key", "endpoint"));
    }

    public void testFromQueryConfigWithSasToken() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("account", "myaccount");
        raw.put("sas_token", "?sv=2020-01-01");
        raw.put("header_row", false);

        Configured<AzureConfiguration> result = AzureConfiguration.fromQueryConfig(raw);
        AzureConfiguration config = result.value();
        assertNotNull(config);
        assertEquals("?sv=2020-01-01", config.sasToken());
        assertTrue(config.hasCredentials());
        assertThat(result.consumedKeys(), containsInAnyOrder("account", "sas_token"));
    }

    public void testFromQueryConfigStillEnforcesAuthConflict() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("auth", "anonymous");
        raw.put("connection_string", "connstr");
        raw.put("header_row", false);
        ValidationException e = expectThrows(ValidationException.class, () -> AzureConfiguration.fromQueryConfig(raw));
        assertThat(e.getMessage(), containsString("auth=anonymous cannot be combined with explicit credentials"));
    }

    public void testFromQueryConfigWithOnlyUnknownKeysReturnsNull() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("header_row", false);
        raw.put("column_prefix", "f");
        Configured<AzureConfiguration> result = AzureConfiguration.fromQueryConfig(raw);
        assertNull(result.value());
        assertEquals(Set.of(), result.consumedKeys());
    }

    public void testFromQueryConfigWithNullReturnsNull() {
        Configured<AzureConfiguration> result = AzureConfiguration.fromQueryConfig(null);
        assertNull(result.value());
        assertEquals(Set.of(), result.consumedKeys());
    }

    public void testKeylessAuthWithAllFields() {
        AzureConfiguration config = AzureConfiguration.fromMap(
            Map.of("tenant_id", "my-tenant", "client_id", "my-client", "jwt_audience", "api://AzureADTokenExchange")
        );

        assertNotNull(config);
        assertEquals("my-tenant", config.tenantId());
        assertEquals("my-client", config.clientId());
        assertEquals("api://AzureADTokenExchange", config.jwtAudience());
        assertTrue(config.hasKeylessAuth());
        assertFalse(config.hasCredentials());
    }

    public void testKeylessAuthRequiresAllFields() {
        ValidationException e = expectThrows(ValidationException.class, () -> AzureConfiguration.fromMap(Map.of("tenant_id", "my-tenant")));
        assertThat(e.getMessage(), containsString("client_id is required when keyless authentication settings are configured"));
        assertThat(e.getMessage(), containsString("jwt_audience is required when keyless authentication settings are configured"));
    }

    public void testKeylessAuthConflictsWithExplicitCredentials() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> AzureConfiguration.fromMap(
                Map.of("account", "acc", "key", "k", "tenant_id", "my-tenant", "client_id", "my-client", "jwt_audience", "aud")
            )
        );
        assertThat(e.getMessage(), containsString("explicit credentials cannot be combined with keyless authentication settings"));
    }

    public void testKeylessAuthConflictsWithAuthAnonymous() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> AzureConfiguration.fromMap(
                Map.of("auth", "anonymous", "tenant_id", "my-tenant", "client_id", "my-client", "jwt_audience", "aud")
            )
        );
        assertThat(e.getMessage(), containsString("auth=anonymous cannot be combined with keyless authentication settings"));
    }

    public void testAuthFederatedIdentityExplicit() {
        AzureConfiguration config = AzureConfiguration.fromMap(
            Map.of("auth", "federated_identity", "tenant_id", "my-tenant", "client_id", "my-client", "jwt_audience", "aud")
        );
        assertTrue(config.isFederatedIdentity());
        assertTrue(config.hasKeylessAuth());
        assertEquals("federated_identity", config.auth());
    }

    public void testAuthStaticCredentialsRejectsIncompleteCredentials() {
        // A key with no account is an incomplete Azure static credential — explicit static_credentials rejects it.
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> AzureConfiguration.fromMap(Map.of("auth", "static_credentials", "key", "k"))
        );
        assertThat(e.getMessage(), containsString("auth=static_credentials requires complete explicit credentials"));
    }

    public void testAuthStaticCredentialsAcceptsAccountAndKey() {
        AzureConfiguration config = AzureConfiguration.fromMap(Map.of("auth", "static_credentials", "account", "acc", "key", "k"));
        assertTrue(config.isStaticCredentials());
        assertTrue(config.hasCredentials());
    }

    // --- Backwards compatibility: deprecated value names are canonicalized on parse and warn ---

    public void testDeprecatedAuthNoneCanonicalizedToAnonymous() {
        AzureConfiguration config = AzureConfiguration.fromMap(Map.of("auth", "none"));
        assertEquals("anonymous", config.auth());
        assertTrue(config.isAnonymous());
        assertWarnings("auth value [none] is deprecated; the canonical value is [anonymous]");
    }

    public void testDeprecatedAuthWorkloadIdentityCanonicalizedToManagedIdentity() {
        AzureConfiguration config = AzureConfiguration.fromMap(Map.of("auth", "workload_identity"));
        assertEquals("managed_identity", config.auth());
        assertTrue(config.isManagedIdentity());
        assertWarnings("auth value [workload_identity] is deprecated; the canonical value is [managed_identity]");
    }
}
