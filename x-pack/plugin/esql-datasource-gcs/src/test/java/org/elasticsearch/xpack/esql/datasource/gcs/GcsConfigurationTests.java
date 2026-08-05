/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;

/**
 * Unit tests for GcsConfiguration.
 * Tests parsing GCS credentials and configuration from query parameters.
 */
public class GcsConfigurationTests extends ESTestCase {

    public void testFromMapWithAllFields() {
        GcsConfiguration config = GcsConfiguration.fromMap(
            Map.of(
                "credentials",
                "{\"type\":\"service_account\",\"project_id\":\"test\"}",
                "project_id",
                "my-project",
                "endpoint",
                "http://localhost:4443"
            )
        );

        assertNotNull(config);
        assertEquals("{\"type\":\"service_account\",\"project_id\":\"test\"}", config.serviceAccountCredentials());
        assertEquals("my-project", config.projectId());
        assertEquals("http://localhost:4443", config.endpoint());
        assertTrue(config.hasCredentials());
    }

    public void testFromMapWithCredentialsOnly() {
        GcsConfiguration config = GcsConfiguration.fromMap(Map.of("credentials", "{\"type\":\"service_account\"}"));

        assertNotNull(config);
        assertEquals("{\"type\":\"service_account\"}", config.serviceAccountCredentials());
        assertNull(config.projectId());
        assertNull(config.endpoint());
        assertTrue(config.hasCredentials());
    }

    public void testProjectIdOnlyRejectedAtCreate() {
        // project_id carries no credential, so auto (omitted auth) resolves to nothing and is rejected at create.
        ValidationException e = expectThrows(ValidationException.class, () -> GcsConfiguration.fromMap(Map.of("project_id", "my-project")));
        assertTrue(e.getMessage().contains("requires credentials"));
    }

    public void testEndpointOnlyRejectedAtCreate() {
        // endpoint carries no credential, so auto (omitted auth) resolves to nothing and is rejected at create.
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> GcsConfiguration.fromMap(Map.of("endpoint", "http://localhost:4443"))
        );
        assertTrue(e.getMessage().contains("requires credentials"));
    }

    public void testFromMapWithNullMapReturnsNull() {
        assertNull(GcsConfiguration.fromMap(null));
    }

    public void testFromMapWithEmptyMapReturnsNull() {
        assertNull(GcsConfiguration.fromMap(new HashMap<>()));
    }

    public void testFromMapWithUnknownParamsThrows() {
        expectThrows(ValidationException.class, () -> GcsConfiguration.fromMap(Map.of("other_param", "value")));
    }

    public void testFromMapWithStringValues() {
        GcsConfiguration config = GcsConfiguration.fromMap(
            Map.of("credentials", "{\"type\":\"service_account\"}", "project_id", "my-project")
        );

        assertNotNull(config);
        assertEquals("{\"type\":\"service_account\"}", config.serviceAccountCredentials());
        assertEquals("my-project", config.projectId());
    }

    public void testFromFieldsWithAllFields() {
        GcsConfiguration config = GcsConfiguration.fromFields("{\"type\":\"service_account\"}", "my-project", "http://localhost:4443");

        assertNotNull(config);
        assertEquals("{\"type\":\"service_account\"}", config.serviceAccountCredentials());
        assertEquals("my-project", config.projectId());
        assertEquals("http://localhost:4443", config.endpoint());
        assertTrue(config.hasCredentials());
    }

    public void testNullCredentialsRejectedAtCreate() {
        // Null credentials with only project_id/endpoint leaves auto with nothing to resolve — rejected at create.
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> GcsConfiguration.fromFields(null, "my-project", "http://localhost:4443")
        );
        assertTrue(e.getMessage().contains("requires credentials"));
    }

    public void testFromFieldsWithAllNullReturnsNull() {
        assertNull(GcsConfiguration.fromFields(null, null, null));
    }

    public void testHasCredentialsWithCredentials() {
        GcsConfiguration config = GcsConfiguration.fromFields("{\"type\":\"service_account\"}", null, null);
        assertTrue(config.hasCredentials());
    }

    public void testWithoutCredentialsRejectedAtCreate() {
        // No credential field set — auto has nothing to resolve, so construction is rejected at create.
        ValidationException e = expectThrows(ValidationException.class, () -> GcsConfiguration.fromFields(null, "my-project", null));
        assertTrue(e.getMessage().contains("requires credentials"));
    }

    public void testEqualsAndHashCodeSameValues() {
        GcsConfiguration config1 = GcsConfiguration.fromFields("creds", "project", "endpoint");
        GcsConfiguration config2 = GcsConfiguration.fromFields("creds", "project", "endpoint");

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    public void testEqualsAndHashCodeDifferentCredentials() {
        GcsConfiguration config1 = GcsConfiguration.fromFields("creds1", "project", "endpoint");
        GcsConfiguration config2 = GcsConfiguration.fromFields("creds2", "project", "endpoint");

        assertNotEquals(config1, config2);
    }

    public void testEqualsAndHashCodeDifferentProjectId() {
        GcsConfiguration config1 = GcsConfiguration.fromFields("creds", "project1", "endpoint");
        GcsConfiguration config2 = GcsConfiguration.fromFields("creds", "project2", "endpoint");

        assertNotEquals(config1, config2);
    }

    public void testEqualsAndHashCodeDifferentEndpoint() {
        GcsConfiguration config1 = GcsConfiguration.fromFields("creds", "project", "endpoint1");
        GcsConfiguration config2 = GcsConfiguration.fromFields("creds", "project", "endpoint2");

        assertNotEquals(config1, config2);
    }

    public void testEqualsWithNull() {
        GcsConfiguration config = GcsConfiguration.fromFields("creds", "project", "endpoint");
        assertNotEquals(null, config);
    }

    public void testEqualsWithDifferentClass() {
        GcsConfiguration config = GcsConfiguration.fromFields("creds", "project", "endpoint");
        assertNotEquals("not a config", config);
    }

    public void testEqualsSameInstance() {
        GcsConfiguration config = GcsConfiguration.fromFields("creds", "project", "endpoint");
        assertEquals(config, config);
    }

    public void testEqualsWithNullFields() {
        // anonymous makes the no-credential config resolvable; equality still exercises the null credentials/project_id fields.
        GcsConfiguration config1 = GcsConfiguration.fromFields(null, null, "endpoint", null, "anonymous");
        GcsConfiguration config2 = GcsConfiguration.fromFields(null, null, "endpoint", null, "anonymous");

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    public void testAuthAnonymous() {
        GcsConfiguration config = GcsConfiguration.fromFields(null, null, "http://endpoint", null, "anonymous");
        assertNotNull(config);
        assertTrue(config.isAnonymous());
        assertFalse(config.hasCredentials());
    }

    public void testAuthAnonymousCaseInsensitive() {
        GcsConfiguration config = GcsConfiguration.fromFields(null, null, "http://endpoint", null, "ANONYMOUS");
        assertTrue(config.isAnonymous());
        assertEquals("anonymous", config.auth());
    }

    public void testAuthAnonymousConflictsWithCredentials() {
        expectThrows(
            ValidationException.class,
            () -> GcsConfiguration.fromFields("{\"type\":\"service_account\"}", null, null, null, "anonymous")
        );
    }

    public void testAuthAnonymousConflictsWithFederatedAuth() {
        expectThrows(
            ValidationException.class,
            () -> GcsConfiguration.fromFields(null, null, "http://endpoint", null, "anonymous", "jwt-audience", null, null)
        );
    }

    public void testCredentialsConflictsWithFederatedAuth() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> GcsConfiguration.fromFields("{\"type\":\"service_account\"}", null, null, null, null, "jwt-audience", null, null)
        );
        assertThat(e.getMessage(), containsString("explicit credentials cannot be combined with federated authentication settings"));
    }

    public void testHasFederatedAuth() {
        GcsConfiguration config = GcsConfiguration.fromFields(
            null,
            "my-project",
            null,
            null,
            null,
            "jwt-audience",
            "sts-audience",
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/sa@project.iam.gserviceaccount.com:generateAccessToken"
        );
        assertTrue(config.hasFederatedAuth());
        assertFalse(config.hasCredentials());
    }

    public void testFederatedAuthRequiresStsAudience() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> GcsConfiguration.fromFields(null, null, null, null, null, "jwt-audience", null, null)
        );
        assertThat(e.getMessage(), containsString("sts_audience is required"));
    }

    public void testFederatedAuthAllowsOmittingJwtAudience() {
        GcsConfiguration config = GcsConfiguration.fromFields(null, null, null, null, null, null, "sts-audience", null);
        assertTrue(config.hasFederatedAuth());
        assertNull(config.jwtAudience());
        assertEquals("sts-audience", config.stsAudience());
    }

    public void testFederatedAuthAllowsOmittingServiceAccountImpersonationUrl() {
        GcsConfiguration config = GcsConfiguration.fromFields(null, null, null, null, null, "jwt-audience", "sts-audience", null);
        assertTrue(config.hasFederatedAuth());
        assertEquals("jwt-audience", config.jwtAudience());
        assertEquals("sts-audience", config.stsAudience());
        assertNull(config.serviceAccountImpersonationUrl());
    }

    public void testAuthAnonymousAllowsProjectIdAndEndpoint() {
        GcsConfiguration config = GcsConfiguration.fromFields(null, "my-project", "http://ep", null, "anonymous");
        assertTrue(config.isAnonymous());
        assertEquals("my-project", config.projectId());
    }

    public void testUnsupportedAuthValueThrows() {
        expectThrows(ValidationException.class, () -> GcsConfiguration.fromFields(null, null, "http://ep", null, "unsupported"));
    }

    public void testFromMapRejectsUnknownKeys() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("credentials", "{\"type\":\"service_account\"}");
        raw.put("header_row", false);
        ValidationException e = expectThrows(ValidationException.class, () -> GcsConfiguration.fromMap(raw));
        assertThat(e.getMessage(), containsString("unknown setting [header_row]"));
    }

    public void testFromQueryConfigDropsUnknownKeys() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("credentials", "{\"type\":\"service_account\"}");
        raw.put("project_id", "my-project");
        raw.put("endpoint", "http://localhost:4443");
        raw.put("header_row", false);
        raw.put("column_prefix", "f");

        Configured<GcsConfiguration> result = GcsConfiguration.fromQueryConfig(raw);
        GcsConfiguration config = result.value();
        assertNotNull(config);
        assertEquals("{\"type\":\"service_account\"}", config.serviceAccountCredentials());
        assertEquals("my-project", config.projectId());
        assertEquals("http://localhost:4443", config.endpoint());
        assertThat(result.consumedKeys(), containsInAnyOrder("credentials", "project_id", "endpoint"));
    }

    public void testFromQueryConfigStillEnforcesAuthConflict() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("auth", "anonymous");
        raw.put("credentials", "{\"type\":\"service_account\"}");
        raw.put("header_row", false);
        ValidationException e = expectThrows(ValidationException.class, () -> GcsConfiguration.fromQueryConfig(raw));
        assertThat(e.getMessage(), containsString("auth=anonymous cannot be combined with explicit credentials"));
    }

    public void testFromQueryConfigWithOnlyUnknownKeysReturnsNull() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("header_row", false);
        raw.put("column_prefix", "f");
        Configured<GcsConfiguration> result = GcsConfiguration.fromQueryConfig(raw);
        assertNull(result.value());
        assertEquals(Set.of(), result.consumedKeys());
    }

    public void testFromQueryConfigWithNullReturnsNull() {
        Configured<GcsConfiguration> result = GcsConfiguration.fromQueryConfig(null);
        assertNull(result.value());
        assertEquals(Set.of(), result.consumedKeys());
    }

    public void testAccessToken() {
        GcsConfiguration config = GcsConfiguration.fromMap(Map.of("access_token", "ya29.token", "project_id", "my-project"));
        assertNotNull(config);
        assertEquals("ya29.token", config.accessToken());
        assertNull(config.serviceAccountCredentials());
        assertTrue(config.hasCredentials());
        assertFalse(config.isAnonymous());
    }

    public void testAccessTokenAbsentByDefault() {
        GcsConfiguration config = GcsConfiguration.fromFields("{\"type\":\"service_account\"}", null, null);
        assertNotNull(config);
        assertNull(config.accessToken());
    }

    public void testAccessTokenConflictsWithAuthAnonymous() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> GcsConfiguration.fromMap(Map.of("access_token", "ya29.token", "auth", "anonymous"))
        );
        assertThat(e.getMessage(), containsString("auth=anonymous cannot be combined with explicit credentials"));
    }

    public void testFromQueryConfigWithAccessToken() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("access_token", "ya29.token");
        raw.put("project_id", "my-project");
        raw.put("header_row", false);

        Configured<GcsConfiguration> result = GcsConfiguration.fromQueryConfig(raw);
        GcsConfiguration config = result.value();
        assertNotNull(config);
        assertEquals("ya29.token", config.accessToken());
        assertThat(result.consumedKeys(), containsInAnyOrder("access_token", "project_id"));
    }

    public void testEqualsWithAccessToken() {
        GcsConfiguration config1 = GcsConfiguration.fromMap(Map.of("access_token", "ya29.token"));
        GcsConfiguration config2 = GcsConfiguration.fromMap(Map.of("access_token", "ya29.token"));
        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    public void testNotEqualsWithDifferentAccessToken() {
        GcsConfiguration config1 = GcsConfiguration.fromMap(Map.of("access_token", "ya29.token1"));
        GcsConfiguration config2 = GcsConfiguration.fromMap(Map.of("access_token", "ya29.token2"));
        assertNotEquals(config1, config2);
    }

    public void testAuthFederatedIdentityExplicit() {
        GcsConfiguration config = GcsConfiguration.fromMap(
            Map.of("auth", "federated_identity", "jwt_audience", "aud", "sts_audience", "sts")
        );
        assertTrue(config.isFederatedIdentity());
        assertTrue(config.hasFederatedAuth());
        assertEquals("federated_identity", config.auth());
    }

    public void testAuthStaticCredentialsExplicit() {
        GcsConfiguration config = GcsConfiguration.fromMap(Map.of("auth", "static_credentials", "credentials", "{\"type\":\"x\"}"));
        assertTrue(config.isStaticCredentials());
        assertTrue(config.hasCredentials());
        assertEquals("static_credentials", config.auth());
    }

    public void testAuthStaticCredentialsRequiresCredentials() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> GcsConfiguration.fromMap(Map.of("auth", "static_credentials", "project_id", "p"))
        );
        assertThat(e.getMessage(), containsString("auth=static_credentials requires complete explicit credentials"));
    }

    // --- Backwards compatibility: deprecated value names are canonicalized on parse and warn ---

    public void testDeprecatedAuthNoneCanonicalizedToAnonymous() {
        GcsConfiguration config = GcsConfiguration.fromMap(Map.of("auth", "none"));
        assertEquals("anonymous", config.auth());
        assertTrue(config.isAnonymous());
        assertWarnings("auth value [none] is deprecated; the canonical value is [anonymous]");
    }

    public void testDeprecatedAuthWorkloadIdentityCanonicalizedToManagedIdentity() {
        GcsConfiguration config = GcsConfiguration.fromMap(Map.of("auth", "workload_identity"));
        assertEquals("managed_identity", config.auth());
        assertTrue(config.isManagedIdentity());
        assertWarnings("auth value [workload_identity] is deprecated; the canonical value is [managed_identity]");
    }
}
