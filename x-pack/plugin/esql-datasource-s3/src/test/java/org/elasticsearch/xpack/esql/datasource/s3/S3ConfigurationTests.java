/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;

public class S3ConfigurationTests extends ESTestCase {

    public void testFromFieldsWithAllFields() {
        S3Configuration config = S3Configuration.fromFields("ak", "sk", "http://endpoint", "us-west-2", null);
        assertNotNull(config);
        assertEquals("ak", config.accessKey());
        assertEquals("sk", config.secretKey());
        assertEquals("http://endpoint", config.endpoint());
        assertEquals("us-west-2", config.region());
        assertNull(config.auth());
        assertTrue(config.hasCredentials());
        assertFalse(config.isAnonymous());
    }

    public void testFromFieldsBackwardCompat() {
        S3Configuration config = S3Configuration.fromFields("ak", "sk", "http://endpoint", "us-west-2");
        assertNotNull(config);
        assertEquals("ak", config.accessKey());
        assertFalse(config.isAnonymous());
    }

    public void testAuthAnonymous() {
        S3Configuration config = S3Configuration.fromFields(null, null, "http://endpoint", "us-east-1", "anonymous");
        assertNotNull(config);
        assertTrue(config.isAnonymous());
        assertFalse(config.hasCredentials());
    }

    public void testAuthAnonymousCaseInsensitive() {
        S3Configuration config = S3Configuration.fromFields(null, null, "http://e", null, "ANONYMOUS");
        assertNotNull(config);
        assertTrue(config.isAnonymous());
        // case-insensitive fields normalized to lowercase
        assertEquals("anonymous", config.auth());
    }

    public void testUnsupportedAuthValueThrows() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> S3Configuration.fromFields(null, null, "http://e", null, "unsupported")
        );
        assertThat(e.getMessage(), containsString("Unsupported auth value"));
    }

    public void testAuthManagedIdentity() {
        S3Configuration config = S3Configuration.fromFields(null, null, null, "us-east-1", "managed_identity");
        assertNotNull(config);
        assertTrue(config.isManagedIdentity());
        assertFalse(config.isAnonymous());
        assertFalse(config.hasCredentials());
    }

    public void testAuthManagedIdentityCaseInsensitive() {
        S3Configuration config = S3Configuration.fromFields(null, null, null, null, "MANAGED_IDENTITY");
        assertNotNull(config);
        assertTrue(config.isManagedIdentity());
        assertEquals("managed_identity", config.auth());
    }

    public void testAuthManagedIdentityAllowsRegionAndEndpoint() {
        S3Configuration config = S3Configuration.fromFields(null, null, "http://localhost:9000", "eu-west-1", "managed_identity");
        assertNotNull(config);
        assertTrue(config.isManagedIdentity());
        assertEquals("http://localhost:9000", config.endpoint());
        assertEquals("eu-west-1", config.region());
    }

    public void testAuthManagedIdentityConflictsWithAccessKey() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> S3Configuration.fromFields("ak", null, null, null, "managed_identity")
        );
        assertThat(e.getMessage(), containsString("auth=managed_identity cannot be combined with explicit credentials"));
    }

    public void testAuthManagedIdentityConflictsWithSecretKey() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> S3Configuration.fromFields(null, "sk", null, null, "managed_identity")
        );
        assertThat(e.getMessage(), containsString("auth=managed_identity cannot be combined with explicit credentials"));
    }

    public void testAuthManagedIdentityConflictsWithSessionToken() {
        var raw = new java.util.HashMap<String, Object>();
        raw.put("auth", "managed_identity");
        raw.put("session_token", "tok");
        ValidationException e = expectThrows(ValidationException.class, () -> S3Configuration.fromMap(raw));
        assertThat(e.getMessage(), containsString("auth=managed_identity cannot be combined with explicit credentials"));
    }

    public void testAuthAnonymousConflictsWithAccessKey() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> S3Configuration.fromFields("ak", null, null, null, "anonymous")
        );
        assertThat(e.getMessage(), containsString("auth=anonymous cannot be combined with explicit credentials"));
    }

    public void testAuthAnonymousConflictsWithSecretKey() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> S3Configuration.fromFields(null, "sk", null, null, "anonymous")
        );
        assertThat(e.getMessage(), containsString("auth=anonymous cannot be combined with explicit credentials"));
    }

    public void testAuthAnonymousConflictsWithBothKeys() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> S3Configuration.fromFields("ak", "sk", null, null, "anonymous")
        );
        assertThat(e.getMessage(), containsString("auth=anonymous cannot be combined with explicit credentials"));
    }

    public void testAuthAnonymousAllowsEndpointAndRegion() {
        S3Configuration config = S3Configuration.fromFields(null, null, "http://localhost:9000", "eu-west-1", "anonymous");
        assertNotNull(config);
        assertTrue(config.isAnonymous());
        assertEquals("http://localhost:9000", config.endpoint());
        assertEquals("eu-west-1", config.region());
    }

    public void testFromFieldsAllNullReturnsNull() {
        assertNull(S3Configuration.fromFields(null, null, null, null, null));
    }

    public void testFromMapWithAuth() {
        S3Configuration config = S3Configuration.fromMap(Map.of("auth", "anonymous", "endpoint", "http://localhost:9000"));
        assertNotNull(config);
        assertTrue(config.isAnonymous());
        assertEquals("http://localhost:9000", config.endpoint());
    }

    public void testFromMapWithNullMapReturnsNull() {
        assertNull(S3Configuration.fromMap(null));
    }

    public void testFromMapWithEmptyMapReturnsNull() {
        assertNull(S3Configuration.fromMap(new HashMap<>()));
    }

    public void testFromMapRejectsUnknownKeys() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("access_key", "ak");
        raw.put("header_row", false);
        ValidationException e = expectThrows(ValidationException.class, () -> S3Configuration.fromMap(raw));
        assertThat(e.getMessage(), containsString("unknown setting [header_row]"));
    }

    public void testFromQueryConfigDropsUnknownKeys() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("access_key", "ak");
        raw.put("secret_key", "sk");
        raw.put("endpoint", "http://e");
        // Format-level options that the configuration map may carry; the storage plugin must ignore them.
        raw.put("header_row", false);
        raw.put("column_prefix", "f");

        Configured<S3Configuration> result = S3Configuration.fromQueryConfig(raw);
        S3Configuration config = result.value();
        assertNotNull(config);
        assertEquals("ak", config.accessKey());
        assertEquals("sk", config.secretKey());
        assertEquals("http://e", config.endpoint());
        assertNull(config.region());
        assertThat(result.consumedKeys(), containsInAnyOrder("access_key", "secret_key", "endpoint"));
    }

    public void testFromQueryConfigStillEnforcesAuthConflict() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("auth", "anonymous");
        raw.put("access_key", "ak");
        raw.put("header_row", false);
        ValidationException e = expectThrows(ValidationException.class, () -> S3Configuration.fromQueryConfig(raw));
        assertThat(e.getMessage(), containsString("auth=anonymous cannot be combined with explicit credentials"));
    }

    public void testFromQueryConfigWithOnlyUnknownKeysReturnsNull() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("header_row", false);
        raw.put("column_prefix", "f");
        Configured<S3Configuration> result = S3Configuration.fromQueryConfig(raw);
        assertNull(result.value());
        assertEquals(Set.of(), result.consumedKeys());
    }

    public void testFromQueryConfigWithNullReturnsNull() {
        Configured<S3Configuration> result = S3Configuration.fromQueryConfig(null);
        assertNull(result.value());
        assertEquals(Set.of(), result.consumedKeys());
    }

    public void testEqualsWithAuth() {
        S3Configuration config1 = S3Configuration.fromFields(null, null, "ep", null, "anonymous");
        S3Configuration config2 = S3Configuration.fromFields(null, null, "ep", null, "anonymous");
        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    public void testNotEqualsWithDifferentAuth() {
        // Two resolvable configs differing only in auth: anonymous vs managed_identity (neither needs a secret).
        S3Configuration config1 = S3Configuration.fromFields(null, null, "ep", null, "anonymous");
        S3Configuration config2 = S3Configuration.fromFields(null, null, "ep", null, "managed_identity");
        assertNotEquals(config1, config2);
    }

    public void testSessionToken() {
        S3Configuration config = S3Configuration.fromFields("ak", "sk", "tok", "http://endpoint", "us-west-2", null);
        assertNotNull(config);
        assertEquals("ak", config.accessKey());
        assertEquals("sk", config.secretKey());
        assertEquals("tok", config.sessionToken());
        assertTrue(config.hasCredentials());
        assertFalse(config.isAnonymous());
    }

    public void testSessionTokenFromMap() {
        S3Configuration config = S3Configuration.fromMap(Map.of("access_key", "ak", "secret_key", "sk", "session_token", "tok"));
        assertNotNull(config);
        assertEquals("tok", config.sessionToken());
        assertTrue(config.hasCredentials());
    }

    public void testSessionTokenAbsentByDefault() {
        S3Configuration config = S3Configuration.fromFields("ak", "sk", "http://endpoint", "us-west-2");
        assertNotNull(config);
        assertNull(config.sessionToken());
    }

    public void testSessionTokenConflictsWithAuthAnonymous() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> S3Configuration.fromFields(null, null, "tok", null, null, "anonymous")
        );
        assertThat(e.getMessage(), containsString("auth=anonymous cannot be combined with explicit credentials"));
    }

    public void testFromQueryConfigWithSessionToken() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("access_key", "ak");
        raw.put("secret_key", "sk");
        raw.put("session_token", "tok");
        raw.put("header_row", false);

        Configured<S3Configuration> result = S3Configuration.fromQueryConfig(raw);
        S3Configuration config = result.value();
        assertNotNull(config);
        assertEquals("tok", config.sessionToken());
        assertThat(result.consumedKeys(), containsInAnyOrder("access_key", "secret_key", "session_token"));
    }

    public void testEqualsWithSessionToken() {
        S3Configuration config1 = S3Configuration.fromFields("ak", "sk", "tok", "ep", null, null);
        S3Configuration config2 = S3Configuration.fromFields("ak", "sk", "tok", "ep", null, null);
        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    public void testNotEqualsWithDifferentSessionToken() {
        S3Configuration config1 = S3Configuration.fromFields("ak", "sk", "tok1", "ep", null, null);
        S3Configuration config2 = S3Configuration.fromFields("ak", "sk", "tok2", "ep", null, null);
        assertNotEquals(config1, config2);
    }

    public void testKeylessAuthAllFields() {
        S3Configuration config = S3Configuration.fromKeylessFields(
            "arn:aws:iam::123456789012:role/example",
            "my-session",
            "arn:aws:iam::123456789012:role/example",
            "https://sts.us-east-1.amazonaws.com",
            "us-west-2",
            "http://endpoint",
            "us-east-1"
        );
        assertNotNull(config);
        assertTrue(config.hasKeylessAuth());
        assertFalse(config.hasCredentials());
        assertFalse(config.isAnonymous());
        assertEquals("arn:aws:iam::123456789012:role/example", config.roleArn());
        assertEquals("my-session", config.roleSessionName());
        assertEquals("arn:aws:iam::123456789012:role/example", config.jwtAudience());
        assertEquals("https://sts.us-east-1.amazonaws.com", config.stsEndpoint());
        assertEquals("us-west-2", config.stsRegion());
        assertEquals("us-east-1", config.region());
    }

    public void testKeylessAuthMinimalFields() {
        S3Configuration config = S3Configuration.fromKeylessFields("role-arn", null, "audience", null, null, null, null);
        assertNotNull(config);
        assertTrue(config.hasKeylessAuth());
        assertEquals("role-arn", config.roleArn());
        assertEquals("audience", config.jwtAudience());
        assertNull(config.roleSessionName());
        assertNull(config.stsEndpoint());
        assertNull(config.stsRegion());
    }

    public void testKeylessAuthStsRegionDistinctFromBucketRegion() {
        S3Configuration config = S3Configuration.fromMap(
            Map.of("role_arn", "role-arn", "jwt_audience", "audience", "region", "eu-west-1", "sts_region", "us-east-1")
        );
        assertNotNull(config);
        assertTrue(config.hasKeylessAuth());
        assertEquals("eu-west-1", config.region());
        assertEquals("us-east-1", config.stsRegion());
    }

    public void testKeylessAuthRequiresRoleArn() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> S3Configuration.fromKeylessFields(null, null, "audience", null, null, null, null)
        );
        assertThat(e.getMessage(), containsString("role_arn is required when keyless authentication settings are configured"));
    }

    public void testKeylessAuthRequiresJwtAudience() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> S3Configuration.fromKeylessFields("role-arn", null, null, null, null, null, null)
        );
        assertThat(e.getMessage(), containsString("jwt_audience is required when keyless authentication settings are configured"));
    }

    public void testKeylessAuthConflictsWithCredentials() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> S3Configuration.fromMap(
                Map.of("access_key", "ak", "secret_key", "sk", "role_arn", "role-arn", "jwt_audience", "audience")
            )
        );
        assertThat(e.getMessage(), containsString("explicit credentials cannot be combined with keyless authentication settings"));
    }

    public void testKeylessAuthConflictsWithAuthAnonymous() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> S3Configuration.fromMap(Map.of("auth", "anonymous", "role_arn", "role-arn", "jwt_audience", "audience"))
        );
        assertThat(e.getMessage(), containsString("auth=anonymous cannot be combined with keyless authentication settings"));
    }

    public void testKeylessAuthFromMap() {
        S3Configuration config = S3Configuration.fromMap(Map.of("role_arn", "role-arn", "jwt_audience", "audience", "region", "eu-west-1"));
        assertNotNull(config);
        assertTrue(config.hasKeylessAuth());
        assertEquals("role-arn", config.roleArn());
        assertEquals("audience", config.jwtAudience());
        assertEquals("eu-west-1", config.region());
    }

    public void testKeylessAuthFromQueryConfigDropsUnknownKeys() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("role_arn", "role-arn");
        raw.put("jwt_audience", "audience");
        raw.put("header_row", false);
        Configured<S3Configuration> result = S3Configuration.fromQueryConfig(raw);
        S3Configuration config = result.value();
        assertNotNull(config);
        assertTrue(config.hasKeylessAuth());
        assertThat(result.consumedKeys(), containsInAnyOrder("role_arn", "jwt_audience"));
    }

    // --- Explicit auth modes ---

    public void testAuthAutoIsAcceptedAndDefersToFields() {
        S3Configuration config = S3Configuration.fromMap(Map.of("auth", "auto", "access_key", "ak", "secret_key", "sk"));
        assertEquals("auto", config.auth());
        assertFalse(config.isAnonymous());
        assertFalse(config.isManagedIdentity());
        assertFalse(config.isStaticCredentials());
        assertFalse(config.isFederatedIdentity());
        assertTrue(config.hasCredentials());
    }

    public void testAuthStaticCredentialsExplicit() {
        S3Configuration config = S3Configuration.fromMap(Map.of("auth", "static_credentials", "access_key", "ak", "secret_key", "sk"));
        assertTrue(config.isStaticCredentials());
        assertFalse(config.isAnonymous());
        assertEquals("static_credentials", config.auth());
    }

    public void testAuthStaticCredentialsRequiresCredentials() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> S3Configuration.fromMap(Map.of("auth", "static_credentials", "region", "us-east-1"))
        );
        assertThat(e.getMessage(), containsString("auth=static_credentials requires complete explicit credentials"));
    }

    public void testAuthStaticCredentialsRejectsPartialCredentials() {
        // A session token with no access/secret key is an incomplete static credential — explicit
        // static_credentials must reject it at validation time, not defer to the provider.
        var raw = new HashMap<String, Object>();
        raw.put("auth", "static_credentials");
        raw.put("session_token", "tok");
        ValidationException e = expectThrows(ValidationException.class, () -> S3Configuration.fromMap(raw));
        assertThat(e.getMessage(), containsString("auth=static_credentials requires complete explicit credentials"));
    }

    public void testAuthStaticCredentialsConflictsWithKeyless() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> S3Configuration.fromMap(
                Map.of("auth", "static_credentials", "access_key", "ak", "secret_key", "sk", "role_arn", "role", "jwt_audience", "aud")
            )
        );
        assertThat(e.getMessage(), containsString("auth=static_credentials cannot be combined with keyless authentication settings"));
    }

    public void testAuthFederatedIdentityExplicit() {
        S3Configuration config = S3Configuration.fromMap(Map.of("auth", "federated_identity", "role_arn", "role", "jwt_audience", "aud"));
        assertTrue(config.isFederatedIdentity());
        assertFalse(config.isManagedIdentity());
        assertTrue(config.hasKeylessAuth());
        assertEquals("federated_identity", config.auth());
    }

    public void testAuthFederatedIdentityRequiresKeyless() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> S3Configuration.fromMap(Map.of("auth", "federated_identity", "region", "us-east-1"))
        );
        assertThat(e.getMessage(), containsString("auth=federated_identity requires keyless authentication settings"));
    }

    public void testAuthFederatedIdentityConflictsWithCredentials() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> S3Configuration.fromMap(
                Map.of("auth", "federated_identity", "access_key", "ak", "secret_key", "sk", "role_arn", "role", "jwt_audience", "aud")
            )
        );
        assertThat(e.getMessage(), containsString("auth=federated_identity cannot be combined with explicit credentials"));
    }

    // --- Backwards compatibility: deprecated value names are accepted, canonicalized on parse, and warn on every use ---

    public void testDeprecatedAuthNoneCanonicalizedToAnonymous() {
        S3Configuration config = S3Configuration.fromMap(Map.of("auth", "none", "endpoint", "http://localhost:9000"));
        assertEquals("anonymous", config.auth());
        assertTrue(config.isAnonymous());
        // The stored representation (what GET returns) holds the canonical value, not the deprecated alias.
        assertEquals("anonymous", config.toMap().get("auth"));
        assertWarnings("auth value [none] is deprecated; the canonical value is [anonymous]");
    }

    public void testDeprecatedAuthNoneCaseInsensitiveCanonicalized() {
        S3Configuration config = S3Configuration.fromMap(Map.of("auth", "NONE"));
        assertEquals("anonymous", config.auth());
        assertTrue(config.isAnonymous());
        assertWarnings("auth value [none] is deprecated; the canonical value is [anonymous]");
    }

    public void testDeprecatedAuthWorkloadIdentityCanonicalizedToManagedIdentity() {
        S3Configuration config = S3Configuration.fromMap(Map.of("auth", "workload_identity", "region", "us-east-1"));
        assertEquals("managed_identity", config.auth());
        assertTrue(config.isManagedIdentity());
        assertEquals("managed_identity", config.toMap().get("auth"));
        assertWarnings("auth value [workload_identity] is deprecated; the canonical value is [managed_identity]");
    }
}
