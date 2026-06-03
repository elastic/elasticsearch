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

    public void testAuthNone() {
        S3Configuration config = S3Configuration.fromFields(null, null, "http://endpoint", "us-east-1", "none");
        assertNotNull(config);
        assertTrue(config.isAnonymous());
        assertFalse(config.hasCredentials());
    }

    public void testAuthNoneCaseInsensitive() {
        S3Configuration config = S3Configuration.fromFields(null, null, "http://e", null, "NONE");
        assertNotNull(config);
        assertTrue(config.isAnonymous());
        // case-insensitive fields normalized to lowercase
        assertEquals("none", config.auth());
    }

    public void testUnsupportedAuthValueThrows() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> S3Configuration.fromFields(null, null, "http://e", null, "unsupported")
        );
        assertThat(e.getMessage(), containsString("Unsupported auth value"));
    }

    public void testAuthAmbient() {
        S3Configuration config = S3Configuration.fromFields(null, null, null, "us-east-1", "ambient");
        assertNotNull(config);
        assertTrue(config.isAmbient());
        assertFalse(config.isAnonymous());
        assertFalse(config.hasCredentials());
    }

    public void testAuthAmbientCaseInsensitive() {
        S3Configuration config = S3Configuration.fromFields(null, null, null, null, "AMBIENT");
        assertNotNull(config);
        assertTrue(config.isAmbient());
        assertEquals("ambient", config.auth());
    }

    public void testAuthAmbientAllowsRegionAndEndpoint() {
        S3Configuration config = S3Configuration.fromFields(null, null, "http://localhost:9000", "eu-west-1", "ambient");
        assertNotNull(config);
        assertTrue(config.isAmbient());
        assertEquals("http://localhost:9000", config.endpoint());
        assertEquals("eu-west-1", config.region());
    }

    public void testAuthAmbientConflictsWithAccessKey() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> S3Configuration.fromFields("ak", null, null, null, "ambient")
        );
        assertThat(e.getMessage(), containsString("auth=ambient cannot be combined with explicit credentials"));
    }

    public void testAuthAmbientConflictsWithSecretKey() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> S3Configuration.fromFields(null, "sk", null, null, "ambient")
        );
        assertThat(e.getMessage(), containsString("auth=ambient cannot be combined with explicit credentials"));
    }

    public void testAuthAmbientConflictsWithSessionToken() {
        var raw = new java.util.HashMap<String, Object>();
        raw.put("auth", "ambient");
        raw.put("session_token", "tok");
        ValidationException e = expectThrows(ValidationException.class, () -> S3Configuration.fromMap(raw));
        assertThat(e.getMessage(), containsString("auth=ambient cannot be combined with explicit credentials"));
    }

    public void testAuthNoneConflictsWithAccessKey() {
        ValidationException e = expectThrows(ValidationException.class, () -> S3Configuration.fromFields("ak", null, null, null, "none"));
        assertThat(e.getMessage(), containsString("auth=none cannot be combined with explicit credentials"));
    }

    public void testAuthNoneConflictsWithSecretKey() {
        ValidationException e = expectThrows(ValidationException.class, () -> S3Configuration.fromFields(null, "sk", null, null, "none"));
        assertThat(e.getMessage(), containsString("auth=none cannot be combined with explicit credentials"));
    }

    public void testAuthNoneConflictsWithBothKeys() {
        ValidationException e = expectThrows(ValidationException.class, () -> S3Configuration.fromFields("ak", "sk", null, null, "none"));
        assertThat(e.getMessage(), containsString("auth=none cannot be combined with explicit credentials"));
    }

    public void testAuthNoneAllowsEndpointAndRegion() {
        S3Configuration config = S3Configuration.fromFields(null, null, "http://localhost:9000", "eu-west-1", "none");
        assertNotNull(config);
        assertTrue(config.isAnonymous());
        assertEquals("http://localhost:9000", config.endpoint());
        assertEquals("eu-west-1", config.region());
    }

    public void testFromFieldsAllNullReturnsNull() {
        assertNull(S3Configuration.fromFields(null, null, null, null, null));
    }

    public void testFromMapWithAuth() {
        S3Configuration config = S3Configuration.fromMap(Map.of("auth", "none", "endpoint", "http://localhost:9000"));
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
        raw.put("auth", "none");
        raw.put("access_key", "ak");
        raw.put("header_row", false);
        ValidationException e = expectThrows(ValidationException.class, () -> S3Configuration.fromQueryConfig(raw));
        assertThat(e.getMessage(), containsString("auth=none cannot be combined with explicit credentials"));
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
        S3Configuration config1 = S3Configuration.fromFields(null, null, "ep", null, "none");
        S3Configuration config2 = S3Configuration.fromFields(null, null, "ep", null, "none");
        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    public void testNotEqualsWithDifferentAuth() {
        S3Configuration config1 = S3Configuration.fromFields(null, null, "ep", null, "none");
        S3Configuration config2 = S3Configuration.fromFields(null, null, "ep", null, null);
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

    public void testSessionTokenConflictsWithAuthNone() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> S3Configuration.fromFields(null, null, "tok", null, null, "none")
        );
        assertThat(e.getMessage(), containsString("auth=none cannot be combined with explicit credentials"));
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
}
