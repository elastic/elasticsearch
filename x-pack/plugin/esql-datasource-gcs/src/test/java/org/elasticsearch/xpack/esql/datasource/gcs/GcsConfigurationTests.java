/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

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

    public void testFromMapWithProjectIdOnly() {
        GcsConfiguration config = GcsConfiguration.fromMap(Map.of("project_id", "my-project"));

        assertNotNull(config);
        assertNull(config.serviceAccountCredentials());
        assertEquals("my-project", config.projectId());
        assertNull(config.endpoint());
        assertFalse(config.hasCredentials());
    }

    public void testFromMapWithEndpointOnly() {
        GcsConfiguration config = GcsConfiguration.fromMap(Map.of("endpoint", "http://localhost:4443"));

        assertNotNull(config);
        assertNull(config.serviceAccountCredentials());
        assertNull(config.projectId());
        assertEquals("http://localhost:4443", config.endpoint());
        assertFalse(config.hasCredentials());
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

    public void testFromFieldsWithNullCredentials() {
        GcsConfiguration config = GcsConfiguration.fromFields(null, "my-project", "http://localhost:4443");

        assertNotNull(config);
        assertNull(config.serviceAccountCredentials());
        assertEquals("my-project", config.projectId());
        assertFalse(config.hasCredentials());
    }

    public void testFromFieldsWithAllNullReturnsNull() {
        assertNull(GcsConfiguration.fromFields(null, null, null));
    }

    public void testHasCredentialsWithCredentials() {
        GcsConfiguration config = GcsConfiguration.fromFields("{\"type\":\"service_account\"}", null, null);
        assertTrue(config.hasCredentials());
    }

    public void testHasCredentialsWithoutCredentials() {
        GcsConfiguration config = GcsConfiguration.fromFields(null, "my-project", null);
        assertFalse(config.hasCredentials());
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
        GcsConfiguration config1 = GcsConfiguration.fromFields(null, null, "endpoint");
        GcsConfiguration config2 = GcsConfiguration.fromFields(null, null, "endpoint");

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    public void testAuthNone() {
        GcsConfiguration config = GcsConfiguration.fromFields(null, null, "http://endpoint", null, "none");
        assertNotNull(config);
        assertTrue(config.isAnonymous());
        assertFalse(config.hasCredentials());
    }

    public void testAuthNoneCaseInsensitive() {
        GcsConfiguration config = GcsConfiguration.fromFields(null, null, "http://endpoint", null, "NONE");
        assertTrue(config.isAnonymous());
        assertEquals("none", config.auth());
    }

    public void testAuthNoneConflictsWithCredentials() {
        expectThrows(
            ValidationException.class,
            () -> GcsConfiguration.fromFields("{\"type\":\"service_account\"}", null, null, null, "none")
        );
    }

    public void testAuthNoneAllowsProjectIdAndEndpoint() {
        GcsConfiguration config = GcsConfiguration.fromFields(null, "my-project", "http://ep", null, "none");
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

        GcsConfiguration config = GcsConfiguration.fromQueryConfig(raw);
        assertNotNull(config);
        assertEquals("{\"type\":\"service_account\"}", config.serviceAccountCredentials());
        assertEquals("my-project", config.projectId());
        assertEquals("http://localhost:4443", config.endpoint());
    }

    public void testFromQueryConfigStillEnforcesAuthConflict() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("auth", "none");
        raw.put("credentials", "{\"type\":\"service_account\"}");
        raw.put("header_row", false);
        ValidationException e = expectThrows(ValidationException.class, () -> GcsConfiguration.fromQueryConfig(raw));
        assertThat(e.getMessage(), containsString("auth=none cannot be combined with explicit credentials"));
    }

    public void testFromQueryConfigWithOnlyUnknownKeysReturnsNull() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("header_row", false);
        raw.put("column_prefix", "f");
        assertNull(GcsConfiguration.fromQueryConfig(raw));
    }

    public void testFromQueryConfigWithNullReturnsNull() {
        assertNull(GcsConfiguration.fromQueryConfig(null));
    }
}
