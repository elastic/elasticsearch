/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for GcsConfiguration.
 * Tests parsing GCS credentials and configuration from query parameters.
 */
public class GcsConfigurationTests extends ESTestCase {

    private static final Source SOURCE = Source.EMPTY;

    public void testFromParamsWithAllFields() {
        Map<String, Expression> params = new HashMap<>();
        params.put("credentials", literal("{\"type\":\"service_account\",\"project_id\":\"test\"}"));
        params.put("project_id", literal("my-project"));
        params.put("endpoint", literal("http://localhost:4443"));

        GcsConfiguration config = GcsConfiguration.fromParams(params);

        assertNotNull(config);
        assertEquals("{\"type\":\"service_account\",\"project_id\":\"test\"}", config.serviceAccountCredentials());
        assertEquals("my-project", config.projectId());
        assertEquals("http://localhost:4443", config.endpoint());
        assertTrue(config.hasCredentials());
    }

    public void testFromParamsWithCredentialsOnly() {
        Map<String, Expression> params = new HashMap<>();
        params.put("credentials", literal("{\"type\":\"service_account\"}"));

        GcsConfiguration config = GcsConfiguration.fromParams(params);

        assertNotNull(config);
        assertEquals("{\"type\":\"service_account\"}", config.serviceAccountCredentials());
        assertNull(config.projectId());
        assertNull(config.endpoint());
        assertTrue(config.hasCredentials());
    }

    public void testFromParamsWithProjectIdOnly() {
        Map<String, Expression> params = new HashMap<>();
        params.put("project_id", literal("my-project"));

        GcsConfiguration config = GcsConfiguration.fromParams(params);

        assertNotNull(config);
        assertNull(config.serviceAccountCredentials());
        assertEquals("my-project", config.projectId());
        assertNull(config.endpoint());
        assertFalse(config.hasCredentials());
    }

    public void testFromParamsWithEndpointOnly() {
        Map<String, Expression> params = new HashMap<>();
        params.put("endpoint", literal("http://localhost:4443"));

        GcsConfiguration config = GcsConfiguration.fromParams(params);

        assertNotNull(config);
        assertNull(config.serviceAccountCredentials());
        assertNull(config.projectId());
        assertEquals("http://localhost:4443", config.endpoint());
        assertFalse(config.hasCredentials());
    }

    public void testFromParamsWithNullMapReturnsNull() {
        GcsConfiguration config = GcsConfiguration.fromParams(null);
        assertNull(config);
    }

    public void testFromParamsWithEmptyMapReturnsNull() {
        GcsConfiguration config = GcsConfiguration.fromParams(new HashMap<>());
        assertNull(config);
    }

    public void testFromParamsWithNoGcsParamsReturnsNull() {
        Map<String, Expression> params = new HashMap<>();
        params.put("other_param", literal("value"));
        params.put("another_param", literal(123));

        GcsConfiguration config = GcsConfiguration.fromParams(params);

        assertNull(config);
    }

    public void testFromParamsWithBytesRefValue() {
        Map<String, Expression> params = new HashMap<>();
        params.put("credentials", new Literal(SOURCE, new BytesRef("{\"type\":\"service_account\"}"), DataType.KEYWORD));
        params.put("project_id", new Literal(SOURCE, new BytesRef("my-project"), DataType.KEYWORD));

        GcsConfiguration config = GcsConfiguration.fromParams(params);

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
        GcsConfiguration config = GcsConfiguration.fromFields(null, null, null);
        assertNull(config);
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

    private Literal literal(Object value) {
        DataType dataType;
        Object literalValue = value;
        if (value instanceof String s) {
            dataType = DataType.KEYWORD;
            literalValue = new BytesRef(s);
        } else if (value instanceof Integer) {
            dataType = DataType.INTEGER;
        } else if (value instanceof Long) {
            dataType = DataType.LONG;
        } else if (value instanceof Double) {
            dataType = DataType.DOUBLE;
        } else if (value instanceof Boolean) {
            dataType = DataType.BOOLEAN;
        } else {
            dataType = DataType.KEYWORD;
        }
        return new Literal(SOURCE, literalValue, dataType);
    }
}
