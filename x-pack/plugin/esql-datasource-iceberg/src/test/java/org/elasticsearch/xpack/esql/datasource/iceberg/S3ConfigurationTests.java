/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.iceberg;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for S3Configuration.
 * Tests parsing S3 credentials and configuration from query parameters.
 */
public class S3ConfigurationTests extends ESTestCase {

    private static final Source SOURCE = Source.EMPTY;

    public void testFromParamsWithAllFields() {
        Map<String, Expression> params = new HashMap<>();
        params.put("access_key", literal("AKIAIOSFODNN7EXAMPLE"));
        params.put("secret_key", literal("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"));
        params.put("endpoint", literal("http://localhost:9000"));
        params.put("region", literal("us-east-1"));

        S3Configuration config = S3Configuration.fromParams(params);

        assertNotNull(config);
        assertEquals("AKIAIOSFODNN7EXAMPLE", config.accessKey());
        assertEquals("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", config.secretKey());
        assertEquals("http://localhost:9000", config.endpoint());
        assertEquals("us-east-1", config.region());
        assertTrue(config.hasCredentials());
    }

    public void testFromParamsWithCredentialsOnly() {
        Map<String, Expression> params = new HashMap<>();
        params.put("access_key", literal("AKIAIOSFODNN7EXAMPLE"));
        params.put("secret_key", literal("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"));

        S3Configuration config = S3Configuration.fromParams(params);

        assertNotNull(config);
        assertEquals("AKIAIOSFODNN7EXAMPLE", config.accessKey());
        assertEquals("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", config.secretKey());
        assertNull(config.endpoint());
        assertNull(config.region());
        assertTrue(config.hasCredentials());
    }

    public void testFromParamsWithEndpointOnly() {
        Map<String, Expression> params = new HashMap<>();
        params.put("endpoint", literal("http://localhost:9000"));

        S3Configuration config = S3Configuration.fromParams(params);

        assertNotNull(config);
        assertNull(config.accessKey());
        assertNull(config.secretKey());
        assertEquals("http://localhost:9000", config.endpoint());
        assertNull(config.region());
        assertFalse(config.hasCredentials()); // No access/secret keys
    }

    public void testFromParamsWithRegionOnly() {
        Map<String, Expression> params = new HashMap<>();
        params.put("region", literal("eu-west-1"));

        S3Configuration config = S3Configuration.fromParams(params);

        assertNotNull(config);
        assertNull(config.accessKey());
        assertNull(config.secretKey());
        assertNull(config.endpoint());
        assertEquals("eu-west-1", config.region());
        assertFalse(config.hasCredentials());
    }

    public void testFromParamsWithNullMapReturnsNull() {
        S3Configuration config = S3Configuration.fromParams(null);
        assertNull(config);
    }

    public void testFromParamsWithEmptyMapReturnsNull() {
        S3Configuration config = S3Configuration.fromParams(new HashMap<>());
        assertNull(config);
    }

    public void testFromParamsWithNoS3ParamsReturnsNull() {
        Map<String, Expression> params = new HashMap<>();
        params.put("other_param", literal("value"));
        params.put("another_param", literal(123));

        S3Configuration config = S3Configuration.fromParams(params);

        // No S3 params present, should return null
        assertNull(config);
    }

    public void testFromParamsWithBytesRefValue() {
        Map<String, Expression> params = new HashMap<>();
        params.put("access_key", new Literal(SOURCE, new BytesRef("AKIAIOSFODNN7EXAMPLE"), DataType.KEYWORD));
        params.put("secret_key", new Literal(SOURCE, new BytesRef("secret"), DataType.KEYWORD));

        S3Configuration config = S3Configuration.fromParams(params);

        assertNotNull(config);
        assertEquals("AKIAIOSFODNN7EXAMPLE", config.accessKey());
        assertEquals("secret", config.secretKey());
    }

    public void testFromParamsWithPartialCredentials() {
        Map<String, Expression> params = new HashMap<>();
        params.put("access_key", literal("AKIAIOSFODNN7EXAMPLE"));
        // No secret_key

        S3Configuration config = S3Configuration.fromParams(params);

        assertNotNull(config);
        assertEquals("AKIAIOSFODNN7EXAMPLE", config.accessKey());
        assertNull(config.secretKey());
        assertFalse(config.hasCredentials()); // Missing secret key
    }

    public void testFromFieldsWithAllFields() {
        S3Configuration config = S3Configuration.fromFields(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "http://localhost:9000",
            "us-east-1"
        );

        assertNotNull(config);
        assertEquals("AKIAIOSFODNN7EXAMPLE", config.accessKey());
        assertEquals("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", config.secretKey());
        assertEquals("http://localhost:9000", config.endpoint());
        assertEquals("us-east-1", config.region());
        assertTrue(config.hasCredentials());
    }

    public void testFromFieldsWithNullAccessKey() {
        S3Configuration config = S3Configuration.fromFields(null, "secret", "http://localhost:9000", "us-east-1");

        assertNotNull(config);
        assertNull(config.accessKey());
        assertEquals("secret", config.secretKey());
        assertFalse(config.hasCredentials()); // Missing access key
    }

    public void testFromFieldsWithNullSecretKey() {
        S3Configuration config = S3Configuration.fromFields("AKIAIOSFODNN7EXAMPLE", null, "http://localhost:9000", "us-east-1");

        assertNotNull(config);
        assertEquals("AKIAIOSFODNN7EXAMPLE", config.accessKey());
        assertNull(config.secretKey());
        assertFalse(config.hasCredentials()); // Missing secret key
    }

    public void testFromFieldsWithAllNullReturnsNull() {
        S3Configuration config = S3Configuration.fromFields(null, null, null, null);
        assertNull(config);
    }

    public void testHasCredentialsWithBothKeys() {
        S3Configuration config = S3Configuration.fromFields("access", "secret", null, null);

        assertTrue(config.hasCredentials());
    }

    public void testHasCredentialsWithAccessKeyOnly() {
        S3Configuration config = S3Configuration.fromFields("access", null, "endpoint", null);

        assertFalse(config.hasCredentials());
    }

    public void testHasCredentialsWithSecretKeyOnly() {
        S3Configuration config = S3Configuration.fromFields(null, "secret", "endpoint", null);

        assertFalse(config.hasCredentials());
    }

    public void testEqualsAndHashCodeSameValues() {
        S3Configuration config1 = S3Configuration.fromFields("access", "secret", "endpoint", "region");
        S3Configuration config2 = S3Configuration.fromFields("access", "secret", "endpoint", "region");

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    public void testEqualsAndHashCodeDifferentAccessKey() {
        S3Configuration config1 = S3Configuration.fromFields("access1", "secret", "endpoint", "region");
        S3Configuration config2 = S3Configuration.fromFields("access2", "secret", "endpoint", "region");

        assertNotEquals(config1, config2);
    }

    public void testEqualsAndHashCodeDifferentSecretKey() {
        S3Configuration config1 = S3Configuration.fromFields("access", "secret1", "endpoint", "region");
        S3Configuration config2 = S3Configuration.fromFields("access", "secret2", "endpoint", "region");

        assertNotEquals(config1, config2);
    }

    public void testEqualsAndHashCodeDifferentEndpoint() {
        S3Configuration config1 = S3Configuration.fromFields("access", "secret", "endpoint1", "region");
        S3Configuration config2 = S3Configuration.fromFields("access", "secret", "endpoint2", "region");

        assertNotEquals(config1, config2);
    }

    public void testEqualsAndHashCodeDifferentRegion() {
        S3Configuration config1 = S3Configuration.fromFields("access", "secret", "endpoint", "region1");
        S3Configuration config2 = S3Configuration.fromFields("access", "secret", "endpoint", "region2");

        assertNotEquals(config1, config2);
    }

    public void testEqualsWithNull() {
        S3Configuration config = S3Configuration.fromFields("access", "secret", "endpoint", "region");

        assertNotEquals(null, config);
    }

    public void testEqualsWithDifferentClass() {
        S3Configuration config = S3Configuration.fromFields("access", "secret", "endpoint", "region");

        assertNotEquals("not a config", config);
    }

    public void testEqualsSameInstance() {
        S3Configuration config = S3Configuration.fromFields("access", "secret", "endpoint", "region");

        assertEquals(config, config);
    }

    public void testEqualsWithNullFields() {
        S3Configuration config1 = S3Configuration.fromFields(null, null, "endpoint", null);
        S3Configuration config2 = S3Configuration.fromFields(null, null, "endpoint", null);

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
