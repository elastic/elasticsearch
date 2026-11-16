/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.iceberg;

import org.elasticsearch.test.ESTestCase;

/**
 * Unit tests for S3Configuration.
 * Tests building S3 credentials and configuration from individual fields.
 */
public class S3ConfigurationTests extends ESTestCase {

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
}
