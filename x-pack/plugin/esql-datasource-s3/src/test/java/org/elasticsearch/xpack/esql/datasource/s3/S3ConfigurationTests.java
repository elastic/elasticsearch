/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.HashMap;
import java.util.Map;

public class S3ConfigurationTests extends ESTestCase {

    private static final Source SOURCE = Source.EMPTY;

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
        // auth is normalized to lowercase
        assertEquals("none", config.auth());
    }

    public void testUnsupportedAuthValueThrows() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> S3Configuration.fromFields(null, null, "http://e", null, "unsupported")
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("Unsupported auth value"));
    }

    public void testAuthNoneConflictsWithAccessKey() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> S3Configuration.fromFields("ak", null, null, null, "none")
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("auth=none cannot be combined with access_key/secret_key"));
    }

    public void testAuthNoneConflictsWithSecretKey() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> S3Configuration.fromFields(null, "sk", null, null, "none")
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("auth=none cannot be combined with access_key/secret_key"));
    }

    public void testAuthNoneConflictsWithBothKeys() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> S3Configuration.fromFields("ak", "sk", null, null, "none")
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("auth=none cannot be combined with access_key/secret_key"));
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

    public void testFromParamsWithAuth() {
        Map<String, Expression> params = new HashMap<>();
        params.put("auth", literal("none"));
        params.put("endpoint", literal("http://localhost:9000"));

        S3Configuration config = S3Configuration.fromParams(params);
        assertNotNull(config);
        assertTrue(config.isAnonymous());
        assertEquals("http://localhost:9000", config.endpoint());
    }

    public void testFromParamsWithNullMapReturnsNull() {
        assertNull(S3Configuration.fromParams(null));
    }

    public void testFromParamsWithEmptyMapReturnsNull() {
        assertNull(S3Configuration.fromParams(new HashMap<>()));
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

    private Literal literal(Object value) {
        Object literalValue = value instanceof String s ? new BytesRef(s) : value;
        DataType dataType = value instanceof String ? DataType.KEYWORD : DataType.KEYWORD;
        return new Literal(SOURCE, literalValue, dataType);
    }
}
