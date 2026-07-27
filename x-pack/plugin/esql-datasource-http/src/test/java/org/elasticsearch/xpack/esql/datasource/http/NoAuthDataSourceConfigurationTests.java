/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class NoAuthDataSourceConfigurationTests extends ESTestCase {

    public void testNullReturnsNull() {
        assertNull(NoAuthDataSourceConfiguration.fromMap(null));
    }

    public void testEmptyReturnsNull() {
        assertNull(NoAuthDataSourceConfiguration.fromMap(Map.of()));
    }

    public void testAuthAnonymousAccepted() {
        NoAuthDataSourceConfiguration config = NoAuthDataSourceConfiguration.fromMap(Map.of("auth", "anonymous"));
        assertNotNull(config);
        assertEquals("anonymous", config.get("auth"));
    }

    public void testAuthAnonymousIsCaseInsensitive() {
        NoAuthDataSourceConfiguration config = NoAuthDataSourceConfiguration.fromMap(Map.of("auth", "ANONYMOUS"));
        assertNotNull(config);
        assertEquals("anonymous", config.get("auth"));
    }

    public void testDeprecatedAuthNoneCanonicalizedToAnonymous() {
        // Symmetric with the file-based sources: the former value name is accepted, canonicalized, and warns.
        NoAuthDataSourceConfiguration config = NoAuthDataSourceConfiguration.fromMap(Map.of("auth", "none"));
        assertNotNull(config);
        assertEquals("anonymous", config.get("auth"));
        assertWarnings("auth value [none] is deprecated; the canonical value is [anonymous]");
    }

    public void testUnsupportedAuthValueRejected() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> NoAuthDataSourceConfiguration.fromMap(Map.of("auth", "basic"))
        );
        assertThat(e.getMessage(), containsString("Unsupported auth value [basic]"));
    }

    public void testUnknownSettingIsRejected() {
        // Only auth is declared, so any other datasource-level setting is an unknown field.
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> NoAuthDataSourceConfiguration.fromMap(Map.of("region", "us-east-1"))
        );
        assertThat(e.getMessage(), containsString("region"));
    }

    public void testMultipleUnknownSettingsAccumulateErrors() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("region", "us-east-1");
        raw.put("access_key", "AKIA123");
        ValidationException e = expectThrows(ValidationException.class, () -> NoAuthDataSourceConfiguration.fromMap(raw));
        assertEquals(2, e.validationErrors().size());
    }
}
