/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.function.Function;

/**
 * Unit tests for {@link Federation}, the federation (external data sources) kill switch. The live state is a
 * {@code static final} read once at class load and cannot be flipped in-JVM, so the property parsing and the
 * enforcement branch are exercised through the package-private {@link Federation#readEnabled} /
 * {@link Federation#ensureEnabled(boolean)} seams, which take their input as a parameter.
 */
public class FederationTests extends ESTestCase {

    private static Function<String, String> property(String value) {
        return Map.of(Federation.ENABLED_PROPERTY, value)::get;
    }

    public void testEnabledByDefaultWhenPropertyAbsent() {
        assertTrue(Federation.readEnabled(key -> null));
    }

    public void testEnabledByDefaultWhenBlank() {
        assertTrue(Federation.readEnabled(property("   ")));
    }

    public void testEnabledWhenTrue() {
        assertTrue(Federation.readEnabled(property("true")));
    }

    public void testDisabledWhenFalse() {
        assertFalse(Federation.readEnabled(property("false")));
    }

    public void testInvalidValueFailsFast() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> Federation.readEnabled(property("maybe")));
        assertTrue(e.getMessage().contains(Federation.ENABLED_PROPERTY));
    }

    public void testEnsureEnabledIsNoopWhenEnabled() {
        Federation.ensureEnabled(true); // must not throw
    }

    public void testEnsureEnabledThrowsForbiddenWhenDisabled() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> Federation.ensureEnabled(false));
        assertEquals(RestStatus.FORBIDDEN, e.status());
        assertTrue(e.getMessage().contains(Federation.ENABLED_PROPERTY));
    }

    public void testDisabledExceptionIsForbidden() {
        assertEquals(RestStatus.FORBIDDEN, Federation.disabledException().status());
    }
}
