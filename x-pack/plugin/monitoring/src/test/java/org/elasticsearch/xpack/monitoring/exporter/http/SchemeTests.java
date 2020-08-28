/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.elasticsearch.test.ESTestCase;

import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests {@link Scheme}.
 */
public class SchemeTests extends ESTestCase {

    public void testToString() {
        for (final Scheme scheme : Scheme.values()) {
            assertThat(scheme.toString(), equalTo(scheme.name().toLowerCase(Locale.ROOT)));
        }
    }

    public void testFromString() {
        for (final Scheme scheme : Scheme.values()) {
            assertThat(Scheme.fromString(scheme.name()), sameInstance(scheme));
            assertThat(Scheme.fromString(scheme.name().toLowerCase(Locale.ROOT)), sameInstance(scheme));
        }
    }

    public void testFromStringMalformed() {
        assertIllegalScheme("htp");
        assertIllegalScheme("htttp");
        assertIllegalScheme("httpd");
        assertIllegalScheme("ftp");
        assertIllegalScheme("ws");
        assertIllegalScheme("wss");
        assertIllegalScheme("gopher");
    }

    private void assertIllegalScheme(final String scheme) {
        try {
            Scheme.fromString(scheme);
            fail("scheme should be unknown: [" + scheme + "]");
        } catch (final IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("[" + scheme + "]"));
        }
    }

}
