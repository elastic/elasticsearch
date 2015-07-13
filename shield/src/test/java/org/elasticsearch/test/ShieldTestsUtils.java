/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.rest.RestStatus;
import org.hamcrest.Matcher;

import static org.elasticsearch.shield.test.ShieldAssertions.assertContainsWWWAuthenticateHeader;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 *
 */
public class ShieldTestsUtils {

    private ShieldTestsUtils() {
    }

    public static void assertAuthenticationException(ElasticsearchSecurityException e) {
        assertThat(e.status(), is(RestStatus.UNAUTHORIZED));
        // making sure it's not a license expired exception
        assertThat(e.getHeader("es.license.expired.feature"), nullValue());
        assertContainsWWWAuthenticateHeader(e);
    }

    public static void assertAuthenticationException(ElasticsearchSecurityException e, Matcher<String> messageMatcher) {
        assertAuthenticationException(e);
        assertThat(e.getMessage(), messageMatcher);
    }

    public static void assertAuthorizationException(ElasticsearchSecurityException e) {
        assertThat(e.status(), is(RestStatus.FORBIDDEN));
    }

    public static void assertAuthorizationException(ElasticsearchSecurityException e, Matcher<String> messageMatcher) {
        assertAuthorizationException(e);
        assertThat(e.getMessage(), messageMatcher);
    }

}
