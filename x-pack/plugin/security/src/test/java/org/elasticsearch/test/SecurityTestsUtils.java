/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.rest.RestStatus;
import org.hamcrest.Matcher;

import static org.apache.lucene.util.LuceneTestCase.expectThrows;
import static org.elasticsearch.xpack.core.security.test.SecurityAssertions.assertContainsWWWAuthenticateHeader;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SecurityTestsUtils {

    private SecurityTestsUtils() {
    }

    public static void assertAuthenticationException(ElasticsearchSecurityException e) {
        assertThat(e.status(), is(RestStatus.UNAUTHORIZED));
        // making sure it's not a license expired exception
        assertThat(e.getMetadata(LicenseUtils.EXPIRED_FEATURE_METADATA), nullValue());
        assertContainsWWWAuthenticateHeader(e);
    }

    public static void assertAuthenticationException(ElasticsearchSecurityException e, Matcher<String> messageMatcher) {
        assertAuthenticationException(e);
        assertThat(e.getMessage(), messageMatcher);
    }

    public static void assertThrowsAuthorizationException(LuceneTestCase.ThrowingRunnable throwingRunnable, String action, String user) {
        assertThrowsAuthorizationException(throwingRunnable,
                containsString("[" + action + "] is unauthorized for user [" + user + "]"));
    }

    public static void assertThrowsAuthorizationExceptionRunAs(LuceneTestCase.ThrowingRunnable throwingRunnable,
                                                               String action, String user, String runAs) {
        assertThrowsAuthorizationException(throwingRunnable,
                containsString("[" + action + "] is unauthorized for user [" + user + "] run as [" + runAs + "]"));
    }

    public static void assertThrowsAuthorizationExceptionDefaultUsers(LuceneTestCase.ThrowingRunnable throwingRunnable, String action) {
        ElasticsearchSecurityException exception = expectThrows(ElasticsearchSecurityException.class, throwingRunnable);
        assertAuthorizationExceptionDefaultUsers(exception, action);
    }

    public static void assertAuthorizationExceptionDefaultUsers(Throwable throwable, String action) {
        assertAuthorizationException(throwable, either(containsString("[" + action + "] is unauthorized for user ["
                + SecuritySettingsSource.TEST_USER_NAME + "]")).or(containsString("[" + action + "] is unauthorized for user ["
                + SecuritySettingsSource.DEFAULT_TRANSPORT_CLIENT_USER_NAME + "]")));
    }

    public static void assertThrowsAuthorizationException(LuceneTestCase.ThrowingRunnable throwingRunnable,
                                                           Matcher<String> messageMatcher) {
        ElasticsearchSecurityException securityException = expectThrows(ElasticsearchSecurityException.class, throwingRunnable);
        assertAuthorizationException(securityException, messageMatcher);
    }

    private static void assertAuthorizationException(Throwable throwable, Matcher<String> messageMatcher) {
        assertThat(throwable, instanceOf(ElasticsearchSecurityException.class));
        ElasticsearchSecurityException securityException = (ElasticsearchSecurityException) throwable;
        assertThat(securityException.status(), is(RestStatus.FORBIDDEN));
        assertThat(throwable.getMessage(), messageMatcher);
    }
}
