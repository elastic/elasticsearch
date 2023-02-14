/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.test;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.user.User;
import org.hamcrest.Matcher;

import java.util.Locale;

import static org.apache.lucene.tests.util.LuceneTestCase.expectThrows;
import static org.elasticsearch.xpack.core.security.test.SecurityAssertions.assertContainsWWWAuthenticateHeader;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SecurityTestsUtils {

    private SecurityTestsUtils() {}

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
        assertThrowsAuthorizationException(null, throwingRunnable, action, user);
    }

    public static void assertThrowsAuthorizationException(
        String context,
        LuceneTestCase.ThrowingRunnable throwingRunnable,
        String action,
        String user
    ) {
        String message = "Expected authorization failure for user=[" + user + "], action=[" + action + "]";
        if (Strings.hasText(context)) {
            message += " - " + context;
        }
        assertThrowsAuthorizationException(
            message,
            throwingRunnable,
            containsString("[" + action + "] is unauthorized for user [" + user + "]")
        );
    }

    public static void assertThrowsAuthorizationExceptionRunAsDenied(
        LuceneTestCase.ThrowingRunnable throwingRunnable,
        String action,
        User authenticatingUser,
        String runAs
    ) {
        assertThrowsAuthorizationException(
            "Expected authorization failure for user=["
                + authenticatingUser.principal()
                + "], run-as=["
                + runAs
                + "], action=["
                + action
                + "]",
            throwingRunnable,
            containsString(
                "action ["
                    + action
                    + "] is unauthorized for user ["
                    + authenticatingUser.principal()
                    + "]"
                    + String.format(
                        Locale.ROOT,
                        " with effective roles [%s]",
                        Strings.arrayToCommaDelimitedString(authenticatingUser.roles())
                    )
                    + ", because user ["
                    + authenticatingUser.principal()
                    + "] is unauthorized to run as ["
                    + runAs
                    + "]"
            )
        );
    }

    public static void assertThrowsAuthorizationExceptionRunAsUnauthorizedAction(
        LuceneTestCase.ThrowingRunnable throwingRunnable,
        String action,
        String user,
        String runAs
    ) {
        assertThrowsAuthorizationException(
            "Expected authorization failure for user=[" + user + "], run-as=[" + runAs + "], action=[" + action + "]",
            throwingRunnable,
            containsString("[" + action + "] is unauthorized for user [" + user + "] run as [" + runAs + "]")
        );
    }

    public static void assertThrowsAuthorizationExceptionDefaultUsers(LuceneTestCase.ThrowingRunnable throwingRunnable, String action) {
        ElasticsearchSecurityException exception = expectThrows(ElasticsearchSecurityException.class, throwingRunnable);
        assertAuthorizationExceptionDefaultUsers(exception, action);
    }

    public static void assertAuthorizationExceptionDefaultUsers(Throwable throwable, String action) {
        assertAuthorizationException(
            throwable,
            either(containsString("[" + action + "] is unauthorized for user [" + SecuritySettingsSource.TEST_USER_NAME + "]")).or(
                containsString(
                    "[" + action + "] is unauthorized for user [" + SecuritySettingsSource.DEFAULT_TRANSPORT_CLIENT_USER_NAME + "]"
                )
            )
        );
    }

    public static void assertThrowsAuthorizationException(
        String failureMessageIfNoException,
        LuceneTestCase.ThrowingRunnable throwingRunnable,
        Matcher<String> messageMatcher
    ) {
        ElasticsearchSecurityException securityException = expectThrows(
            ElasticsearchSecurityException.class,
            failureMessageIfNoException,
            throwingRunnable
        );
        assertAuthorizationException(securityException, messageMatcher);
    }

    private static void assertAuthorizationException(Throwable throwable, Matcher<String> messageMatcher) {
        assertThat(throwable, instanceOf(ElasticsearchSecurityException.class));
        ElasticsearchSecurityException securityException = (ElasticsearchSecurityException) throwable;
        assertThat(securityException.status(), is(RestStatus.FORBIDDEN));
        assertThat(throwable.getMessage(), messageMatcher);
    }
}
