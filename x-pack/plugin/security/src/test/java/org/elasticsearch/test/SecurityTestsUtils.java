/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.hamcrest.Matcher;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static org.apache.lucene.util.LuceneTestCase.expectThrows;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
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

    public static Authentication createApiKeyAuthentication(ApiKeyService apiKeyService, Authentication userAuthentication) throws Exception {
        XContentBuilder keyDocSource = apiKeyService.newDocument(new SecureString("secret".toCharArray()), "test", userAuthentication,
                Collections.singleton(new RoleDescriptor("user_role_" + randomAlphaOfLength(4), new String[]{"manage"}, null, null)),
                Instant.now(), Instant.now().plus(Duration.ofSeconds(3600)), null, Version.CURRENT);
        Map<String, Object> keyDocMap = XContentHelper.convertToMap(BytesReference.bytes(keyDocSource), true, XContentType.JSON).v2();
        PlainActionFuture<AuthenticationResult> authenticationResultFuture = PlainActionFuture.newFuture();
        apiKeyService.validateApiKeyExpiration(keyDocMap, new ApiKeyService.ApiKeyCredentials("id", new SecureString("secret".toCharArray())),
                Clock.systemUTC(), authenticationResultFuture);
        return apiKeyService.createApiKeyAuthentication(authenticationResultFuture.get(), "node01");
    }

    private static void assertAuthorizationException(Throwable throwable, Matcher<String> messageMatcher) {
        assertThat(throwable, instanceOf(ElasticsearchSecurityException.class));
        ElasticsearchSecurityException securityException = (ElasticsearchSecurityException) throwable;
        assertThat(securityException.status(), is(RestStatus.FORBIDDEN));
        assertThat(throwable.getMessage(), messageMatcher);
    }
}
