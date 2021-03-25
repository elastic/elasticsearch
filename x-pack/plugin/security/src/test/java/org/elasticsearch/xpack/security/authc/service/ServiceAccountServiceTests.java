/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class ServiceAccountServiceTests extends ESTestCase {

    private ThreadContext threadContext;
    private ServiceAccountsTokenStore serviceAccountsTokenStore;
    private ServiceAccountService serviceAccountService;

    @Before
    public void init() {
        threadContext = new ThreadContext(Settings.EMPTY);
        serviceAccountsTokenStore = mock(ServiceAccountsTokenStore.class);
        serviceAccountService = new ServiceAccountService(serviceAccountsTokenStore);
    }

    public void testIsServiceAccount() {
        final User user =
            new User(randomAlphaOfLengthBetween(3, 8), randomArray(0, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8)));
        final Authentication.RealmRef authRealm;
        final boolean authRealmIsForServiceAccount = randomBoolean();
        if (authRealmIsForServiceAccount) {
            authRealm = new Authentication.RealmRef(ServiceAccountService.REALM_NAME,
                ServiceAccountService.REALM_TYPE,
                randomAlphaOfLengthBetween(3, 8));
        } else {
            authRealm = randomRealmRef();
        }
        final Authentication.RealmRef lookupRealm = randomFrom(randomRealmRef(), null);
        final Authentication authentication = new Authentication(user, authRealm, lookupRealm);

        if (authRealmIsForServiceAccount && lookupRealm == null) {
            assertThat(ServiceAccountService.isServiceAccount(authentication), is(true));
        } else {
            assertThat(ServiceAccountService.isServiceAccount(authentication), is(false));
        }
    }

    public void testGetServiceAccountPrincipals() {
        assertThat(ServiceAccountService.getServiceAccountPrincipals(), equalTo(Set.of("elastic/fleet")));
    }

    public void testTryParseToken() throws IOException, IllegalAccessException {
        // Null for null
        assertNull(ServiceAccountService.tryParseToken(null));

        final byte[] magicBytes = { 0, 1, 0, 1 };

        final Logger satLogger = LogManager.getLogger(ServiceAccountToken.class);
        Loggers.setLevel(satLogger, Level.TRACE);
        final Logger sasLogger = LogManager.getLogger(ServiceAccountService.class);
        Loggers.setLevel(sasLogger, Level.TRACE);

        final MockLogAppender appender = new MockLogAppender();
        Loggers.addAppender(satLogger, appender);
        Loggers.addAppender(sasLogger, appender);
        appender.start();

        try {
            // Less than 4 bytes
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "less than 4 bytes", ServiceAccountToken.class.getName(), Level.TRACE,
                "service account token expects the 4 leading bytes")
            );
            final SecureString bearerString0 = createBearerString(List.of(Arrays.copyOfRange(magicBytes, 0, randomIntBetween(0, 3))));
            assertNull(ServiceAccountService.tryParseToken(bearerString0));
            appender.assertAllExpectationsMatched();

            // Prefix mismatch
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "prefix mismatch", ServiceAccountToken.class.getName(), Level.TRACE,
                "service account token expects the 4 leading bytes"
            ));
            final SecureString bearerString1 = createBearerString(List.of(
                new byte[] { randomValueOtherThan((byte) 0, ESTestCase::randomByte) },
                randomByteArrayOfLength(randomIntBetween(30, 50))));
            assertNull(ServiceAccountService.tryParseToken(bearerString1));
            appender.assertAllExpectationsMatched();

            // No colon
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "no colon", ServiceAccountToken.class.getName(), Level.TRACE,
                "failed to extract qualified service token name and secret, missing ':'"
            ));
            final SecureString bearerString2 = createBearerString(List.of(
                magicBytes,
                randomAlphaOfLengthBetween(30, 50).getBytes(StandardCharsets.UTF_8)));
            assertNull(ServiceAccountService.tryParseToken(bearerString2));
            appender.assertAllExpectationsMatched();

            // Invalid delimiter for qualified name
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "invalid delimiter for qualified name", ServiceAccountToken.class.getName(), Level.TRACE,
                "The qualified name of a service token should take format of 'namespace/service_name/token_name'"
            ));
            if (randomBoolean()) {
                final SecureString bearerString3 = createBearerString(List.of(
                    magicBytes,
                    (randomAlphaOfLengthBetween(10, 20) + ":" + randomAlphaOfLengthBetween(10, 20)).getBytes(StandardCharsets.UTF_8)
                ));
                assertNull(ServiceAccountService.tryParseToken(bearerString3));
            } else {
                final SecureString bearerString3 = createBearerString(List.of(
                    magicBytes,
                    (randomAlphaOfLengthBetween(3, 8) + "/" + randomAlphaOfLengthBetween(3, 8)
                        + ":" + randomAlphaOfLengthBetween(10, 20)).getBytes(StandardCharsets.UTF_8)
                ));
                assertNull(ServiceAccountService.tryParseToken(bearerString3));
            }
            appender.assertAllExpectationsMatched();

            // Invalid token name
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "invalid token name", ServiceAccountService.class.getName(), Level.TRACE,
                "Cannot parse possible service account token"
            ));
            final SecureString bearerString4 = createBearerString(List.of(
                magicBytes,
                (randomAlphaOfLengthBetween(3, 8) + "/" + randomAlphaOfLengthBetween(3, 8)
                    + "/" + ServiceAccountTokenTests.randomInvalidTokenName()
                    + ":" + randomAlphaOfLengthBetween(10, 20)).getBytes(StandardCharsets.UTF_8)
            ));
            assertNull(ServiceAccountService.tryParseToken(bearerString4));
            appender.assertAllExpectationsMatched();

            // Everything is good
            final String namespace = randomAlphaOfLengthBetween(3, 8);
            final String serviceName = randomAlphaOfLengthBetween(3, 8);
            final String tokenName = ServiceAccountTokenTests.randomTokenName();
            final ServiceAccountId accountId = new ServiceAccountId(namespace, serviceName);
            final String secret = randomAlphaOfLengthBetween(10, 20);
            final SecureString bearerString5 = createBearerString(List.of(
                magicBytes,
                (namespace + "/" + serviceName + "/" + tokenName + ":" + secret).getBytes(StandardCharsets.UTF_8)
            ));
            final ServiceAccountToken serviceAccountToken1 = ServiceAccountService.tryParseToken(bearerString5);
            final ServiceAccountToken serviceAccountToken2 = new ServiceAccountToken(accountId, tokenName,
                new SecureString(secret.toCharArray()));
            assertThat(serviceAccountToken1, equalTo(serviceAccountToken2));

            // Serialise and de-serialise service account token
            final ServiceAccountToken parsedToken = ServiceAccountService.tryParseToken(serviceAccountToken2.asBearerString());
            assertThat(parsedToken, equalTo(serviceAccountToken2));

            // Invalid magic byte
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "invalid magic byte again", ServiceAccountToken.class.getName(), Level.TRACE,
                "service account token expects the 4 leading bytes"
            ));
            assertNull(ServiceAccountService.tryParseToken(
                new SecureString("AQEAAWVsYXN0aWMvZmxlZXQvdG9rZW4xOnN1cGVyc2VjcmV0".toCharArray())));
            appender.assertAllExpectationsMatched();

            // No colon
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "no colon again", ServiceAccountToken.class.getName(), Level.TRACE,
                "failed to extract qualified service token name and secret, missing ':'"
            ));
            assertNull(ServiceAccountService.tryParseToken(
                new SecureString("AAEAAWVsYXN0aWMvZmxlZXQvdG9rZW4xX3N1cGVyc2VjcmV0".toCharArray())));
            appender.assertAllExpectationsMatched();

            // Invalid qualified name
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "invalid delimiter for qualified name again", ServiceAccountToken.class.getName(), Level.TRACE,
                "The qualified name of a service token should take format of 'namespace/service_name/token_name'"
            ));
            assertNull(ServiceAccountService.tryParseToken(
                new SecureString("AAEAAWVsYXN0aWMvZmxlZXRfdG9rZW4xOnN1cGVyc2VjcmV0".toCharArray())));
            appender.assertAllExpectationsMatched();

            // Invalid token name
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "invalid token name again", ServiceAccountService.class.getName(), Level.TRACE,
                "Cannot parse possible service account token"
            ));
            assertNull(ServiceAccountService.tryParseToken(
                new SecureString("AAEAAWVsYXN0aWMvZmxlZXQvdG9rZW4hOnN1cGVyc2VjcmV0".toCharArray())));
            appender.assertAllExpectationsMatched();

            // everything is fine
            assertThat(ServiceAccountService.tryParseToken(
                new SecureString("AAEAAWVsYXN0aWMvZmxlZXQvdG9rZW4xOnN1cGVyc2VjcmV0".toCharArray())),
                equalTo(new ServiceAccountToken(new ServiceAccountId("elastic", "fleet"), "token1",
                    new SecureString("supersecret".toCharArray()))));
        } finally {
            appender.stop();
            Loggers.setLevel(satLogger, Level.INFO);
            Loggers.setLevel(sasLogger, Level.INFO);
            Loggers.removeAppender(satLogger, appender);
            Loggers.removeAppender(sasLogger, appender);
        }
    }

    private Authentication.RealmRef randomRealmRef() {
        return new Authentication.RealmRef(randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8));
    }

    public void testTryAuthenticateBearerToken() throws ExecutionException, InterruptedException {
        // Valid token
        final PlainActionFuture<Authentication> future5 = new PlainActionFuture<>();
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<Boolean> listener = (ActionListener<Boolean>) invocationOnMock.getArguments()[1];
            listener.onResponse(true);
            return null;
        }).when(serviceAccountsTokenStore).authenticate(any(), any());
        final String nodeName = randomAlphaOfLengthBetween(3, 8);
        serviceAccountService.authenticateToken(
            new ServiceAccountToken(new ServiceAccountId("elastic", "fleet"), "token1",
                new SecureString("super-secret-value".toCharArray())),
            nodeName, future5);
        assertThat(future5.get(), equalTo(
            new Authentication(
                new User("elastic/fleet", Strings.EMPTY_ARRAY, "Service account - elastic/fleet", null,
                    Map.of("_elastic_service_account", true), true),
                new Authentication.RealmRef(ServiceAccountService.REALM_NAME, ServiceAccountService.REALM_TYPE, nodeName),
                null, Version.CURRENT, Authentication.AuthenticationType.TOKEN,
                Map.of("_token_name", "token1")
            )
        ));
    }

    public void testAuthenticateWithToken() throws ExecutionException, InterruptedException, IllegalAccessException {
        final Logger sasLogger = LogManager.getLogger(ServiceAccountService.class);
        Loggers.setLevel(sasLogger, Level.TRACE);

        final MockLogAppender appender = new MockLogAppender();
        Loggers.addAppender(sasLogger, appender);
        appender.start();

        try {
            // non-elastic service account
            final ServiceAccountId accountId1 = new ServiceAccountId(
                randomValueOtherThan(ElasticServiceAccounts.NAMESPACE, () -> randomAlphaOfLengthBetween(3, 8)),
                randomAlphaOfLengthBetween(3, 8));
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "non-elastic service account", ServiceAccountService.class.getName(), Level.DEBUG,
                "only [elastic] service accounts are supported, but received [" + accountId1.asPrincipal() + "]"
            ));
            final SecureString secret = new SecureString(randomAlphaOfLength(20).toCharArray());
            final ServiceAccountToken token1 = new ServiceAccountToken(accountId1, randomAlphaOfLengthBetween(3, 8), secret);
            final PlainActionFuture<Authentication> future1 = new PlainActionFuture<>();
            serviceAccountService.authenticateToken(token1, randomAlphaOfLengthBetween(3, 8), future1);
            final ExecutionException e1 = expectThrows(ExecutionException.class, future1::get);
            assertThat(e1.getCause().getClass(), is(ElasticsearchSecurityException.class));
            assertThat(e1.getMessage(), containsString("failed to authenticate service account ["
                + token1.getAccountId().asPrincipal() + "] with token name [" + token1.getTokenName() + "]"));
            appender.assertAllExpectationsMatched();

            // Unknown elastic service name
            final ServiceAccountId accountId2 = new ServiceAccountId(
                ElasticServiceAccounts.NAMESPACE,
                randomValueOtherThan("fleet", () -> randomAlphaOfLengthBetween(3, 8)));
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "non-elastic service account", ServiceAccountService.class.getName(), Level.DEBUG,
                "the [" + accountId2.asPrincipal() + "] service account does not exist"
            ));
            final ServiceAccountToken token2 = new ServiceAccountToken(accountId2, randomAlphaOfLengthBetween(3, 8), secret);
            final PlainActionFuture<Authentication> future2 = new PlainActionFuture<>();
            serviceAccountService.authenticateToken(token2, randomAlphaOfLengthBetween(3, 8), future2);
            final ExecutionException e2 = expectThrows(ExecutionException.class, future2::get);
            assertThat(e2.getCause().getClass(), is(ElasticsearchSecurityException.class));
            assertThat(e2.getMessage(), containsString("failed to authenticate service account ["
                + token2.getAccountId().asPrincipal() + "] with token name [" + token2.getTokenName() + "]"));
            appender.assertAllExpectationsMatched();

            // Success based on credential store
            final ServiceAccountId accountId3 = new ServiceAccountId(ElasticServiceAccounts.NAMESPACE, "fleet");
            final ServiceAccountToken token3 = new ServiceAccountToken(accountId3, randomAlphaOfLengthBetween(3, 8), secret);
            final ServiceAccountToken token4 = new ServiceAccountToken(accountId3, randomAlphaOfLengthBetween(3, 8),
                new SecureString(randomAlphaOfLength(20).toCharArray()));
            final String nodeName = randomAlphaOfLengthBetween(3, 8);
            doAnswer(invocationOnMock -> {
                @SuppressWarnings("unchecked")
                final ActionListener<Boolean> listener = (ActionListener<Boolean>) invocationOnMock.getArguments()[1];
                listener.onResponse(true);
                return null;
            }).when(serviceAccountsTokenStore).authenticate(eq(token3), any());

            doAnswer(invocationOnMock -> {
                @SuppressWarnings("unchecked")
                final ActionListener<Boolean> listener = (ActionListener<Boolean>) invocationOnMock.getArguments()[1];
                listener.onResponse(false);
                return null;
            }).when(serviceAccountsTokenStore).authenticate(eq(token4), any());

            final PlainActionFuture<Authentication> future3 = new PlainActionFuture<>();
            serviceAccountService.authenticateToken(token3, nodeName, future3);
            final Authentication authentication = future3.get();
            assertThat(authentication, equalTo(new Authentication(
                new User("elastic/fleet", Strings.EMPTY_ARRAY,
                    "Service account - elastic/fleet", null, Map.of("_elastic_service_account", true),
                    true),
                new Authentication.RealmRef(ServiceAccountService.REALM_NAME, ServiceAccountService.REALM_TYPE, nodeName),
                null, Version.CURRENT, Authentication.AuthenticationType.TOKEN,
                Map.of("_token_name", token3.getTokenName())
            )));

            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "non-elastic service account", ServiceAccountService.class.getName(), Level.DEBUG,
                "failed to authenticate service account [" + token4.getAccountId().asPrincipal()
                    + "] with token name [" + token4.getTokenName() + "]"
            ));
            final PlainActionFuture<Authentication> future4 = new PlainActionFuture<>();
            serviceAccountService.authenticateToken(token4, nodeName, future4);
            final ExecutionException e4 = expectThrows(ExecutionException.class, future4::get);
            assertThat(e4.getCause().getClass(), is(ElasticsearchSecurityException.class));
            assertThat(e4.getMessage(), containsString("failed to authenticate service account ["
                + token4.getAccountId().asPrincipal() + "] with token name [" + token4.getTokenName() + "]"));
            appender.assertAllExpectationsMatched();
        } finally {
            appender.stop();
            Loggers.setLevel(sasLogger, Level.INFO);
            Loggers.removeAppender(sasLogger, appender);
        }
    }

    public void testGetRoleDescriptor() throws ExecutionException, InterruptedException {
        final Authentication auth1 = new Authentication(
            new User("elastic/fleet",
                Strings.EMPTY_ARRAY,
                "Service account - elastic/fleet",
                null,
                Map.of("_elastic_service_account", true),
                true),
            new Authentication.RealmRef(
                ServiceAccountService.REALM_NAME, ServiceAccountService.REALM_TYPE, randomAlphaOfLengthBetween(3, 8)),
            null,
            Version.CURRENT,
            Authentication.AuthenticationType.TOKEN,
            Map.of("_token_name", randomAlphaOfLengthBetween(3, 8)));

        final PlainActionFuture<RoleDescriptor> future1 = new PlainActionFuture<>();
        serviceAccountService.getRoleDescriptor(auth1, future1);
        final RoleDescriptor roleDescriptor1 = future1.get();
        assertNotNull(roleDescriptor1);
        assertThat(roleDescriptor1.getName(), equalTo("elastic/fleet"));

        final String username =
            randomValueOtherThan("elastic/fleet", () -> randomAlphaOfLengthBetween(3, 8) + "/" + randomAlphaOfLengthBetween(3, 8));
        final Authentication auth2 = new Authentication(
            new User(username, Strings.EMPTY_ARRAY, "Service account - " + username, null,
                Map.of("_elastic_service_account", true), true),
            new Authentication.RealmRef(
                ServiceAccountService.REALM_NAME, ServiceAccountService.REALM_TYPE, randomAlphaOfLengthBetween(3, 8)),
            null,
            Version.CURRENT,
            Authentication.AuthenticationType.TOKEN,
            Map.of("_token_name", randomAlphaOfLengthBetween(3, 8)));
        final PlainActionFuture<RoleDescriptor> future2 = new PlainActionFuture<>();
        serviceAccountService.getRoleDescriptor(auth2, future2);
        final ExecutionException e = expectThrows(ExecutionException.class, () -> future2.get());
        assertThat(e.getCause().getClass(), is(ElasticsearchSecurityException.class));
        assertThat(e.getMessage(), containsString(
            "cannot load role for service account [" + username + "] - no such service account"));
    }

    private SecureString createBearerString(List<byte[]> bytesList) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            for (byte[] bytes : bytesList) {
                out.write(bytes);
            }
            return new SecureString(Base64.getEncoder().withoutPadding().encodeToString(out.toByteArray()).toCharArray());
        }
    }
}
