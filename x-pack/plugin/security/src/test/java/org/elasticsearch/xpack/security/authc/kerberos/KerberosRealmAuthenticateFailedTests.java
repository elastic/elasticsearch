/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.support.MockLookupRealm;
import org.ietf.jgss.GSSException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.LoginException;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class KerberosRealmAuthenticateFailedTests extends KerberosRealmTestCase {

    public void testAuthenticateWithNonKerberosAuthenticationToken() {
        final KerberosRealm kerberosRealm = createKerberosRealm(randomAlphaOfLength(5));

        final UsernamePasswordToken usernamePasswordToken = new UsernamePasswordToken(
            randomAlphaOfLength(5),
            new SecureString(new char[] { 'a', 'b', 'c' })
        );
        expectThrows(AssertionError.class, () -> kerberosRealm.authenticate(usernamePasswordToken, new PlainActionFuture<>()));
    }

    public void testAuthenticateDifferentFailureScenarios() throws LoginException, GSSException {
        final String username = randomPrincipalName();
        final String outToken = randomAlphaOfLength(10);
        final KerberosRealm kerberosRealm = createKerberosRealm(username);
        final boolean validTicket = rarely();
        final boolean throwExceptionForInvalidTicket = validTicket ? false : randomBoolean();
        final boolean throwLoginException = randomBoolean();
        final byte[] decodedTicket = randomByteArrayOfLength(5);
        final Path keytabPath = config.env().configDir().resolve(config.getSetting(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH));
        final boolean krbDebug = config.getSetting(KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE);
        if (validTicket) {
            mockKerberosTicketValidator(decodedTicket, keytabPath, krbDebug, new Tuple<>(username, outToken), null);
        } else {
            if (throwExceptionForInvalidTicket) {
                if (throwLoginException) {
                    mockKerberosTicketValidator(decodedTicket, keytabPath, krbDebug, null, new LoginException("Login Exception"));
                } else {
                    mockKerberosTicketValidator(decodedTicket, keytabPath, krbDebug, null, new GSSException(GSSException.FAILURE));
                }
            } else {
                mockKerberosTicketValidator(decodedTicket, keytabPath, krbDebug, new Tuple<>(null, outToken), null);
            }
        }
        final boolean nullKerberosAuthnToken = rarely();
        final KerberosAuthenticationToken kerberosAuthenticationToken = nullKerberosAuthnToken
            ? null
            : new KerberosAuthenticationToken(decodedTicket);
        if (nullKerberosAuthnToken) {
            expectThrows(AssertionError.class, () -> kerberosRealm.authenticate(kerberosAuthenticationToken, new PlainActionFuture<>()));
        } else {
            final PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
            kerberosRealm.authenticate(kerberosAuthenticationToken, future);
            AuthenticationResult<User> result = future.actionGet();
            assertThat(result, is(notNullValue()));
            if (validTicket) {
                final String expectedUsername = maybeRemoveRealmName(username);
                final Map<String, Object> metadata = new HashMap<>();
                metadata.put(KerberosRealm.KRB_METADATA_REALM_NAME_KEY, realmName(username));
                metadata.put(KerberosRealm.KRB_METADATA_UPN_KEY, username);
                final User expectedUser = new User(expectedUsername, roles.toArray(new String[roles.size()]), null, null, metadata, true);
                assertSuccessAuthenticationResult(expectedUser, outToken, result);
            } else {
                assertThat(result.getStatus(), is(equalTo(AuthenticationResult.Status.TERMINATE)));
                if (throwExceptionForInvalidTicket == false) {
                    assertThat(result.getException(), is(instanceOf(ElasticsearchSecurityException.class)));
                    final List<String> wwwAuthnHeader = ((ElasticsearchSecurityException) result.getException()).getHeader(
                        KerberosAuthenticationToken.WWW_AUTHENTICATE
                    );
                    assertThat(wwwAuthnHeader, is(notNullValue()));
                    assertThat(wwwAuthnHeader.get(0), is(equalTo(KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER_PREFIX + outToken)));
                    assertThat(result.getMessage(), is(equalTo("failed to authenticate user, gss context negotiation not complete")));
                } else {
                    if (throwLoginException) {
                        assertThat(result.getMessage(), is(equalTo("failed to authenticate user, service login failure")));
                    } else {
                        assertThat(result.getMessage(), is(equalTo("failed to authenticate user, gss context negotiation failure")));
                    }
                    assertThat(result.getException(), is(instanceOf(ElasticsearchSecurityException.class)));
                    final List<String> wwwAuthnHeader = ((ElasticsearchSecurityException) result.getException()).getHeader(
                        KerberosAuthenticationToken.WWW_AUTHENTICATE
                    );
                    assertThat(wwwAuthnHeader, is(notNullValue()));
                    assertThat(wwwAuthnHeader.get(0), is(equalTo(KerberosAuthenticationToken.NEGOTIATE_SCHEME_NAME)));
                }
            }
            verify(mockKerberosTicketValidator).validateTicket(aryEq(decodedTicket), eq(keytabPath), eq(krbDebug), anyActionListener());
        }
    }

    public void testDelegatedAuthorizationFailedToResolve() throws Exception {
        final String username = randomPrincipalName();
        RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("mock", "other_realm");
        final MockLookupRealm otherRealm = new MockLookupRealm(
            new RealmConfig(
                realmIdentifier,
                Settings.builder()
                    .put(globalSettings)
                    .put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
                    .build(),
                TestEnvironment.newEnvironment(globalSettings),
                new ThreadContext(globalSettings)
            )
        );
        final User lookupUser = new User(randomAlphaOfLength(5));
        otherRealm.registerUser(lookupUser);

        settings = Settings.builder().put(settings).putList("authorization_realms", "other_realm").build();
        final KerberosRealm kerberosRealm = createKerberosRealm(Collections.singletonList(otherRealm), username);
        final byte[] decodedTicket = "base64encodedticket".getBytes(StandardCharsets.UTF_8);
        final Path keytabPath = config.env().configDir().resolve(config.getSetting(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH));
        final boolean krbDebug = config.getSetting(KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE);
        mockKerberosTicketValidator(decodedTicket, keytabPath, krbDebug, new Tuple<>(username, "out-token"), null);
        final KerberosAuthenticationToken kerberosAuthenticationToken = new KerberosAuthenticationToken(decodedTicket);

        final PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        kerberosRealm.authenticate(kerberosAuthenticationToken, future);

        AuthenticationResult<User> result = future.actionGet();
        assertThat(result.getStatus(), is(equalTo(AuthenticationResult.Status.CONTINUE)));
        verify(mockKerberosTicketValidator, times(1)).validateTicket(
            aryEq(decodedTicket),
            eq(keytabPath),
            eq(krbDebug),
            anyActionListener()
        );
        verify(mockNativeRoleMappingStore).clearRealmCacheOnChange(kerberosRealm);
        verifyNoMoreInteractions(mockKerberosTicketValidator, mockNativeRoleMappingStore);
    }
}
