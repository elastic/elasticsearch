/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper.UserData;
import org.ietf.jgss.GSSException;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class KerberosRealmCacheTests extends KerberosRealmTestCase {

    public void testAuthenticateWithCache() throws LoginException, GSSException {
        final String username = randomPrincipalName();
        final String outToken = randomAlphaOfLength(10);
        final KerberosRealm kerberosRealm = createKerberosRealm(username);

        final String expectedUsername = maybeRemoveRealmName(username);
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put(KerberosRealm.KRB_METADATA_REALM_NAME_KEY, realmName(username));
        metadata.put(KerberosRealm.KRB_METADATA_UPN_KEY, username);
        final User expectedUser = new User(expectedUsername, roles.toArray(new String[roles.size()]), null, null, metadata, true);
        final byte[] decodedTicket = randomByteArrayOfLength(10);
        final Path keytabPath = config.env().configFile().resolve(config.getSetting(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH));
        final boolean krbDebug = config.getSetting(KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE);
        mockKerberosTicketValidator(decodedTicket, keytabPath, krbDebug, new Tuple<>(username, outToken), null);
        final KerberosAuthenticationToken kerberosAuthenticationToken = new KerberosAuthenticationToken(decodedTicket);

        // authenticate
        final User user1 = authenticateAndAssertResult(kerberosRealm, expectedUser, kerberosAuthenticationToken, outToken);
        // authenticate with cache
        final User user2 = authenticateAndAssertResult(kerberosRealm, expectedUser, kerberosAuthenticationToken, outToken);

        assertThat(user1, sameInstance(user2));
        verify(mockKerberosTicketValidator, times(2)).validateTicket(aryEq(decodedTicket), eq(keytabPath), eq(krbDebug),
                any(ActionListener.class));
        verify(mockNativeRoleMappingStore).refreshRealmOnChange(kerberosRealm);
        verify(mockNativeRoleMappingStore).resolveRoles(any(UserData.class), any(ActionListener.class));
        verifyNoMoreInteractions(mockKerberosTicketValidator, mockNativeRoleMappingStore);
    }

    public void testCacheInvalidationScenarios() throws LoginException, GSSException {
        final String outToken = randomAlphaOfLength(10);
        final List<String> userNames = Arrays.asList(randomPrincipalName(), randomPrincipalName());
        final KerberosRealm kerberosRealm = createKerberosRealm(userNames.toArray(new String[0]));
        verify(mockNativeRoleMappingStore).refreshRealmOnChange(kerberosRealm);

        final String authNUsername = randomFrom(userNames);
        final byte[] decodedTicket = randomByteArrayOfLength(10);
        final Path keytabPath = config.env().configFile().resolve(config.getSetting(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH));
        final boolean krbDebug = config.getSetting(KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE);
        mockKerberosTicketValidator(decodedTicket, keytabPath, krbDebug, new Tuple<>(authNUsername, outToken), null);
        final String expectedUsername = maybeRemoveRealmName(authNUsername);
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put(KerberosRealm.KRB_METADATA_REALM_NAME_KEY, realmName(authNUsername));
        metadata.put(KerberosRealm.KRB_METADATA_UPN_KEY, authNUsername);
        final User expectedUser = new User(expectedUsername, roles.toArray(new String[roles.size()]), null, null, metadata, true);

        final KerberosAuthenticationToken kerberosAuthenticationToken = new KerberosAuthenticationToken(decodedTicket);
        final User user1 = authenticateAndAssertResult(kerberosRealm, expectedUser, kerberosAuthenticationToken, outToken);

        final String expireThisUser = randomFrom(userNames);
        boolean expireAll = randomBoolean();
        if (expireAll) {
            kerberosRealm.expireAll();
        } else {
            kerberosRealm.expire(maybeRemoveRealmName(expireThisUser));
        }

        final User user2 = authenticateAndAssertResult(kerberosRealm, expectedUser, kerberosAuthenticationToken, outToken);

        if (expireAll || expireThisUser.equals(authNUsername)) {
            assertThat(user1, is(not(sameInstance(user2))));
            verify(mockNativeRoleMappingStore, times(2)).resolveRoles(any(UserData.class), any(ActionListener.class));
        } else {
            assertThat(user1, sameInstance(user2));
            verify(mockNativeRoleMappingStore).resolveRoles(any(UserData.class), any(ActionListener.class));
        }
        verify(mockKerberosTicketValidator, times(2)).validateTicket(aryEq(decodedTicket), eq(keytabPath), eq(krbDebug),
                any(ActionListener.class));
        verifyNoMoreInteractions(mockKerberosTicketValidator, mockNativeRoleMappingStore);
    }

    public void testAuthenticateWithValidTicketSucessAuthnWithUserDetailsWhenCacheDisabled()
            throws LoginException, GSSException, IOException {
        // if cache.ttl <= 0 then the cache is disabled
        settings = buildKerberosRealmSettings(REALM_NAME,
            writeKeyTab(dir.resolve("key.keytab"), randomAlphaOfLength(4)).toString(), 100, "0m", true, randomBoolean());
        final String username = randomPrincipalName();
        final String outToken = randomAlphaOfLength(10);
        final KerberosRealm kerberosRealm = createKerberosRealm(username);

        final String expectedUsername = maybeRemoveRealmName(username);
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put(KerberosRealm.KRB_METADATA_REALM_NAME_KEY, realmName(username));
        metadata.put(KerberosRealm.KRB_METADATA_UPN_KEY, username);
        final User expectedUser = new User(expectedUsername, roles.toArray(new String[roles.size()]), null, null, metadata, true);
        final byte[] decodedTicket = randomByteArrayOfLength(10);
        final Path keytabPath = config.env().configFile().resolve(config.getSetting(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH));
        final boolean krbDebug = config.getSetting(KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE);
        mockKerberosTicketValidator(decodedTicket, keytabPath, krbDebug, new Tuple<>(username, outToken), null);
        final KerberosAuthenticationToken kerberosAuthenticationToken = new KerberosAuthenticationToken(decodedTicket);

        // authenticate
        final User user1 = authenticateAndAssertResult(kerberosRealm, expectedUser, kerberosAuthenticationToken, outToken);
        // authenticate when cache has been disabled
        final User user2 = authenticateAndAssertResult(kerberosRealm, expectedUser, kerberosAuthenticationToken, outToken);

        assertThat(user1, not(sameInstance(user2)));
        verify(mockKerberosTicketValidator, times(2)).validateTicket(aryEq(decodedTicket), eq(keytabPath), eq(krbDebug),
                any(ActionListener.class));
        verify(mockNativeRoleMappingStore).refreshRealmOnChange(kerberosRealm);
        verify(mockNativeRoleMappingStore, times(2)).resolveRoles(any(UserData.class), any(ActionListener.class));
        verifyNoMoreInteractions(mockKerberosTicketValidator, mockNativeRoleMappingStore);
    }

    private User authenticateAndAssertResult(final KerberosRealm kerberosRealm, final User expectedUser,
            final KerberosAuthenticationToken kerberosAuthenticationToken, String outToken) {
        final PlainActionFuture<AuthenticationResult> future = PlainActionFuture.newFuture();
        kerberosRealm.authenticate(kerberosAuthenticationToken, future);
        final AuthenticationResult result = future.actionGet();
        assertSuccessAuthenticationResult(expectedUser, outToken, result);
        return result.getUser();
    }
}
