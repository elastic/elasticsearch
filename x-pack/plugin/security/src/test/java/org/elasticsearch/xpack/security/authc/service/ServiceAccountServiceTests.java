/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.support.SecurityTokenType;
import org.junit.Before;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServiceAccountServiceTests extends ESTestCase {

    private ThreadContext threadContext;
    private ServiceAccountsCredentialStore serviceAccountsCredentialStore;
    private ServiceAccountService serviceAccountService;

    @Before
    public void init() {
        threadContext = new ThreadContext(Settings.EMPTY);
        serviceAccountsCredentialStore = mock(ServiceAccountsCredentialStore.class);
        serviceAccountService = new ServiceAccountService(serviceAccountsCredentialStore);
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

    public void testTryParseToken() throws IOException {
        // Null for null
        assertNull(ServiceAccountService.tryParseToken(null));

        final ServiceAccount.ServiceAccountId accountId =
            new ServiceAccount.ServiceAccountId(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
        final String tokenName = randomAlphaOfLengthBetween(3, 8);
        final SecureString secret = new SecureString(randomAlphaOfLength(20).toCharArray());

        // Invalid version or token type
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            if (randomBoolean()) {
                final Version invalidVersion = VersionUtils.randomVersionBetween(random(),
                    Version.V_7_0_0,
                    VersionUtils.getPreviousVersion(ServiceAccountService.VERSION_MINIMUM));
                Version.writeVersion(invalidVersion, out);
                out.writeEnum(SecurityTokenType.SERVICE_ACCOUNT);
            } else {
                Version.writeVersion(
                    VersionUtils.randomVersionBetween(random(), ServiceAccountService.VERSION_MINIMUM, Version.CURRENT),
                    out);
                out.writeEnum(randomFrom(SecurityTokenType.ACCESS_TOKEN, SecurityTokenType.REFRESH_TOKEN));
            }
            accountId.write(out);
            out.writeString(tokenName);
            out.writeSecureString(secret);
            out.flush();

            final String base64 = Base64.getEncoder().withoutPadding().encodeToString(out.bytes().toBytesRef().bytes);
            assertNull(ServiceAccountService.tryParseToken(new SecureString(base64.toCharArray())));
        }

        // Serialise and de-serialise service account token
        final ServiceAccountToken serviceAccountToken = new ServiceAccountToken(accountId, tokenName, secret);
        final ServiceAccountToken parsedToken = ServiceAccountService.tryParseToken(serviceAccountToken.asBearerString());
        assertThat(parsedToken, equalTo(serviceAccountToken));
    }

    private Authentication.RealmRef randomRealmRef() {
        return new Authentication.RealmRef(randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8));
    }

    public void testAuthenticateWithToken() throws ExecutionException, InterruptedException {
        // Null for non-elastic service account
        final ServiceAccount.ServiceAccountId accountId1 = new ServiceAccount.ServiceAccountId(
            randomValueOtherThan(ElasticServiceAccounts.NAMESPACE, () -> randomAlphaOfLengthBetween(3, 8)),
            randomAlphaOfLengthBetween(3, 8));
        final SecureString secret = new SecureString(randomAlphaOfLength(20).toCharArray());
        final ServiceAccountToken token1 = new ServiceAccountToken(accountId1, randomAlphaOfLengthBetween(3, 8), secret);
        final PlainActionFuture<Authentication> future1 = new PlainActionFuture<>();
        serviceAccountService.authenticateWithToken(token1, threadContext, randomAlphaOfLengthBetween(3, 8), future1);
        final ExecutionException e1 = expectThrows(ExecutionException.class, future1::get);
        assertThat(e1.getCause().getClass(), is(ElasticsearchSecurityException.class));
        assertThat(e1.getMessage(), containsString(
            "only [" + ElasticServiceAccounts.NAMESPACE + "] service accounts are supported, " +
                "but received [" + accountId1.asPrincipal() + "]"));

        // Null for unknown elastic service name
        final ServiceAccount.ServiceAccountId accountId2 = new ServiceAccount.ServiceAccountId(
            ElasticServiceAccounts.NAMESPACE,
            randomValueOtherThan("fleet", () -> randomAlphaOfLengthBetween(3, 8)));
        final ServiceAccountToken token2 = new ServiceAccountToken(accountId2, randomAlphaOfLengthBetween(3, 8), secret);
        final PlainActionFuture<Authentication> future2 = new PlainActionFuture<>();
        serviceAccountService.authenticateWithToken(token2, threadContext, randomAlphaOfLengthBetween(3, 8), future2);
        final ExecutionException e2 = expectThrows(ExecutionException.class, future2::get);
        assertThat(e2.getCause().getClass(), is(ElasticsearchSecurityException.class));
        assertThat(e2.getMessage(), containsString(
            "the [" + accountId2.asPrincipal() + "] service account does not exist"));

        // Success based on credential store
        final ServiceAccount.ServiceAccountId accountId3 = new ServiceAccount.ServiceAccountId(ElasticServiceAccounts.NAMESPACE, "fleet");
        final ServiceAccountToken token3 = new ServiceAccountToken(accountId3, randomAlphaOfLengthBetween(3, 8), secret);
        final ServiceAccountToken token4 = new ServiceAccountToken(accountId3, randomAlphaOfLengthBetween(3, 8),
            new SecureString(randomAlphaOfLength(20).toCharArray()));
        final String nodeName = randomAlphaOfLengthBetween(3, 8);
        when(serviceAccountsCredentialStore.authenticate(token3)).thenReturn(true);
        when(serviceAccountsCredentialStore.authenticate(token4)).thenReturn(false);

        final PlainActionFuture<Authentication> future3 = new PlainActionFuture<>();
        serviceAccountService.authenticateWithToken(token3, threadContext, nodeName, future3);
        final Authentication authentication = future3.get();
        assertThat(authentication, equalTo(new Authentication(
            new User("elastic/fleet", Strings.EMPTY_ARRAY,
                "Service account - elastic/fleet", null, Map.of("_elastic_service_account", true),
                true),
            new Authentication.RealmRef(ServiceAccountService.REALM_NAME, ServiceAccountService.REALM_TYPE, nodeName),
            null, Version.CURRENT, Authentication.AuthenticationType.TOKEN,
            Map.of("_token_name", token3.getTokenName())
        )));

        final PlainActionFuture<Authentication> future4 = new PlainActionFuture<>();
        serviceAccountService.authenticateWithToken(token4, threadContext, nodeName, future4);
        final ExecutionException e4 = expectThrows(ExecutionException.class, future4::get);
        assertThat(e4.getCause().getClass(), is(ElasticsearchSecurityException.class));
        assertThat(e4.getMessage(), containsString("failed to authenticate service account ["
            + token4.getAccountId().asPrincipal() + "] with token name [" + token4.getTokenName() + "]"));
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
}
