/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheAction;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheRequest;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheResponse;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Map;

import static org.elasticsearch.test.SecuritySettingsSource.TEST_PASSWORD_HASHED;
import static org.elasticsearch.test.SecuritySettingsSource.addSSLSettingsForNodePEMFiles;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ServiceAccountSingleNodeTests extends SecuritySingleNodeTestCase {

    private static final String BEARER_TOKEN = "AAEAAWVsYXN0aWMvZmxlZXQtc2VydmVyL3Rva2VuMTpyNXdkYmRib1FTZTl2R09Ld2FKR0F3";
    private static final String SERVICE_ACCOUNT_MANAGER_NAME = "service_account_manager";

    @Override
    protected String configUsers() {
        return super.configUsers() + SERVICE_ACCOUNT_MANAGER_NAME + ":" + TEST_PASSWORD_HASHED + "\n";
    }

    @Override
    protected String configRoles() {
        return super.configRoles() + SERVICE_ACCOUNT_MANAGER_NAME + ":\n" + "  cluster:\n" + "    - 'manage_service_account'\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + SERVICE_ACCOUNT_MANAGER_NAME + ":" + SERVICE_ACCOUNT_MANAGER_NAME + "\n";
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings());
        addSSLSettingsForNodePEMFiles(builder, "xpack.security.http.", true);
        builder.put("xpack.security.http.ssl.enabled", true);
        return builder.build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected boolean transportSSLEnabled() {
        return true;
    }

    @Override
    protected String configServiceTokens() {
        return super.configServiceTokens()
            + "elastic/fleet-server/token1:"
            + "{PBKDF2_STRETCH}10000$8QN+eThJEaCd18sCP0nfzxJq2D9yhmSZgI20TDooYcE=$+0ELfqW4D2+/SlHvm/885dzv67qO2SMJg32Mv/9epXk=";
    }

    public void testAuthenticateWithServiceFileToken() {
        final AuthenticateRequest authenticateRequest = new AuthenticateRequest("elastic/fleet-server");
        final AuthenticateResponse authenticateResponse = createServiceAccountClient().execute(
            AuthenticateAction.INSTANCE,
            authenticateRequest
        ).actionGet();
        final String nodeName = node().settings().get(Node.NODE_NAME_SETTING.getKey());
        assertThat(authenticateResponse.authentication(), equalTo(getExpectedAuthentication("token1", "file")));
    }

    public void testApiServiceAccountToken() {
        final IndexServiceAccountTokenStore store = node().injector().getInstance(IndexServiceAccountTokenStore.class);
        final Cache<String, ListenableFuture<CachingServiceAccountTokenStore.CachedResult>> cache = store.getCache();
        final SecureString secretValue1 = createApiServiceToken("api-token-1");
        assertThat(cache.count(), equalTo(0));

        final AuthenticateRequest authenticateRequest = new AuthenticateRequest("elastic/fleet-server");
        final AuthenticateResponse authenticateResponse = createServiceAccountClient(secretValue1.toString()).execute(
            AuthenticateAction.INSTANCE,
            authenticateRequest
        ).actionGet();
        assertThat(authenticateResponse.authentication(), equalTo(getExpectedAuthentication("api-token-1", "index")));
        // cache is populated after authenticate
        assertThat(cache.count(), equalTo(1));

        final DeleteServiceAccountTokenRequest deleteServiceAccountTokenRequest = new DeleteServiceAccountTokenRequest(
            "elastic",
            "fleet-server",
            "api-token-1"
        );
        final DeleteServiceAccountTokenResponse deleteServiceAccountTokenResponse = createServiceAccountManagerClient().execute(
            DeleteServiceAccountTokenAction.INSTANCE,
            deleteServiceAccountTokenRequest
        ).actionGet();
        assertThat(deleteServiceAccountTokenResponse.found(), is(true));
        // cache is cleared after token deletion
        assertThat(cache.count(), equalTo(0));
    }

    public void testClearCache() {
        final IndexServiceAccountTokenStore indexStore = node().injector().getInstance(IndexServiceAccountTokenStore.class);
        final Cache<String, ListenableFuture<CachingServiceAccountTokenStore.CachedResult>> cache = indexStore.getCache();
        final SecureString secret1 = createApiServiceToken("api-token-1");
        final SecureString secret2 = createApiServiceToken("api-token-2");
        assertThat(cache.count(), equalTo(0));

        authenticateWithApiToken("api-token-1", secret1);
        assertThat(cache.count(), equalTo(1));
        authenticateWithApiToken("api-token-2", secret2);
        assertThat(cache.count(), equalTo(2));

        final ClearSecurityCacheRequest clearSecurityCacheRequest1 = new ClearSecurityCacheRequest().cacheName("service");
        if (randomBoolean()) {
            clearSecurityCacheRequest1.keys("elastic/fleet-server/");
        }
        final PlainActionFuture<ClearSecurityCacheResponse> future1 = new PlainActionFuture<>();
        client().execute(ClearSecurityCacheAction.INSTANCE, clearSecurityCacheRequest1, future1);
        assertThat(future1.actionGet().failures().isEmpty(), is(true));
        assertThat(cache.count(), equalTo(0));

        authenticateWithApiToken("api-token-1", secret1);
        assertThat(cache.count(), equalTo(1));
        authenticateWithApiToken("api-token-2", secret2);
        assertThat(cache.count(), equalTo(2));

        final ClearSecurityCacheRequest clearSecurityCacheRequest2 = new ClearSecurityCacheRequest().cacheName("service")
            .keys("elastic/fleet-server/api-token-" + randomFrom("1", "2"));
        final PlainActionFuture<ClearSecurityCacheResponse> future2 = new PlainActionFuture<>();
        client().execute(ClearSecurityCacheAction.INSTANCE, clearSecurityCacheRequest2, future2);
        assertThat(future2.actionGet().failures().isEmpty(), is(true));
        assertThat(cache.count(), equalTo(1));
    }

    private Client createServiceAccountManagerClient() {
        return client().filterWithHeader(
            Map.of("Authorization", basicAuthHeaderValue(SERVICE_ACCOUNT_MANAGER_NAME, new SecureString(TEST_PASSWORD.toCharArray())))
        );
    }

    private Client createServiceAccountClient() {
        return createServiceAccountClient(BEARER_TOKEN);
    }

    private Client createServiceAccountClient(String bearerString) {
        return client().filterWithHeader(Map.of("Authorization", "Bearer " + bearerString));
    }

    private Authentication getExpectedAuthentication(String tokenName, String tokenSource) {
        final String nodeName = node().settings().get(Node.NODE_NAME_SETTING.getKey());
        return new Authentication(
            new User(
                "elastic/fleet-server",
                Strings.EMPTY_ARRAY,
                "Service account - elastic/fleet-server",
                null,
                Map.of("_elastic_service_account", true),
                true
            ),
            new Authentication.RealmRef("_service_account", "_service_account", nodeName),
            null,
            Version.CURRENT,
            Authentication.AuthenticationType.TOKEN,
            Map.of("_token_name", tokenName, "_token_source", tokenSource)
        );
    }

    private SecureString createApiServiceToken(String tokenName) {
        final CreateServiceAccountTokenRequest createServiceAccountTokenRequest = new CreateServiceAccountTokenRequest(
            "elastic",
            "fleet-server",
            tokenName
        );
        final CreateServiceAccountTokenResponse createServiceAccountTokenResponse = createServiceAccountManagerClient().execute(
            CreateServiceAccountTokenAction.INSTANCE,
            createServiceAccountTokenRequest
        ).actionGet();
        assertThat(createServiceAccountTokenResponse.getName(), equalTo(tokenName));
        return createServiceAccountTokenResponse.getValue();
    }

    private void authenticateWithApiToken(String tokenName, SecureString secret) {
        final AuthenticateRequest authenticateRequest = new AuthenticateRequest("elastic/fleet-server");
        final AuthenticateResponse authenticateResponse = createServiceAccountClient(secret.toString()).execute(
            AuthenticateAction.INSTANCE,
            authenticateRequest
        ).actionGet();
        assertThat(authenticateResponse.authentication(), equalTo(getExpectedAuthentication(tokenName, "index")));
    }
}
