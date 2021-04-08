/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
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

import static org.elasticsearch.test.SecuritySettingsSource.addSSLSettingsForNodePEMFiles;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ServiceAccountSingleNodeTests extends SecuritySingleNodeTestCase {

    private static final String BEARER_TOKEN = "AAEAAWVsYXN0aWMvZmxlZXQtc2VydmVyL3Rva2VuMTpyNXdkYmRib1FTZTl2R09Ld2FKR0F3";

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
        final AuthenticateResponse authenticateResponse =
            createServiceAccountClient().execute(AuthenticateAction.INSTANCE, authenticateRequest).actionGet();
        final String nodeName = node().settings().get(Node.NODE_NAME_SETTING.getKey());
        assertThat(authenticateResponse.authentication(), equalTo(
           getExpectedAuthentication("token1")
        ));
    }

    public void testApiServiceAccountToken() {
        final IndexServiceAccountsTokenStore store = node().injector().getInstance(IndexServiceAccountsTokenStore.class);
        final Cache<String, ListenableFuture<CachingServiceAccountsTokenStore.CachedResult>> cache = store.getCache();
        final CreateServiceAccountTokenRequest createServiceAccountTokenRequest =
            new CreateServiceAccountTokenRequest("elastic", "fleet-server", "api-token-1");
        final CreateServiceAccountTokenResponse createServiceAccountTokenResponse =
            client().execute(CreateServiceAccountTokenAction.INSTANCE, createServiceAccountTokenRequest).actionGet();
        assertThat(createServiceAccountTokenResponse.getName(), equalTo("api-token-1"));
        assertThat(cache.count(), equalTo(0));

        final AuthenticateRequest authenticateRequest = new AuthenticateRequest("elastic/fleet-server");
        final AuthenticateResponse authenticateResponse =
            createServiceAccountClient(createServiceAccountTokenResponse.getValue().toString())
                .execute(AuthenticateAction.INSTANCE, authenticateRequest).actionGet();
        assertThat(authenticateResponse.authentication(), equalTo(getExpectedAuthentication("api-token-1")));
        // cache is populated after authenticate
        assertThat(cache.count(), equalTo(1));

        final DeleteServiceAccountTokenRequest deleteServiceAccountTokenRequest =
            new DeleteServiceAccountTokenRequest("elastic", "fleet-server", "api-token-1");
        final DeleteServiceAccountTokenResponse deleteServiceAccountTokenResponse =
            client().execute(DeleteServiceAccountTokenAction.INSTANCE, deleteServiceAccountTokenRequest).actionGet();
        assertThat(deleteServiceAccountTokenResponse.found(), is(true));
        // cache is cleared after token deletion
        assertThat(cache.count(), equalTo(0));
    }

    private Client createServiceAccountClient() {
        return createServiceAccountClient(BEARER_TOKEN);
    }

    private Client createServiceAccountClient(String bearerString) {
        return client().filterWithHeader(Map.of("Authorization", "Bearer " + bearerString));
    }

    private Authentication getExpectedAuthentication(String tokenName) {
        final String nodeName = node().settings().get(Node.NODE_NAME_SETTING.getKey());
        return new Authentication(
            new User("elastic/fleet-server", Strings.EMPTY_ARRAY, "Service account - elastic/fleet-server", null,
                Map.of("_elastic_service_account", true), true),
            new Authentication.RealmRef("service_account", "service_account", nodeName),
            null, Version.CURRENT, Authentication.AuthenticationType.TOKEN, Map.of("_token_name", tokenName)
        );
    }
}
