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
import org.elasticsearch.node.Node;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ServiceAccountSingleNodeTests extends SecuritySingleNodeTestCase {

    private static final String BEARER_TOKEN = "AAEAAWVsYXN0aWMvZmxlZXQvdG9rZW4xOnI1d2RiZGJvUVNlOXZHT0t3YUpHQXc";

    @Override
    protected String configServiceTokens() {
        return super.configServiceTokens()
            + "elastic/fleet/token1:"
            + "{PBKDF2_STRETCH}10000$8QN+eThJEaCd18sCP0nfzxJq2D9yhmSZgI20TDooYcE=$+0ELfqW4D2+/SlHvm/885dzv67qO2SMJg32Mv/9epXk=";
    }

    public void testAuthenticateWithServiceFileToken() {
        final AuthenticateRequest authenticateRequest = new AuthenticateRequest("elastic/fleet");
        final AuthenticateResponse authenticateResponse =
            createServiceAccountClient().execute(AuthenticateAction.INSTANCE, authenticateRequest).actionGet();
        final String nodeName = node().settings().get(Node.NODE_NAME_SETTING.getKey());
        assertThat(authenticateResponse.authentication(), equalTo(
            new Authentication(
                new User("elastic/fleet", Strings.EMPTY_ARRAY, "Service account - elastic/fleet", null,
                    Map.of("_elastic_service_account", true), true),
                new Authentication.RealmRef("service_account", "service_account", nodeName),
                null, Version.CURRENT, Authentication.AuthenticationType.TOKEN, Map.of("_token_name", "token1")
            )
        ));
    }

    private Client createServiceAccountClient() {
        return client().filterWithHeader(Map.of("Authorization", "Bearer " + BEARER_TOKEN));
    }
}
