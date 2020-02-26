/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportSamlInitiateSingleSignOnRequestTests extends ESTestCase {

    public void testGetResponseForRegisteredSp() throws Exception {
        final SamlInitiateSingleSignOnRequest request = new SamlInitiateSingleSignOnRequest();
        request.setSpEntityId("https://sp.some.org");

        final PlainActionFuture<SamlInitiateSingleSignOnResponse> future = new PlainActionFuture<>();
        final TransportSamlInitiateSingleSignOnAction action = setupTransportAction(true);
        action.doExecute(mock(Task.class), request, future);

        final SamlInitiateSingleSignOnResponse response = future.get();
        assertThat(response.getSpEntityId(), equalTo("https://sp.some.org"));
        assertThat(response.getRedirectUrl(), equalTo("https://sp.some.org/api/security/v1/saml"));
        assertThat(response.getResponseBody(), containsString("saml_enduser"));
    }

    public void testGetResponseWithoutSecondaryAuthentication() throws Exception {
        final SamlInitiateSingleSignOnRequest request = new SamlInitiateSingleSignOnRequest();
        request.setSpEntityId("https://sp.some.org");

        final PlainActionFuture<SamlInitiateSingleSignOnResponse> future = new PlainActionFuture<>();
        final TransportSamlInitiateSingleSignOnAction action = setupTransportAction(false);
        action.doExecute(mock(Task.class), request, future);

        Exception e = expectThrows(Exception.class, () -> future.get());
        assertThat(e.getCause().getMessage(), containsString("Request is missing secondary authentication"));
    }

    public void testGetResponseForNotRegisteredSp() throws Exception {
        final SamlInitiateSingleSignOnRequest request = new SamlInitiateSingleSignOnRequest();
        request.setSpEntityId("https://sp2.other.org");

        final PlainActionFuture<SamlInitiateSingleSignOnResponse> future = new PlainActionFuture<>();
        final TransportSamlInitiateSingleSignOnAction action = setupTransportAction(true);
        action.doExecute(mock(Task.class), request, future);

        Exception e = expectThrows(Exception.class, () -> future.get());
        assertThat(e.getCause().getMessage(), containsString("https://sp2.other.org"));
        assertThat(e.getCause().getMessage(), containsString("is not registered with this Identity Provider"));
    }

    private TransportSamlInitiateSingleSignOnAction setupTransportAction(boolean withSecondaryAuth) throws Exception {
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.idp.enabled", true)
            .put("xpack.idp.entity_id", "https://idp.cloud.elastic.co")
            .put("xpack.idp.sso_endpoint.redirect", "https://idp.cloud.elastic.co/saml/init")
            .build();
        final ThreadContext threadContext = new ThreadContext(settings);
        final ThreadPool threadPool = mock(ThreadPool.class);
        final SecurityContext securityContext = new SecurityContext(settings, threadContext);
        final TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());
        final ActionFilters actionFilters = mock(ActionFilters.class);
        final Environment env = TestEnvironment.newEnvironment(settings);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        new Authentication(new User("saml_service_account", "saml_service_role"),
            new Authentication.RealmRef("default_native", "native", "node_name"),
            new Authentication.RealmRef("default_native", "native", "node_name"))
            .writeToContext(threadContext);
        if (withSecondaryAuth) {
            new SecondaryAuthentication(securityContext,
                new Authentication(new User("saml_enduser", "saml_enduser_role"),
                    new Authentication.RealmRef("_es_api_key", "_es_api_key", "node_name"),
                    new Authentication.RealmRef("_es_api_key", "_es_api_key", "node_name")))
                .writeToContext(threadContext);
        }
        return new TransportSamlInitiateSingleSignOnAction(transportService, securityContext, actionFilters, env);
    }
}
