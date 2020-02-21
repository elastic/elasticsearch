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
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportSamlInitiateSingleSignOnRequestTests extends ESTestCase {

    private TransportSamlInitiateSingleSignOnAction action;

    @Before
    public void setup() throws Exception {
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.idp.enabled", true)
            .put("xpack.idp.entity_id", "https://idp.cloud.elastic.co")
            .put("xpack.idp.sso_endpoint.redirect", "https://idp.cloud.elastic.co/saml/init")
            .build();
        final ThreadContext threadContext = new ThreadContext(settings);
        final ThreadPool threadPool = mock(ThreadPool.class);
        final TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());
        final ActionFilters actionFilters = mock(ActionFilters.class);
        final Environment env = TestEnvironment.newEnvironment(settings);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        new Authentication(new User("test_saml_user", "saml_idp_role"),
            new Authentication.RealmRef("_es_api_key", "_es_api_key", "node_name"),
            new Authentication.RealmRef("_es_api_key", "_es_api_key", "node_name"))
            .writeToContext(threadContext);
        action = new TransportSamlInitiateSingleSignOnAction(threadPool, transportService, actionFilters, env);
    }

    public void testGetResponseForRegisteredSp() throws Exception {
        final SamlInitiateSingleSignOnRequest request = new SamlInitiateSingleSignOnRequest();
        request.setSpEntityId("https://sp.some.org");

        final PlainActionFuture<SamlInitiateSingleSignOnResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future);

        final SamlInitiateSingleSignOnResponse response = future.get();
        assertThat(response.getSpEntityId(), equalTo("https://sp.some.org"));
        assertThat(response.getRedirectUrl(), equalTo("https://sp.some.org/api/security/v1/saml"));
        assertThat(response.getResponseBody(), containsString("test_saml_user"));
    }

    public void testGetResponseForNotRegisteredSp() throws Exception {
        final SamlInitiateSingleSignOnRequest request = new SamlInitiateSingleSignOnRequest();
        request.setSpEntityId("https://sp2.other.org");

        final PlainActionFuture<SamlInitiateSingleSignOnResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future);

        Exception e = expectThrows(Exception.class, () -> future.get());
        assertThat(e.getCause().getMessage(), containsString("https://sp2.other.org"));
        assertThat(e.getCause().getMessage(), containsString("is not registered with this Identity Provider"));
    }
}
