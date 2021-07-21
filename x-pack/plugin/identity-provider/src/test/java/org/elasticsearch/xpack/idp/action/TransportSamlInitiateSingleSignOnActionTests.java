/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.idp.privileges.ServiceProviderPrivileges;
import org.elasticsearch.xpack.idp.privileges.UserPrivilegeResolver;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.sp.CloudServiceProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderResolver;
import org.elasticsearch.xpack.idp.saml.sp.ServiceProviderDefaults;
import org.elasticsearch.xpack.idp.saml.sp.WildcardServiceProviderResolver;
import org.elasticsearch.xpack.idp.saml.support.SamlAuthenticationState;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;
import org.elasticsearch.xpack.idp.saml.test.IdpSamlTestCase;
import org.joda.time.Duration;
import org.mockito.Mockito;
import org.opensaml.saml.saml2.core.StatusCode;
import org.opensaml.security.x509.X509Credential;

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensaml.saml.saml2.core.NameIDType.TRANSIENT;

public class TransportSamlInitiateSingleSignOnActionTests extends IdpSamlTestCase {

    public void testGetResponseForRegisteredSp() throws Exception {
        final SamlInitiateSingleSignOnRequest request = new SamlInitiateSingleSignOnRequest();
        request.setSpEntityId("https://sp.some.org");
        request.setAssertionConsumerService("https://sp.some.org/api/security/v1/saml");
        final PlainActionFuture<SamlInitiateSingleSignOnResponse> future = new PlainActionFuture<>();
        final TransportSamlInitiateSingleSignOnAction action = setupTransportAction(true);
        action.doExecute(mock(Task.class), request, future);

        final SamlInitiateSingleSignOnResponse response = future.get();
        assertThat(response.getEntityId(), equalTo("https://sp.some.org"));
        assertThat(response.getPostUrl(), equalTo("https://sp.some.org/api/security/v1/saml"));
        assertThat(response.getSamlResponse(), containsString(TRANSIENT));
        assertContainsAttributeWithValue(response.getSamlResponse(), "email", "samlenduser@elastic.co");
        assertContainsAttributeWithValue(response.getSamlResponse(), "name", "Saml Enduser");
        assertContainsAttributeWithValue(response.getSamlResponse(), "principal", "saml_enduser");
        assertThat(response.getSamlResponse(), containsString("https://saml.elasticsearch.org/attributes/roles"));
    }

    public void testGetResponseWithoutSecondaryAuthenticationInIdpInitiated() throws Exception {
        final SamlInitiateSingleSignOnRequest request = new SamlInitiateSingleSignOnRequest();
        request.setSpEntityId("https://sp.some.org");
        request.setAssertionConsumerService("https://sp.some.org/api/security/v1/saml");
        final PlainActionFuture<SamlInitiateSingleSignOnResponse> future = new PlainActionFuture<>();
        final TransportSamlInitiateSingleSignOnAction action = setupTransportAction(false);
        action.doExecute(mock(Task.class), request, future);

        Exception e = expectThrows(Exception.class, () -> future.get());
        assertThat(e.getCause().getMessage(), containsString("Request is missing secondary authentication"));
    }

    public void testGetResponseForNotRegisteredSpInIdpInitiated() throws Exception {
        final SamlInitiateSingleSignOnRequest request = new SamlInitiateSingleSignOnRequest();
        request.setSpEntityId("https://sp2.other.org");
        request.setAssertionConsumerService("https://sp2.some.org/api/security/v1/saml");
        final PlainActionFuture<SamlInitiateSingleSignOnResponse> future = new PlainActionFuture<>();
        final TransportSamlInitiateSingleSignOnAction action = setupTransportAction(true);
        action.doExecute(mock(Task.class), request, future);

        Exception e = expectThrows(Exception.class, () -> future.get());
        assertThat(e.getCause().getMessage(), containsString("https://sp2.other.org"));
        assertThat(e.getCause().getMessage(), containsString("is not known to this Identity Provider"));
    }

    public void testGetResponseWithoutSecondaryAuthenticationInSpInitiatedFlow() throws Exception {
        final SamlInitiateSingleSignOnRequest request = new SamlInitiateSingleSignOnRequest();
        request.setSpEntityId("https://sp.some.org");
        request.setAssertionConsumerService("https://sp.some.org/saml/acs");
        final String requestId = randomAlphaOfLength(12);
        final SamlAuthenticationState samlAuthenticationState = new SamlAuthenticationState();
        samlAuthenticationState.setAuthnRequestId(requestId);
        request.setSamlAuthenticationState(samlAuthenticationState);

        final PlainActionFuture<SamlInitiateSingleSignOnResponse> future = new PlainActionFuture<>();
        final TransportSamlInitiateSingleSignOnAction action = setupTransportAction(false);
        action.doExecute(mock(Task.class), request, future);

        final SamlInitiateSingleSignOnResponse response = future.get();
        assertThat(response.getError(), equalTo("Request is missing secondary authentication"));
        assertThat(response.getSamlStatus(), equalTo(StatusCode.REQUESTER));
        assertThat(response.getPostUrl(), equalTo("https://sp.some.org/saml/acs"));
        assertThat(response.getEntityId(), equalTo("https://sp.some.org"));
        assertThat(response.getSamlResponse(), containsString("InResponseTo=\"" + requestId + "\""));
    }

    @SuppressWarnings("unchecked")
    private TransportSamlInitiateSingleSignOnAction setupTransportAction(boolean withSecondaryAuth) throws Exception {
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.idp.enabled", true)
            .put("xpack.idp.entity_id", "https://idp.cloud.elastic.co")
            .put("xpack.idp.sso_endpoint.redirect", "https://idp.cloud.elastic.co/saml/init")
            .put("xpack.idp.organization.url", "https://cloud.elastic.co")
            .put("xpack.idp.organization.name", "Cloud Elastic")
            .put("xpack.idp.contact.email", "some@other.org")
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
                new Authentication(
                    new User("saml_enduser", new String[]{"saml_enduser_role"}, "Saml Enduser", "samlenduser@elastic.co",
                        new HashMap<>(), true),
                    new Authentication.RealmRef("_es_api_key", "_es_api_key", "node_name"),
                    new Authentication.RealmRef("_es_api_key", "_es_api_key", "node_name")))
                .writeToContext(threadContext);
        }

        final SamlServiceProviderResolver serviceResolver = Mockito.mock(SamlServiceProviderResolver.class);
        final WildcardServiceProviderResolver wildcardResolver = Mockito.mock(WildcardServiceProviderResolver.class);
        final CloudServiceProvider serviceProvider = new CloudServiceProvider("https://sp.some.org",
            "test sp",
            true,
            new URL("https://sp.some.org/api/security/v1/saml"),
            TRANSIENT,
            Duration.standardMinutes(5),
            null,
            new SamlServiceProvider.AttributeNames(
                "https://saml.elasticsearch.org/attributes/principal",
                "https://saml.elasticsearch.org/attributes/name",
                "https://saml.elasticsearch.org/attributes/email",
                "https://saml.elasticsearch.org/attributes/roles"),
            null, false, false);
        mockRegisteredServiceProvider(serviceResolver, "https://sp.some.org", serviceProvider);
        mockRegisteredServiceProvider(serviceResolver, "https://sp2.other.org", null);
        final ServiceProviderDefaults defaults = new ServiceProviderDefaults(
            "elastic-cloud", TRANSIENT, Duration.standardMinutes(15));
        final X509Credential signingCredential = readCredentials("RSA", randomFrom(1024, 2048, 4096));
        final SamlIdentityProvider idp = SamlIdentityProvider
            .builder(serviceResolver, wildcardResolver)
            .fromSettings(env)
            .signingCredential(signingCredential)
            .serviceProviderDefaults(defaults)
            .build();
        final SamlFactory factory = new SamlFactory();
        final UserPrivilegeResolver privilegeResolver = Mockito.mock(UserPrivilegeResolver.class);
        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            assertThat(args, arrayWithSize(2));
            ActionListener<UserPrivilegeResolver.UserPrivileges> listener
                = (ActionListener<UserPrivilegeResolver.UserPrivileges>) args[args.length - 1];
            final UserPrivilegeResolver.UserPrivileges privileges = new UserPrivilegeResolver.UserPrivileges(
                "saml_enduser", true,
                new HashSet<>(Arrays.asList(generateRandomStringArray(5, 8, false, false))
                ));
            listener.onResponse(privileges);
            return null;
        }).when(privilegeResolver).resolve(any(ServiceProviderPrivileges.class), any(ActionListener.class));
        return new TransportSamlInitiateSingleSignOnAction(transportService, actionFilters, securityContext,
            idp, factory, privilegeResolver);
    }

    private void assertContainsAttributeWithValue(String message, String attribute, String value) {
        assertThat(message, containsString("<saml2:Attribute FriendlyName=\"" + attribute + "\" Name=\"https://saml.elasticsearch" +
            ".org/attributes/" + attribute + "\" NameFormat=\"urn:oasis:names:tc:SAML:2.0:attrname-format:uri\"><saml2:AttributeValue " +
            "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xsd:string\">" + value + "</saml2:AttributeValue></saml2" +
            ":Attribute>"));
    }
}
