/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest;

import com.nimbusds.jose.util.StandardCharset;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.license.License;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.support.SecondaryAuthenticator;
import org.junit.Before;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SecurityRestFilterTests extends ESTestCase {

    private ThreadContext threadContext;
    private AuthenticationService authcService;
    private SecondaryAuthenticator secondaryAuthenticator;
    private RestChannel channel;
    private SecurityRestFilter filter;
    private XPackLicenseState licenseState;
    private RestHandler restHandler;

    @Before
    public void init() throws Exception {
        authcService = mock(AuthenticationService.class);
        channel = mock(RestChannel.class);
        when(channel.newErrorBuilder()).thenReturn(JsonXContent.contentBuilder());
        licenseState = mock(XPackLicenseState.class);
        when(licenseState.isSecurityEnabled()).thenReturn(true);
        restHandler = mock(RestHandler.class);
        threadContext = new ThreadContext(Settings.EMPTY);
        secondaryAuthenticator = new SecondaryAuthenticator(
            Settings.EMPTY,
            threadContext,
            authcService,
            new AuditTrailService(Arrays.asList(mock(AuditTrail.class)), licenseState)
        );
        filter = new SecurityRestFilter(
            licenseState,
            threadContext,
            secondaryAuthenticator,
            new AuditTrailService(Arrays.asList(mock(AuditTrail.class)), licenseState),
            restHandler
        );
    }

    public void testProcess() throws Exception {
        RestRequest request = new FakeRestRequest();
        when(channel.request()).thenReturn(request);
        Authentication authentication = mock(Authentication.class);
        doAnswer((i) -> {
            @SuppressWarnings("unchecked")
            ActionListener<Authentication> callback = (ActionListener<Authentication>) i.getArguments()[1];
            callback.onResponse(authentication);
            return Void.TYPE;
        }).when(authcService).authenticate(eq(request.getHttpRequest()), anyActionListener());
        filter.handleRequest(request, channel, null);
        verify(restHandler).handleRequest(request, channel, null);
        verifyNoMoreInteractions(channel);
    }

    public void testProcessSecondaryAuthentication() throws Exception {
        RestRequest request = mock(RestRequest.class);
        when(channel.request()).thenReturn(request);
        when(request.getHttpChannel()).thenReturn(mock(HttpChannel.class));
        HttpRequest httpRequest = mock(HttpRequest.class);
        when(request.getHttpRequest()).thenReturn(httpRequest);

        Authentication primaryAuthentication = mock(Authentication.class);
        when(primaryAuthentication.encode()).thenReturn(randomAlphaOfLengthBetween(12, 36));
        doAnswer(i -> {
            final Object[] arguments = i.getArguments();
            @SuppressWarnings("unchecked")
            ActionListener<Authentication> callback = (ActionListener<Authentication>) arguments[arguments.length - 1];
            callback.onResponse(primaryAuthentication);
            return null;
        }).when(authcService).authenticate(eq(httpRequest), anyActionListener());

        Authentication secondaryAuthentication = mock(Authentication.class);
        when(secondaryAuthentication.encode()).thenReturn(randomAlphaOfLengthBetween(12, 36));
        doAnswer(i -> {
            final Object[] arguments = i.getArguments();
            @SuppressWarnings("unchecked")
            ActionListener<Authentication> callback = (ActionListener<Authentication>) arguments[arguments.length - 1];
            callback.onResponse(secondaryAuthentication);
            return null;
        }).when(authcService).authenticate(eq(httpRequest), eq(false), anyActionListener());

        SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        AtomicReference<SecondaryAuthentication> secondaryAuthRef = new AtomicReference<>();
        doAnswer(i -> {
            secondaryAuthRef.set(securityContext.getSecondaryAuthentication());
            return null;
        }).when(restHandler).handleRequest(request, channel, null);

        final String credentials = randomAlphaOfLengthBetween(4, 8) + ":" + randomAlphaOfLengthBetween(4, 12);
        threadContext.putHeader(
            SecondaryAuthenticator.SECONDARY_AUTH_HEADER_NAME,
            "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharset.UTF_8))
        );
        filter.handleRequest(request, channel, null);
        verify(restHandler).handleRequest(request, channel, null);
        verifyNoMoreInteractions(channel);

        assertThat(secondaryAuthRef.get(), notNullValue());
        assertThat(secondaryAuthRef.get().getAuthentication(), sameInstance(secondaryAuthentication));
    }

    public void testProcessBasicLicense() throws Exception {
        RestRequest request = mock(RestRequest.class);
        when(licenseState.isSecurityEnabled()).thenReturn(false);
        filter.handleRequest(request, channel, null);
        assertWarnings(
            "Elasticsearch built-in security features are not enabled. Without authentication, your cluster "
                + "could be accessible to anyone. See https://www.elastic.co/guide/en/elasticsearch/reference/"
                + Version.CURRENT.major
                + "."
                + Version.CURRENT.minor
                + "/security-minimal-setup.html to enable security."
        );
        verify(restHandler).handleRequest(request, channel, null);
        verifyNoMoreInteractions(channel, authcService);
    }

    public void testProcessOptionsMethod() throws Exception {
        RestRequest request = mock(RestRequest.class);
        when(request.method()).thenReturn(RestRequest.Method.OPTIONS);
        filter.handleRequest(request, channel, null);
        verify(restHandler).handleRequest(request, channel, null);
        verifyNoMoreInteractions(channel);
        verifyNoMoreInteractions(authcService);
    }

    public void testProcessFiltersBodyCorrectly() throws Exception {
        FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(
            new BytesArray("{\"password\": \"" + SecuritySettingsSourceField.TEST_PASSWORD + "\", \"foo\": \"bar\"}"),
            XContentType.JSON
        ).build();
        when(channel.request()).thenReturn(restRequest);
        SetOnce<RestRequest> handlerRequest = new SetOnce<>();
        restHandler = new FilteredRestHandler() {
            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                handlerRequest.set(request);
            }

            @Override
            public Set<String> getFilteredFields() {
                return Collections.singleton("password");
            }
        };
        AuditTrail auditTrail = mock(AuditTrail.class);
        SetOnce<RestRequest> auditTrailRequest = new SetOnce<>();
        doAnswer((i) -> {
            auditTrailRequest.set((RestRequest) i.getArguments()[0]);
            return Void.TYPE;
        }).when(auditTrail).authenticationSuccess(any(RestRequest.class));
        TestUtils.UpdatableLicenseState licenseState = new TestUtils.UpdatableLicenseState();
        licenseState.update(License.OperationMode.GOLD, true, null);
        filter = new SecurityRestFilter(
            licenseState,
            threadContext,
            secondaryAuthenticator,
            new AuditTrailService(Arrays.asList(auditTrail), licenseState),
            restHandler
        );

        filter.handleRequest(restRequest, channel, null);

        assertEquals(restRequest, handlerRequest.get());
        assertEquals(restRequest.content(), handlerRequest.get().content());
        Map<String, Object> original = XContentType.JSON.xContent()
            .createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                handlerRequest.get().content().streamInput()
            )
            .map();
        assertEquals(2, original.size());
        assertEquals(SecuritySettingsSourceField.TEST_PASSWORD, original.get("password"));
        assertEquals("bar", original.get("foo"));

        assertNotEquals(restRequest, auditTrailRequest.get());
        assertNotEquals(restRequest.content(), auditTrailRequest.get().content());

        Map<String, Object> map = XContentType.JSON.xContent()
            .createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                auditTrailRequest.get().content().streamInput()
            )
            .map();
        assertEquals(1, map.size());
        assertEquals("bar", map.get("foo"));
    }

    private interface FilteredRestHandler extends RestHandler, RestRequestFilter {}
}
