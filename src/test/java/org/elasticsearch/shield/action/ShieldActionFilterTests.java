/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.shield.authz.AuthorizationException;
import org.elasticsearch.shield.authz.AuthorizationService;
import org.elasticsearch.shield.license.LicenseEventsNotifier;
import org.elasticsearch.shield.signature.SignatureException;
import org.elasticsearch.shield.signature.SignatureService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;

/**
 *
 */
public class ShieldActionFilterTests extends ElasticsearchTestCase {

    private AuthenticationService authcService;
    private AuthorizationService authzService;
    private SignatureService signatureService;
    private AuditTrail auditTrail;
    private LicenseEventsNotifier licenseEventsNotifier;
    private ShieldActionFilter filter;

    @Before
    public void init() throws Exception {
        authcService = mock(AuthenticationService.class);
        authzService = mock(AuthorizationService.class);
        signatureService = mock(SignatureService.class);
        auditTrail = mock(AuditTrail.class);
        licenseEventsNotifier = new MockLicenseEventsNotifier();
        filter = new ShieldActionFilter(ImmutableSettings.EMPTY, authcService, authzService, signatureService, auditTrail, licenseEventsNotifier, new ShieldActionMapper());
    }

    @Test
    public void testApply() throws Exception {
        ActionRequest request = mock(ActionRequest.class);
        ActionListener listener = mock(ActionListener.class);
        ActionFilterChain chain = mock(ActionFilterChain.class);
        User user = new User.Simple("username", "r1", "r2");
        when(authcService.authenticate("_action", request, User.SYSTEM)).thenReturn(user);
        doReturn(request).when(spy(filter)).unsign(user, "_action", request);
        filter.apply("_action", request, listener, chain);
        verify(authzService).authorize(user, "_action", request);
        verify(chain).proceed(eq("_action"), eq(request), isA(ShieldActionFilter.SigningListener.class));
    }

    @Test
    public void testAction_Process_Exception() throws Exception {
        ActionRequest request = mock(ActionRequest.class);
        ActionListener listener = mock(ActionListener.class);
        ActionFilterChain chain = mock(ActionFilterChain.class);
        RuntimeException exception = new RuntimeException("process-error");
        User user = new User.Simple("username", "r1", "r2");
        when(authcService.authenticate("_action", request, User.SYSTEM)).thenReturn(user);
        doThrow(exception).when(authzService).authorize(user, "_action", request);
        filter.apply("_action", request, listener, chain);
        verify(listener).onFailure(exception);
        verifyNoMoreInteractions(chain);
    }

    @Test
    public void testAction_Signature() throws Exception {
        SearchScrollRequest request = new SearchScrollRequest("signed_scroll_id");
        ActionListener listener = mock(ActionListener.class);
        ActionFilterChain chain = mock(ActionFilterChain.class);
        User user = mock(User.class);
        when(authcService.authenticate("_action", request, User.SYSTEM)).thenReturn(user);
        when(signatureService.signed("signed_scroll_id")).thenReturn(true);
        when(signatureService.unsignAndVerify("signed_scroll_id")).thenReturn("scroll_id");
        filter.apply("_action", request, listener, chain);
        assertThat(request.scrollId(), equalTo("scroll_id"));
        verify(authzService).authorize(user, "_action", request);
        verify(chain).proceed(eq("_action"), eq(request), isA(ShieldActionFilter.SigningListener.class));
    }

    @Test
    public void testAction_SignatureError() throws Exception {
        SearchScrollRequest request = new SearchScrollRequest("scroll_id");
        ActionListener listener = mock(ActionListener.class);
        ActionFilterChain chain = mock(ActionFilterChain.class);
        SignatureException sigException = new SignatureException("bad bad boy");
        User user = mock(User.class);
        when(authcService.authenticate("_action", request, User.SYSTEM)).thenReturn(user);
        when(signatureService.signed("scroll_id")).thenReturn(true);
        doThrow(sigException).when(signatureService).unsignAndVerify("scroll_id");
        filter.apply("_action", request, listener, chain);
        verify(listener).onFailure(isA(AuthorizationException.class));
        verify(auditTrail).tamperedRequest(user, "_action", request);
        verifyNoMoreInteractions(chain);
    }

    private class MockLicenseEventsNotifier extends LicenseEventsNotifier {
        @Override
        public void register(MockLicenseEventsNotifier.Listener listener) {
            listener.enabled();
        }
    }
}
