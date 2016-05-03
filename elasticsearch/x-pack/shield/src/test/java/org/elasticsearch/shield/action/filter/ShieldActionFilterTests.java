/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.filter;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.shield.action.ShieldActionMapper;
import org.elasticsearch.shield.user.SystemUser;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.shield.authz.AuthorizationService;
import org.elasticsearch.shield.crypto.CryptoService;
import org.elasticsearch.shield.SecurityLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.HashSet;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 *
 */
public class ShieldActionFilterTests extends ESTestCase {
    private AuthenticationService authcService;
    private AuthorizationService authzService;
    private CryptoService cryptoService;
    private AuditTrail auditTrail;
    private SecurityLicenseState securityLicenseState;
    private ShieldActionFilter filter;

    @Before
    public void init() throws Exception {
        authcService = mock(AuthenticationService.class);
        authzService = mock(AuthorizationService.class);
        cryptoService = mock(CryptoService.class);
        auditTrail = mock(AuditTrail.class);
        securityLicenseState = mock(SecurityLicenseState.class);
        when(securityLicenseState.authenticationAndAuthorizationEnabled()).thenReturn(true);
        when(securityLicenseState.statsAndHealthEnabled()).thenReturn(true);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        filter = new ShieldActionFilter(Settings.EMPTY, authcService, authzService, cryptoService, auditTrail, securityLicenseState,
                new ShieldActionMapper(), new HashSet<>(), threadPool);
    }

    public void testApply() throws Exception {
        ActionRequest request = mock(ActionRequest.class);
        ActionListener listener = mock(ActionListener.class);
        ActionFilterChain chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        User user = new User("username", "r1", "r2");
        when(authcService.authenticate("_action", request, SystemUser.INSTANCE)).thenReturn(user);
        doReturn(request).when(spy(filter)).unsign(user, "_action", request);
        filter.apply(task, "_action", request, listener, chain);
        verify(authzService).authorize(user, "_action", request);
        verify(chain).proceed(eq(task), eq("_action"), eq(request), isA(ShieldActionFilter.SigningListener.class));
    }

    public void testActionProcessException() throws Exception {
        ActionRequest request = mock(ActionRequest.class);
        ActionListener listener = mock(ActionListener.class);
        ActionFilterChain chain = mock(ActionFilterChain.class);
        RuntimeException exception = new RuntimeException("process-error");
        Task task = mock(Task.class);
        User user = new User("username", "r1", "r2");
        when(authcService.authenticate("_action", request, SystemUser.INSTANCE)).thenReturn(user);
        doThrow(exception).when(authzService).authorize(user, "_action", request);
        filter.apply(task, "_action", request, listener, chain);
        verify(listener).onFailure(exception);
        verifyNoMoreInteractions(chain);
    }

    public void testActionSignature() throws Exception {
        SearchScrollRequest request = new SearchScrollRequest("signed_scroll_id");
        ActionListener listener = mock(ActionListener.class);
        ActionFilterChain chain = mock(ActionFilterChain.class);
        User user = mock(User.class);
        Task task = mock(Task.class);
        when(authcService.authenticate("_action", request, SystemUser.INSTANCE)).thenReturn(user);
        when(cryptoService.signed("signed_scroll_id")).thenReturn(true);
        when(cryptoService.unsignAndVerify("signed_scroll_id")).thenReturn("scroll_id");
        filter.apply(task, "_action", request, listener, chain);
        assertThat(request.scrollId(), equalTo("scroll_id"));
        verify(authzService).authorize(user, "_action", request);
        verify(chain).proceed(eq(task), eq("_action"), eq(request), isA(ShieldActionFilter.SigningListener.class));
    }

    public void testActionSignatureError() throws Exception {
        SearchScrollRequest request = new SearchScrollRequest("scroll_id");
        ActionListener listener = mock(ActionListener.class);
        ActionFilterChain chain = mock(ActionFilterChain.class);
        IllegalArgumentException sigException = new IllegalArgumentException("bad bad boy");
        User user = mock(User.class);
        Task task = mock(Task.class);
        when(authcService.authenticate("_action", request, SystemUser.INSTANCE)).thenReturn(user);
        when(cryptoService.signed("scroll_id")).thenReturn(true);
        doThrow(sigException).when(cryptoService).unsignAndVerify("scroll_id");
        filter.apply(task, "_action", request, listener, chain);
        verify(listener).onFailure(isA(ElasticsearchSecurityException.class));
        verify(auditTrail).tamperedRequest(user, "_action", request);
        verifyNoMoreInteractions(chain);
    }

    public void testApplyUnlicensed() throws Exception {
        ActionRequest request = mock(ActionRequest.class);
        ActionListener listener = mock(ActionListener.class);
        ActionFilterChain chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        when(securityLicenseState.authenticationAndAuthorizationEnabled()).thenReturn(false);
        filter.apply(task, "_action", request, listener, chain);
        verifyZeroInteractions(authcService);
        verifyZeroInteractions(authzService);
        verify(chain).proceed(eq(task), eq("_action"), eq(request), eq(listener));
    }

}
