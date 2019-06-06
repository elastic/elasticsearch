/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.filter;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.MockIndicesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.junit.Before;

import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class SecurityActionFilterTests extends ESTestCase {
    private AuthenticationService authcService;
    private AuthorizationService authzService;
    private XPackLicenseState licenseState;
    private SecurityActionFilter filter;
    private ThreadContext threadContext;
    private boolean failDestructiveOperations;

    @Before
    public void init() throws Exception {
        authcService = mock(AuthenticationService.class);
        authzService = mock(AuthorizationService.class);
        licenseState = mock(XPackLicenseState.class);
        when(licenseState.isAuthAllowed()).thenReturn(true);
        when(licenseState.isStatsAndHealthAllowed()).thenReturn(true);
        ThreadPool threadPool = mock(ThreadPool.class);
        threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        failDestructiveOperations = randomBoolean();
        Settings settings = Settings.builder()
                .put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), failDestructiveOperations).build();
        DestructiveOperations destructiveOperations = new DestructiveOperations(settings,
                new ClusterSettings(settings, Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING)));
        ClusterState state = mock(ClusterState.class);
        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("id1", buildNewFakeTransportAddress(), Version.CURRENT))
                .add(new DiscoveryNode("id2", buildNewFakeTransportAddress(), Version.CURRENT.minimumCompatibilityVersion()))
                .build();
        when(state.nodes()).thenReturn(nodes);

        SecurityContext securityContext = new SecurityContext(settings, threadContext);
        filter = new SecurityActionFilter(authcService, authzService, licenseState, threadPool, securityContext, destructiveOperations);
    }

    public void testApply() throws Exception {
        ActionRequest request = mock(ActionRequest.class);
        ActionListener listener = mock(ActionListener.class);
        ActionFilterChain chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        User user = new User("username", "r1", "r2");
        Authentication authentication = new Authentication(user, new RealmRef("test", "test", "foo"), null);
        doAnswer((i) -> {
            ActionListener callback =
                    (ActionListener) i.getArguments()[3];
            callback.onResponse(authentication);
            return Void.TYPE;
        }).when(authcService).authenticate(eq("_action"), eq(request), eq(SystemUser.INSTANCE), any(ActionListener.class));
        doAnswer((i) -> {
            ActionListener<Void> callback = (ActionListener<Void>) i.getArguments()[3];
            callback.onResponse(null);
            return Void.TYPE;
        }).when(authzService)
            .authorize(any(Authentication.class), any(String.class), any(TransportRequest.class), any(ActionListener.class));
        filter.apply(task, "_action", request, listener, chain);
        verify(authzService).authorize(eq(authentication), eq("_action"), eq(request), any(ActionListener.class));
        verify(chain).proceed(eq(task), eq("_action"), eq(request), isA(ContextPreservingActionListener.class));
    }

    public void testApplyRestoresThreadContext() throws Exception {
        ActionRequest request = mock(ActionRequest.class);
        ActionListener listener = mock(ActionListener.class);
        ActionFilterChain chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        User user = new User("username", "r1", "r2");
        Authentication authentication = new Authentication(user, new RealmRef("test", "test", "foo"), null);
        doAnswer((i) -> {
            ActionListener callback =
                    (ActionListener) i.getArguments()[3];
            assertNull(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
            threadContext.putTransient(AuthenticationField.AUTHENTICATION_KEY, authentication);
            callback.onResponse(authentication);
            return Void.TYPE;
        }).when(authcService).authenticate(eq("_action"), eq(request), eq(SystemUser.INSTANCE), any(ActionListener.class));
        doAnswer((i) -> {
            ActionListener<Void> callback = (ActionListener<Void>) i.getArguments()[3];
            callback.onResponse(null);
            return Void.TYPE;
        }).when(authzService)
            .authorize(any(Authentication.class), any(String.class), any(TransportRequest.class), any(ActionListener.class));
        assertNull(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));

        filter.apply(task, "_action", request, listener, chain);

        assertNull(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
        verify(authzService).authorize(eq(authentication), eq("_action"), eq(request), any(ActionListener.class));
        verify(chain).proceed(eq(task), eq("_action"), eq(request), isA(ContextPreservingActionListener.class));
    }

    public void testApplyAsSystemUser() throws Exception {
        ActionRequest request = mock(ActionRequest.class);
        ActionListener listener = mock(ActionListener.class);
        User user = new User("username", "r1", "r2");
        Authentication authentication = new Authentication(user, new RealmRef("test", "test", "foo"), null);
        SetOnce<Authentication> authenticationSetOnce = new SetOnce<>();
        ActionFilterChain chain = (task, action, request1, listener1) -> {
            authenticationSetOnce.set(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
        };
        Task task = mock(Task.class);
        final boolean hasExistingAuthentication = randomBoolean();
        final String action = "internal:foo";
        if (hasExistingAuthentication) {
            threadContext.putTransient(AuthenticationField.AUTHENTICATION_KEY, authentication);
            threadContext.putHeader(AuthenticationField.AUTHENTICATION_KEY, "foo");
            threadContext.putTransient(AuthorizationService.ORIGINATING_ACTION_KEY, "indices:foo");
        } else {
            assertNull(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
        }
        doAnswer((i) -> {
            ActionListener callback =
                    (ActionListener) i.getArguments()[3];
            callback.onResponse(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
            return Void.TYPE;
        }).when(authcService).authenticate(eq(action), eq(request), eq(SystemUser.INSTANCE), any(ActionListener.class));
        doAnswer((i) -> {
            ActionListener<Void> callback = (ActionListener<Void>) i.getArguments()[3];
            callback.onResponse(null);
            return Void.TYPE;
        }).when(authzService)
            .authorize(any(Authentication.class), any(String.class), any(TransportRequest.class), any(ActionListener.class));

        filter.apply(task, action, request, listener, chain);

        if (hasExistingAuthentication) {
            assertEquals(authentication, threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
        } else {
            assertNull(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
        }
        assertNotNull(authenticationSetOnce.get());
        assertNotEquals(authentication, authenticationSetOnce.get());
        assertEquals(SystemUser.INSTANCE, authenticationSetOnce.get().getUser());
    }

    public void testApplyDestructiveOperations() throws Exception {
        ActionRequest request = new MockIndicesRequest(
                IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()),
                randomFrom("*", "_all", "test*"));
        String action = randomFrom(CloseIndexAction.NAME, OpenIndexAction.NAME, DeleteIndexAction.NAME);
        ActionListener listener = mock(ActionListener.class);
        ActionFilterChain chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        User user = new User("username", "r1", "r2");
        Authentication authentication = new Authentication(user, new RealmRef("test", "test", "foo"), null);
        doAnswer((i) -> {
            ActionListener callback =
                    (ActionListener) i.getArguments()[3];
            callback.onResponse(authentication);
            return Void.TYPE;
        }).when(authcService).authenticate(eq(action), eq(request), eq(SystemUser.INSTANCE), any(ActionListener.class));
        doAnswer((i) -> {
            ActionListener<Void> callback = (ActionListener<Void>) i.getArguments()[3];
            callback.onResponse(null);
            return Void.TYPE;
        }).when(authzService)
            .authorize(any(Authentication.class), any(String.class), any(TransportRequest.class), any(ActionListener.class));
        filter.apply(task, action, request, listener, chain);
        if (failDestructiveOperations) {
            verify(listener).onFailure(isA(IllegalArgumentException.class));
            verifyNoMoreInteractions(authzService, chain);
        } else {
            verify(authzService).authorize(eq(authentication), eq(action), eq(request), any(ActionListener.class));
            verify(chain).proceed(eq(task), eq(action), eq(request), isA(ContextPreservingActionListener.class));
        }
    }

    public void testActionProcessException() throws Exception {
        ActionRequest request = mock(ActionRequest.class);
        ActionListener listener = mock(ActionListener.class);
        ActionFilterChain chain = mock(ActionFilterChain.class);
        RuntimeException exception = new RuntimeException("process-error");
        Task task = mock(Task.class);
        User user = new User("username", "r1", "r2");
        Authentication authentication = new Authentication(user, new RealmRef("test", "test", "foo"), null);
        doAnswer((i) -> {
            ActionListener callback =
                    (ActionListener) i.getArguments()[3];
            callback.onResponse(authentication);
            return Void.TYPE;
        }).when(authcService).authenticate(eq("_action"), eq(request), eq(SystemUser.INSTANCE), any(ActionListener.class));
        doThrow(exception).when(authzService).authorize(eq(authentication), eq("_action"), eq(request), any(ActionListener.class));
        filter.apply(task, "_action", request, listener, chain);
        verify(listener).onFailure(exception);
        verifyNoMoreInteractions(chain);
    }

    public void testApplyUnlicensed() throws Exception {
        ActionRequest request = mock(ActionRequest.class);
        ActionListener listener = mock(ActionListener.class);
        ActionFilterChain chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        when(licenseState.isAuthAllowed()).thenReturn(false);
        filter.apply(task, "_action", request, listener, chain);
        verifyZeroInteractions(authcService);
        verifyZeroInteractions(authzService);
        verify(chain).proceed(eq(task), eq("_action"), eq(request), eq(listener));
    }
}
