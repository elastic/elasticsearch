/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.filter;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.MockIndicesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
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
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.junit.Before;

import java.util.Collections;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.INDICES_PERMISSIONS_KEY;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class SecurityActionFilterTests extends ESTestCase {
    private AuthenticationService authcService;
    private AuthorizationService authzService;
    private AuditTrailService auditTrailService;
    private AuditTrail auditTrail;
    private ActionFilterChain chain;
    private XPackLicenseState licenseState;
    private SecurityActionFilter filter;
    private ThreadContext threadContext;
    private boolean failDestructiveOperations;

    @Before
    public void init() throws Exception {
        authcService = mock(AuthenticationService.class);
        authzService = mock(AuthorizationService.class);
        auditTrailService = mock(AuditTrailService.class);
        auditTrail = mock(AuditTrail.class);
        when(auditTrailService.get()).thenReturn(auditTrail);
        chain = mock(ActionFilterChain.class);
        licenseState = mock(XPackLicenseState.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        failDestructiveOperations = randomBoolean();
        Settings settings = Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), failDestructiveOperations).build();
        DestructiveOperations destructiveOperations = new DestructiveOperations(
            settings,
            new ClusterSettings(settings, Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))
        );
        ClusterState state = mock(ClusterState.class);
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("id1", buildNewFakeTransportAddress(), Version.CURRENT))
            .add(new DiscoveryNode("id2", buildNewFakeTransportAddress(), Version.CURRENT.minimumCompatibilityVersion()))
            .build();
        when(state.nodes()).thenReturn(nodes);

        SecurityContext securityContext = new SecurityContext(settings, threadContext);
        filter = new SecurityActionFilter(
            authcService,
            authzService,
            auditTrailService,
            licenseState,
            threadPool,
            securityContext,
            destructiveOperations
        );
    }

    public void testApply() throws Exception {
        ActionRequest request = mock(ActionRequest.class);
        ActionListener listener = mock(ActionListener.class);
        Task task = mock(Task.class);
        User user = new User("username", "r1", "r2");
        Authentication authentication = new Authentication(user, new RealmRef("test", "test", "foo"), null);
        String requestId = UUIDs.randomBase64UUID();
        mockAuthentication(request, authentication, requestId);
        mockAuthorize();
        ActionResponse actionResponse = mock(ActionResponse.class);
        mockChain(task, "_action", request, actionResponse);
        filter.apply(task, "_action", request, listener, chain);
        verify(authzService).authorize(eq(authentication), eq("_action"), eq(request), anyActionListener());
        verify(auditTrail).coordinatingActionResponse(eq(requestId), eq(authentication), eq("_action"), eq(request), eq(actionResponse));
    }

    public void testApplyRestoresThreadContext() throws Exception {
        ActionRequest request = mock(ActionRequest.class);
        ActionListener listener = mock(ActionListener.class);
        Task task = mock(Task.class);
        User user = new User("username", "r1", "r2");
        Authentication authentication = new Authentication(user, new RealmRef("test", "test", "foo"), null);
        String requestId = UUIDs.randomBase64UUID();
        mockAuthentication(request, authentication, requestId);
        mockAuthorize();
        ActionResponse actionResponse = mock(ActionResponse.class);
        mockChain(task, "_action", request, actionResponse);
        assertNull(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
        assertNull(threadContext.getTransient(INDICES_PERMISSIONS_KEY));

        filter.apply(task, "_action", request, listener, chain);

        assertNull(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
        assertNull(threadContext.getTransient(INDICES_PERMISSIONS_KEY));
        verify(authzService).authorize(eq(authentication), eq("_action"), eq(request), anyActionListener());
        verify(auditTrail).coordinatingActionResponse(eq(requestId), eq(authentication), eq("_action"), eq(request), eq(actionResponse));
    }

    public void testApplyAsSystemUser() throws Exception {
        ActionRequest request = mock(ActionRequest.class);
        ActionListener listener = mock(ActionListener.class);
        User user = new User("username", "r1", "r2");
        Authentication authentication = new Authentication(user, new RealmRef("test", "test", "foo"), null);
        SetOnce<Authentication> authenticationSetOnce = new SetOnce<>();
        SetOnce<IndicesAccessControl> accessControlSetOnce = new SetOnce<>();
        SetOnce<String> requestIdOnActionHandler = new SetOnce<>();
        ActionFilterChain chain = (task, action, request1, listener1) -> {
            authenticationSetOnce.set(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
            accessControlSetOnce.set(threadContext.getTransient(INDICES_PERMISSIONS_KEY));
            requestIdOnActionHandler.set(AuditUtil.extractRequestId(threadContext));
        };
        Task task = mock(Task.class);
        final boolean hasExistingAuthentication = randomBoolean();
        final boolean hasExistingAccessControl = randomBoolean();
        final String action = "internal:foo";
        if (hasExistingAuthentication) {
            AuditUtil.generateRequestId(threadContext);
            threadContext.putTransient(AuthenticationField.AUTHENTICATION_KEY, authentication);
            threadContext.putHeader(AuthenticationField.AUTHENTICATION_KEY, "foo");
            threadContext.putTransient(AuthorizationServiceField.ORIGINATING_ACTION_KEY, "indices:foo");
            if (hasExistingAccessControl) {
                threadContext.putTransient(INDICES_PERMISSIONS_KEY, IndicesAccessControl.ALLOW_NO_INDICES);
            }
        } else {
            assertNull(AuditUtil.extractRequestId(threadContext));
            assertNull(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
        }
        SetOnce<String> requestIdFromAuthn = new SetOnce<>();
        doAnswer(i -> {
            final Object[] args = i.getArguments();
            assertThat(args, arrayWithSize(4));
            ActionListener callback = (ActionListener) args[args.length - 1];
            requestIdFromAuthn.set(AuditUtil.generateRequestId(threadContext));
            callback.onResponse(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
            return Void.TYPE;
        }).when(authcService).authenticate(eq(action), eq(request), eq(SystemUser.INSTANCE), anyActionListener());
        IndicesAccessControl authzAccessControl = mock(IndicesAccessControl.class);
        mockAuthorize(authzAccessControl);

        filter.apply(task, action, request, listener, chain);

        if (hasExistingAuthentication) {
            assertEquals(authentication, threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
            if (hasExistingAccessControl) {
                assertThat(threadContext.getTransient(INDICES_PERMISSIONS_KEY), sameInstance(IndicesAccessControl.ALLOW_NO_INDICES));
            }
        } else {
            assertNull(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
        }
        assertNotNull(authenticationSetOnce.get());
        assertNotEquals(authentication, authenticationSetOnce.get());
        assertEquals(SystemUser.INSTANCE, authenticationSetOnce.get().getUser());
        assertThat(accessControlSetOnce.get(), sameInstance(authzAccessControl));
        assertThat(requestIdOnActionHandler.get(), is(requestIdFromAuthn.get()));
    }

    public void testApplyDestructiveOperations() throws Exception {
        ActionRequest request = new MockIndicesRequest(
            IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()),
            randomFrom("*", "_all", "test*")
        );
        String action = randomFrom(CloseIndexAction.NAME, OpenIndexAction.NAME, DeleteIndexAction.NAME);
        ActionListener listener = mock(ActionListener.class);
        Task task = mock(Task.class);
        User user = new User("username", "r1", "r2");
        Authentication authentication = new Authentication(user, new RealmRef("test", "test", "foo"), null);
        ActionResponse actionResponse = mock(ActionResponse.class);
        mockChain(task, action, request, actionResponse);
        SetOnce<String> requestIdFromAuthn = new SetOnce<>();
        doAnswer(i -> {
            final Object[] args = i.getArguments();
            assertThat(args, arrayWithSize(4));
            ActionListener callback = (ActionListener) args[args.length - 1];
            requestIdFromAuthn.set(AuditUtil.generateRequestId(threadContext));
            threadContext.putTransient(AuthenticationField.AUTHENTICATION_KEY, authentication);
            threadContext.putHeader(AuthenticationField.AUTHENTICATION_KEY, authentication.encode());
            callback.onResponse(authentication);
            return Void.TYPE;
        }).when(authcService).authenticate(eq(action), eq(request), eq(SystemUser.INSTANCE), anyActionListener());
        doAnswer((i) -> {
            ActionListener<Void> callback = (ActionListener<Void>) i.getArguments()[3];
            callback.onResponse(null);
            return Void.TYPE;
        }).when(authzService).authorize(any(Authentication.class), any(String.class), any(TransportRequest.class), anyActionListener());
        filter.apply(task, action, request, listener, chain);
        if (failDestructiveOperations) {
            verify(listener).onFailure(isA(IllegalArgumentException.class));
            verifyNoMoreInteractions(authzService, chain, auditTrailService, auditTrail);
        } else {
            verify(authzService).authorize(eq(authentication), eq(action), eq(request), anyActionListener());
            verify(chain).proceed(eq(task), eq(action), eq(request), anyActionListener());
            verify(auditTrail).coordinatingActionResponse(
                eq(requestIdFromAuthn.get()),
                eq(authentication),
                eq(action),
                eq(request),
                eq(actionResponse)
            );
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
        doAnswer(i -> {
            final Object[] args = i.getArguments();
            assertThat(args, arrayWithSize(4));
            ActionListener callback = (ActionListener) args[args.length - 1];
            assertNull(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
            AuditUtil.generateRequestId(threadContext);
            callback.onResponse(authentication);
            return Void.TYPE;
        }).when(authcService).authenticate(eq("_action"), eq(request), eq(SystemUser.INSTANCE), anyActionListener());
        if (randomBoolean()) {
            doThrow(exception).when(authzService).authorize(eq(authentication), eq("_action"), eq(request), anyActionListener());
        } else {
            doAnswer((i) -> {
                ActionListener<Void> callback = (ActionListener<Void>) i.getArguments()[3];
                callback.onFailure(exception);
                return Void.TYPE;
            }).when(authzService).authorize(eq(authentication), eq("_action"), eq(request), anyActionListener());
        }
        filter.apply(task, "_action", request, listener, chain);
        verify(listener).onFailure(exception);
        verifyNoMoreInteractions(chain);
    }

    private void mockAuthentication(ActionRequest request, Authentication authentication, String requestId) {
        doAnswer(i -> {
            final Object[] args = i.getArguments();
            assertThat(args, arrayWithSize(4));
            ActionListener callback = (ActionListener) args[args.length - 1];
            assertNull(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
            threadContext.putTransient(AuthenticationField.AUTHENTICATION_KEY, authentication);
            threadContext.putHeader(AuthenticationField.AUTHENTICATION_KEY, authentication.encode());
            threadContext.putHeader("_xpack_audit_request_id", requestId);
            callback.onResponse(authentication);
            return Void.TYPE;
        }).when(authcService).authenticate(eq("_action"), eq(request), eq(SystemUser.INSTANCE), anyActionListener());
    }

    private void mockAuthorize() {
        mockAuthorize(IndicesAccessControl.ALLOW_NO_INDICES);
    }

    private void mockAuthorize(IndicesAccessControl indicesAccessControl) {
        doAnswer(i -> {
            final Object[] args = i.getArguments();
            assertThat(args, arrayWithSize(4));
            ActionListener callback = (ActionListener) args[args.length - 1];
            assertNull(threadContext.getTransient(INDICES_PERMISSIONS_KEY));
            threadContext.putTransient(INDICES_PERMISSIONS_KEY, indicesAccessControl);
            callback.onResponse(null);
            return Void.TYPE;
        }).when(authzService).authorize(any(Authentication.class), any(String.class), any(TransportRequest.class), anyActionListener());
    }

    private void mockChain(Task task, String action, ActionRequest request, ActionResponse actionResponse) {
        doAnswer(i -> {
            final Object[] args = i.getArguments();
            assertThat(args, arrayWithSize(4));
            ActionListener callback = (ActionListener) args[args.length - 1];
            callback.onResponse(actionResponse);
            return Void.TYPE;
        }).when(chain).proceed(eq(task), eq(action), eq(request), anyActionListener());
    }
}
