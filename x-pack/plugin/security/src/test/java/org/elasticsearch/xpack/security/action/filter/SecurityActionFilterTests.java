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
import org.elasticsearch.action.admin.indices.close.TransportCloseIndexAction;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.Set;

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
import static org.mockito.Mockito.verifyNoInteractions;
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
            .add(DiscoveryNodeUtils.create("id1"))
            .add(
                DiscoveryNodeUtils.builder("id2")
                    .version(Version.CURRENT.minimumCompatibilityVersion(), IndexVersions.MINIMUM_COMPATIBLE, IndexVersion.current())
                    .build()
            )
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
            destructiveOperations,
            () -> Set.of("_action_secondary_auth")
        );
    }

    public void testApply() throws Exception {
        ActionRequest request = mock(ActionRequest.class);
        ActionListener listener = mock(ActionListener.class);
        Task task = mock(Task.class);
        User user = new User("username", "r1", "r2");
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(user)
            .realmRef(new RealmRef("test", "test", "foo"))
            .build(false);
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
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(user)
            .realmRef(new RealmRef("test", "test", "foo"))
            .build(false);
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
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(user)
            .realmRef(new RealmRef("test", "test", "foo"))
            .build(false);
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
                new SecurityContext(Settings.EMPTY, threadContext).putIndicesAccessControl(IndicesAccessControl.ALLOW_NO_INDICES);
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
        }).when(authcService).authenticate(eq(action), eq(request), eq(InternalUsers.SYSTEM_USER), anyActionListener());
        IndicesAccessControl authzAccessControl = mock(IndicesAccessControl.class);
        when(authzAccessControl.isGranted()).thenReturn(true);
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
        assertEquals(InternalUsers.SYSTEM_USER, authenticationSetOnce.get().getEffectiveSubject().getUser());
        assertThat(accessControlSetOnce.get(), sameInstance(authzAccessControl));
        assertThat(requestIdOnActionHandler.get(), is(requestIdFromAuthn.get()));
    }

    public void testApplyDestructiveOperations() throws Exception {
        ActionRequest request = new MockIndicesRequest(
            IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()),
            randomFrom("*", "_all", "test*")
        );
        String action = randomFrom(TransportCloseIndexAction.NAME, OpenIndexAction.NAME, TransportDeleteIndexAction.TYPE.name());
        ActionListener listener = mock(ActionListener.class);
        Task task = mock(Task.class);
        User user = new User("username", "r1", "r2");
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(user)
            .realmRef(new RealmRef("test", "test", "foo"))
            .build(false);
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
        }).when(authcService).authenticate(eq(action), eq(request), eq(InternalUsers.SYSTEM_USER), anyActionListener());
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
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(user)
            .realmRef(new RealmRef("test", "test", "foo"))
            .build(false);
        doAnswer(i -> {
            final Object[] args = i.getArguments();
            assertThat(args, arrayWithSize(4));
            ActionListener callback = (ActionListener) args[args.length - 1];
            assertNull(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
            AuditUtil.generateRequestId(threadContext);
            callback.onResponse(authentication);
            return Void.TYPE;
        }).when(authcService).authenticate(eq("_action"), eq(request), eq(InternalUsers.SYSTEM_USER), anyActionListener());
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

    public void testSecondaryAuth() throws Exception {
        ActionRequest request = mock(ActionRequest.class);
        ActionListener listener = mock(ActionListener.class);
        Task task = mock(Task.class);
        User user1 = new User("user1", "r1", "r2");
        User user2 = new User("user2", "r3", "r4");
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(user1)
            .realmRef(new RealmRef("test", "test", "foo"))
            .build(false);
        Authentication secondaryAuth = AuthenticationTestHelper.builder()
            .user(user2)
            .realmRef(new RealmRef("test2", "test2", "foo2"))
            .build(false);
        String requestId = UUIDs.randomBase64UUID();

        // mock primary and secondary authentication headers already set
        assertNull(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
        threadContext.putTransient(AuthenticationField.AUTHENTICATION_KEY, authentication);
        threadContext.putHeader(AuthenticationField.AUTHENTICATION_KEY, authentication.encode());
        assertNull(threadContext.getTransient(SecondaryAuthentication.THREAD_CTX_KEY));
        threadContext.putTransient(SecondaryAuthentication.THREAD_CTX_KEY, secondaryAuth);
        threadContext.putHeader(SecondaryAuthentication.THREAD_CTX_KEY, secondaryAuth.encode());

        String actionName = "_action_secondary_auth";
        // ensure that the filter swaps out to the secondary user
        doAnswer(i -> {
            final Object[] args = i.getArguments();
            assertThat(args, arrayWithSize(4));
            ActionListener callback = (ActionListener) args[args.length - 1];
            assertSame(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY), secondaryAuth);
            assertEquals(threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY), secondaryAuth.encode());
            threadContext.putHeader("_xpack_audit_request_id", requestId);
            callback.onResponse(secondaryAuth);
            return Void.TYPE;
        }).when(authcService).authenticate(eq(actionName), eq(request), eq(InternalUsers.SYSTEM_USER), anyActionListener());

        mockAuthorize();
        ActionResponse actionResponse = mock(ActionResponse.class);
        mockChain(task, actionName, request, actionResponse);
        filter.apply(task, actionName, request, listener, chain);
        verify(authzService).authorize(eq(secondaryAuth), eq(actionName), eq(request), anyActionListener());
        verify(auditTrail).coordinatingActionResponse(eq(requestId), eq(secondaryAuth), eq(actionName), eq(request), eq(actionResponse));
    }

    public void testSecondaryAuthRequired() throws Exception {
        ActionRequest request = mock(ActionRequest.class);
        ActionListener listener = mock(ActionListener.class);
        Task task = mock(Task.class);
        User user1 = new User("user1", "r1", "r2");
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(user1)
            .realmRef(new RealmRef("test", "test", "foo"))
            .build(false);
        // mock primary but not secondary authentication headers already set
        assertNull(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
        threadContext.putTransient(AuthenticationField.AUTHENTICATION_KEY, authentication);
        threadContext.putHeader(AuthenticationField.AUTHENTICATION_KEY, authentication.encode());
        String actionName = "_action_secondary_auth";
        ActionResponse actionResponse = mock(ActionResponse.class);
        mockChain(task, actionName, request, actionResponse);
        filter.apply(task, actionName, request, listener, chain);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue() instanceof IllegalArgumentException);
        assertEquals(
            "es-secondary-authorization header must be used to call action [" + actionName + "]",
            exceptionCaptor.getValue().getMessage()
        );
        verifyNoInteractions(authcService);
        verifyNoInteractions(authzService);
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
        }).when(authcService).authenticate(eq("_action"), eq(request), eq(InternalUsers.SYSTEM_USER), anyActionListener());
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
            new SecurityContext(Settings.EMPTY, threadContext).putIndicesAccessControl(indicesAccessControl);
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
