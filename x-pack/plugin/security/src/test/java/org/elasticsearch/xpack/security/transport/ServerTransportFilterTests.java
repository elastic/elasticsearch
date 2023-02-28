/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.MockIndicesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.RemoteAccessAuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.Set;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_HEADER_FILTERS;
import static org.elasticsearch.xpack.core.security.support.Exceptions.authenticationError;
import static org.elasticsearch.xpack.core.security.support.Exceptions.authorizationError;
import static org.elasticsearch.xpack.security.transport.SecurityServerTransportInterceptor.REMOTE_ACCESS_ACTION_ALLOWLIST;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ServerTransportFilterTests extends ESTestCase {

    private AuthenticationService authcService;
    private AuthorizationService authzService;
    private TransportChannel channel;
    private boolean failDestructiveOperations;
    private DestructiveOperations destructiveOperations;
    private RemoteAccessAuthenticationService remoteAccessAuthcService;

    @Before
    public void init() throws Exception {
        authcService = mock(AuthenticationService.class);
        authzService = mock(AuthorizationService.class);
        channel = mock(TransportChannel.class);
        when(channel.getProfileName()).thenReturn(TransportSettings.DEFAULT_PROFILE);
        when(channel.getVersion()).thenReturn(TransportVersion.CURRENT);
        failDestructiveOperations = randomBoolean();
        Settings settings = Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), failDestructiveOperations).build();
        destructiveOperations = new DestructiveOperations(
            settings,
            new ClusterSettings(settings, Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))
        );
        remoteAccessAuthcService = mock(RemoteAccessAuthenticationService.class);
        when(remoteAccessAuthcService.getAuthenticationService()).thenReturn(authcService);
    }

    public void testInbound() {
        TransportRequest request = mock(TransportRequest.class);
        Authentication authentication = AuthenticationTestHelper.builder().build();
        doAnswer(getAnswer(authentication)).when(authcService).authenticate(eq("_action"), eq(request), eq(true), anyActionListener());
        ServerTransportFilter filter = getNodeFilter();
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        filter.inbound("_action", request, channel, future);
        // future.get(); // don't block it's not called really just mocked
        verify(authzService).authorize(eq(authentication), eq("_action"), eq(request), anyActionListener());
    }

    public void testRemoteAccessInbound() {
        TransportRequest request = mock(TransportRequest.class);
        Authentication authentication = AuthenticationTestHelper.builder().build();
        boolean allowlisted = randomBoolean();
        String action = allowlisted ? randomFrom(REMOTE_ACCESS_ACTION_ALLOWLIST) : "_action";
        doAnswer(getAnswer(authentication)).when(authcService).authenticate(eq(action), eq(request), eq(true), anyActionListener());
        doAnswer(getAnswer(authentication, true)).when(remoteAccessAuthcService).authenticate(eq(action), eq(request), anyActionListener());
        ServerTransportFilter filter = getNodeRemoteAccessFilter();
        PlainActionFuture<Void> listener = spy(new PlainActionFuture<>());
        filter.inbound(action, request, channel, listener);
        if (allowlisted) {
            verify(authzService).authorize(
                eq(replaceWithInternalUserAuthcForHandshake(action, authentication)),
                eq(action),
                eq(request),
                anyActionListener()
            );
            verify(remoteAccessAuthcService).authenticate(anyString(), any(), anyActionListener());
            verify(authcService, never()).authenticate(anyString(), any(), anyBoolean(), anyActionListener());
        } else {
            var actual = expectThrows(IllegalArgumentException.class, listener::actionGet);
            assertThat(
                actual.getMessage(),
                equalTo("action [" + action + "] is not allowed as a cross cluster operation on the dedicated remote cluster server port")
            );
            verify(authcService, never()).authenticate(anyString(), any(), anyBoolean(), anyActionListener());
            verify(remoteAccessAuthcService, never()).authenticate(anyString(), any(), anyActionListener());
            verifyNoMoreInteractions(authzService);
        }
    }

    public void testRemoteAccessInboundInvalidHeadersFail() {
        TransportRequest request = mock(TransportRequest.class);
        Authentication authentication = AuthenticationTestHelper.builder().build();
        boolean allowlisted = randomBoolean();
        String action = allowlisted ? randomFrom(REMOTE_ACCESS_ACTION_ALLOWLIST) : "_action";
        doAnswer(getAnswer(authentication)).when(authcService).authenticate(eq(action), eq(request), eq(true), anyActionListener());
        doAnswer(getAnswer(authentication, true)).when(remoteAccessAuthcService).authenticate(eq(action), eq(request), anyActionListener());
        ServerTransportFilter filter = getNodeRemoteAccessFilter(Set.copyOf(randomNonEmptySubsetOf(SECURITY_HEADER_FILTERS)));
        PlainActionFuture<Void> listener = new PlainActionFuture<>();
        filter.inbound(action, request, channel, listener);
        var actual = expectThrows(IllegalArgumentException.class, listener::actionGet);
        if (allowlisted) {
            verifyNoMoreInteractions(authcService);
            verifyNoMoreInteractions(authzService);
            assertThat(
                actual.getMessage(),
                containsString("is not allowed for cross cluster requests through the dedicated remote cluster server port")
            );
        } else {
            verify(authcService, never()).authenticate(anyString(), any(), anyBoolean(), anyActionListener());
            verify(remoteAccessAuthcService, never()).authenticate(anyString(), any(), anyActionListener());
            verifyNoMoreInteractions(authzService);
            assertThat(
                actual.getMessage(),
                equalTo("action [" + action + "] is not allowed as a cross cluster operation on the dedicated remote cluster server port")
            );
        }
        verify(remoteAccessAuthcService, never()).authenticate(anyString(), any(), anyActionListener());
    }

    public void testInboundDestructiveOperations() {
        String action = randomFrom(CloseIndexAction.NAME, OpenIndexAction.NAME, DeleteIndexAction.NAME);
        TransportRequest request = new MockIndicesRequest(
            IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()),
            randomFrom("*", "_all", "test*")
        );
        Authentication authentication = AuthenticationTestHelper.builder().build();
        doAnswer(getAnswer(authentication)).when(authcService).authenticate(eq(action), eq(request), eq(true), anyActionListener());
        boolean remoteAccess = randomBoolean();
        ServerTransportFilter filter = remoteAccess ? getNodeRemoteAccessFilter() : getNodeFilter();
        PlainActionFuture<Void> listener = spy(new PlainActionFuture<>());
        filter.inbound(action, request, channel, listener);
        if (failDestructiveOperations) {
            expectThrows(IllegalArgumentException.class, listener::actionGet);
            verifyNoMoreInteractions(authzService);
        } else {
            if (remoteAccess) {
                var actual = expectThrows(IllegalArgumentException.class, listener::actionGet);
                assertThat(
                    actual.getMessage(),
                    equalTo(
                        "action [" + action + "] is not allowed as a cross cluster operation on the dedicated remote cluster server port"
                    )
                );
                verifyNoMoreInteractions(authzService);
            } else {
                verify(authzService).authorize(eq(authentication), eq(action), eq(request), anyActionListener());
            }
        }
    }

    public void testInboundAuthenticationException() {
        TransportRequest request = mock(TransportRequest.class);
        Exception authE = authenticationError("authc failed");
        doAnswer(i -> {
            final Object[] args = i.getArguments();
            assertThat(args, arrayWithSize(4));
            @SuppressWarnings("unchecked")
            ActionListener<Authentication> callback = (ActionListener<Authentication>) args[args.length - 1];
            callback.onFailure(authE);
            return Void.TYPE;
        }).when(authcService).authenticate(eq("_action"), eq(request), eq(true), anyActionListener());
        ServerTransportFilter filter = getNodeFilter();
        try {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            filter.inbound("_action", request, channel, future);
            future.actionGet();
            fail("expected filter inbound to throw an authentication exception on authentication error");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.getMessage(), equalTo("authc failed"));
        }
        verifyNoMoreInteractions(authzService);
    }

    public void testRemoteAccessInboundAuthenticationException() {
        TransportRequest request = mock(TransportRequest.class);
        Exception authE = authenticationError("authc failed");
        // Only pick allowlisted action -- it does not make sense to pick one that isn't because we will never get to authenticate in that
        // case
        String action = randomFrom(REMOTE_ACCESS_ACTION_ALLOWLIST);
        doAnswer(i -> {
            final Object[] args = i.getArguments();
            assertThat(args, arrayWithSize(3));
            @SuppressWarnings("unchecked")
            ActionListener<Authentication> callback = (ActionListener<Authentication>) args[args.length - 1];
            callback.onFailure(authE);
            return Void.TYPE;
        }).when(remoteAccessAuthcService).authenticate(eq(action), eq(request), anyActionListener());
        doAnswer(i -> {
            final Object[] args = i.getArguments();
            assertThat(args, arrayWithSize(4));
            @SuppressWarnings("unchecked")
            ActionListener<Authentication> callback = (ActionListener<Authentication>) args[args.length - 1];
            callback.onFailure(authE);
            return Void.TYPE;
        }).when(authcService).authenticate(eq(action), eq(request), eq(true), anyActionListener());
        ServerTransportFilter filter = getNodeRemoteAccessFilter();
        try {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            filter.inbound(action, request, channel, future);
            future.actionGet();
            fail("expected filter inbound to throw an authentication exception on authentication error");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.getMessage(), equalTo("authc failed"));
        }
        verifyNoMoreInteractions(authzService);
        verify(remoteAccessAuthcService).authenticate(anyString(), any(), anyActionListener());
        verify(authcService, never()).authenticate(anyString(), any(), anyBoolean(), anyActionListener());
    }

    public void testInboundAuthorizationException() {
        boolean remoteAccess = randomBoolean();
        ServerTransportFilter filter = remoteAccess ? getNodeRemoteAccessFilter() : getNodeFilter();
        TransportRequest request = mock(TransportRequest.class);
        Authentication authentication = AuthenticationTestHelper.builder().build();
        String action = SearchAction.NAME;
        doAnswer(getAnswer(authentication)).when(authcService).authenticate(eq(action), eq(request), eq(true), anyActionListener());
        doAnswer(getAnswer(authentication, true)).when(remoteAccessAuthcService).authenticate(eq(action), eq(request), anyActionListener());
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        doThrow(authorizationError("authz failed")).when(authzService)
            .authorize(eq(authentication), eq(action), eq(request), anyActionListener());
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> {
            filter.inbound(action, request, channel, future);
            future.actionGet();
        });
        assertThat(e.getMessage(), equalTo("authz failed"));
        if (remoteAccess) {
            verify(remoteAccessAuthcService).authenticate(anyString(), any(), anyActionListener());
            verify(authcService, never()).authenticate(anyString(), any(), anyBoolean(), anyActionListener());
        } else {
            verify(authcService).authenticate(anyString(), any(), anyBoolean(), anyActionListener());
            verify(remoteAccessAuthcService, never()).authenticate(anyString(), any(), anyActionListener());
        }
    }

    public void testAllowsNodeActions() {
        final String internalAction = "internal:foo/bar";
        final String nodeOrShardAction = "indices:action" + randomFrom("[s]", "[p]", "[r]", "[n]", "[s][p]", "[s][r]", "[f]");
        ServerTransportFilter filter = getNodeFilter();
        TransportRequest request = mock(TransportRequest.class);
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("test", "superuser"))
            .realmRef(new RealmRef("test", "test", "node1"))
            .build(false);
        doAnswer(getAnswer(authentication)).when(authcService).authenticate(eq(internalAction), eq(request), eq(true), anyActionListener());
        doAnswer(getAnswer(authentication)).when(authcService)
            .authenticate(eq(nodeOrShardAction), eq(request), eq(true), anyActionListener());

        filter.inbound(internalAction, request, channel, new PlainActionFuture<>());
        verify(authcService).authenticate(eq(internalAction), eq(request), eq(true), anyActionListener());
        verify(authzService).authorize(eq(authentication), eq(internalAction), eq(request), anyActionListener());

        filter.inbound(nodeOrShardAction, request, channel, new PlainActionFuture<>());
        verify(authcService).authenticate(eq(nodeOrShardAction), eq(request), eq(true), anyActionListener());
        verify(authzService).authorize(eq(authentication), eq(nodeOrShardAction), eq(request), anyActionListener());
        verifyNoMoreInteractions(authcService, authzService);
    }

    private static Answer<Class<Void>> getAnswer(Authentication authentication) {
        return getAnswer(authentication, false);
    }

    private static Answer<Class<Void>> getAnswer(Authentication authentication, boolean remoteAccess) {
        return i -> {
            final Object[] args = i.getArguments();
            assertThat(args, arrayWithSize(remoteAccess ? 3 : 4));
            @SuppressWarnings("unchecked")
            ActionListener<Authentication> callback = (ActionListener<Authentication>) args[args.length - 1];
            callback.onResponse(authentication);
            return Void.TYPE;
        };
    }

    private ServerTransportFilter getNodeFilter() {
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        ThreadContext threadContext = new ThreadContext(settings);
        return new ServerTransportFilter(
            authcService,
            authzService,
            threadContext,
            false,
            destructiveOperations,
            new SecurityContext(settings, threadContext)
        );
    }

    private RemoteAccessServerTransportFilter getNodeRemoteAccessFilter() {
        return getNodeRemoteAccessFilter(Collections.emptySet());
    }

    private RemoteAccessServerTransportFilter getNodeRemoteAccessFilter(Set<String> additionalHeadersKeys) {
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        ThreadContext threadContext = new ThreadContext(settings);
        for (var header : additionalHeadersKeys) {
            threadContext.putHeader(header, randomAlphaOfLength(20));
        }
        // Randomly include valid headers
        if (randomBoolean()) {
            for (var validHeader : RemoteAccessServerTransportFilter.ALLOWED_TRANSPORT_HEADERS) {
                // don't overwrite additionalHeadersKeys
                if (false == additionalHeadersKeys.contains(validHeader)) {
                    threadContext.putHeader(validHeader, randomAlphaOfLength(20));
                }
            }
        }
        return new RemoteAccessServerTransportFilter(
            remoteAccessAuthcService,
            authzService,
            threadContext,
            false,
            destructiveOperations,
            new SecurityContext(settings, threadContext)
        );
    }

    private static Authentication replaceWithInternalUserAuthcForHandshake(String action, Authentication authentication) {
        return action.equals(TransportService.HANDSHAKE_ACTION_NAME)
            ? Authentication.newInternalAuthentication(SystemUser.INSTANCE, authentication.getEffectiveSubject().getTransportVersion(), "")
            : authentication;
    }
}
