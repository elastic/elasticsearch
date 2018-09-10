/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.MockIndicesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.mock.orig.Mockito.times;
import static org.elasticsearch.xpack.core.security.support.Exceptions.authenticationError;
import static org.elasticsearch.xpack.core.security.support.Exceptions.authorizationError;
import static org.hamcrest.Matchers.equalTo;
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

public class ServerTransportFilterTests extends ESTestCase {

    private AuthenticationService authcService;
    private AuthorizationService authzService;
    private TransportChannel channel;
    private boolean failDestructiveOperations;
    private DestructiveOperations destructiveOperations;

    @Before
    public void init() throws Exception {
        authcService = mock(AuthenticationService.class);
        authzService = mock(AuthorizationService.class);
        channel = mock(TransportChannel.class);
        when(channel.getProfileName()).thenReturn(TcpTransport.DEFAULT_PROFILE);
        when(channel.getVersion()).thenReturn(Version.CURRENT);
        failDestructiveOperations = randomBoolean();
        Settings settings = Settings.builder()
                .put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), failDestructiveOperations).build();
        destructiveOperations = new DestructiveOperations(settings,
                new ClusterSettings(settings, Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING)));
    }

    public void testInbound() throws Exception {
        TransportRequest request = mock(TransportRequest.class);
        Authentication authentication = mock(Authentication.class);
        when(authentication.getVersion()).thenReturn(Version.CURRENT);
        when(authentication.getUser()).thenReturn(SystemUser.INSTANCE);
        doAnswer((i) -> {
            ActionListener callback =
                    (ActionListener) i.getArguments()[3];
            callback.onResponse(authentication);
            return Void.TYPE;
        }).when(authcService).authenticate(eq("_action"), eq(request), eq((User)null), any(ActionListener.class));
        ServerTransportFilter filter = getClientOrNodeFilter();
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        filter.inbound("_action", request, channel, future);
        //future.get(); // don't block it's not called really just mocked
        verify(authzService).authorize(authentication, "_action", request, null, null);
    }

    public void testInboundDestructiveOperations() throws Exception {
        String action = randomFrom(CloseIndexAction.NAME, OpenIndexAction.NAME, DeleteIndexAction.NAME);
        TransportRequest request = new MockIndicesRequest(
                IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()),
                randomFrom("*", "_all", "test*"));
        Authentication authentication = mock(Authentication.class);
        when(authentication.getVersion()).thenReturn(Version.CURRENT);
        when(authentication.getUser()).thenReturn(SystemUser.INSTANCE);
        doAnswer((i) -> {
            ActionListener callback =
                    (ActionListener) i.getArguments()[3];
            callback.onResponse(authentication);
            return Void.TYPE;
        }).when(authcService).authenticate(eq(action), eq(request), eq((User)null), any(ActionListener.class));
        ServerTransportFilter filter = getClientOrNodeFilter();
        PlainActionFuture listener = mock(PlainActionFuture.class);
        filter.inbound(action, request, channel, listener);
        if (failDestructiveOperations) {
            verify(listener).onFailure(isA(IllegalArgumentException.class));
            verifyNoMoreInteractions(authzService);
        } else {
            verify(authzService).authorize(authentication, action, request, null, null);
        }
    }

    public void testInboundAuthenticationException() throws Exception {
        TransportRequest request = mock(TransportRequest.class);
        Exception authE = authenticationError("authc failed");
        doAnswer((i) -> {
            ActionListener callback =
                    (ActionListener) i.getArguments()[3];
            callback.onFailure(authE);
            return Void.TYPE;
        }).when(authcService).authenticate(eq("_action"), eq(request), eq((User)null), any(ActionListener.class));
        ServerTransportFilter filter = getClientOrNodeFilter();
        try {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            filter.inbound("_action", request, channel, future);
            future.actionGet();
            fail("expected filter inbound to throw an authentication exception on authentication error");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.getMessage(), equalTo("authc failed"));
        }
        verifyZeroInteractions(authzService);
    }

    public void testInboundAuthorizationException() throws Exception {
        ServerTransportFilter filter = getClientOrNodeFilter();
        TransportRequest request = mock(TransportRequest.class);
        Authentication authentication = mock(Authentication.class);
        doAnswer((i) -> {
            ActionListener callback =
                    (ActionListener) i.getArguments()[3];
            callback.onResponse(authentication);
            return Void.TYPE;
        }).when(authcService).authenticate(eq("_action"), eq(request), eq((User)null), any(ActionListener.class));
        final Role empty = Role.EMPTY;
        doAnswer((i) -> {
            ActionListener callback =
                    (ActionListener) i.getArguments()[1];
            callback.onResponse(empty);
            return Void.TYPE;
        }).when(authzService).roles(any(User.class), any(ActionListener.class));
        when(authentication.getVersion()).thenReturn(Version.CURRENT);
        when(authentication.getUser()).thenReturn(XPackUser.INSTANCE);
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        doThrow(authorizationError("authz failed")).when(authzService).authorize(authentication, "_action", request,
                empty, null);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> {
            filter.inbound("_action", request, channel, future);
            future.actionGet();
        });
        assertThat(e.getMessage(), equalTo("authz failed"));
    }

    public void testClientProfileRejectsNodeActions() throws Exception {
        TransportRequest request = mock(TransportRequest.class);
        ServerTransportFilter filter = getClientFilter(true);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
                () -> filter.inbound("internal:foo/bar", request, channel, new PlainActionFuture<>()));
        assertEquals("executing internal/shard actions is considered malicious and forbidden", e.getMessage());
        e = expectThrows(ElasticsearchSecurityException.class,
                () -> filter.inbound("indices:action" + randomFrom("[s]", "[p]", "[r]", "[n]", "[s][p]", "[s][r]", "[f]"),
                        request, channel, new PlainActionFuture<>()));
        assertEquals("executing internal/shard actions is considered malicious and forbidden", e.getMessage());
        verifyZeroInteractions(authcService);
    }

    public void testNodeProfileAllowsNodeActions() throws Exception {
        final String internalAction = "internal:foo/bar";
        final String nodeOrShardAction = "indices:action" + randomFrom("[s]", "[p]", "[r]", "[n]", "[s][p]", "[s][r]", "[f]");
        ServerTransportFilter filter = getNodeFilter(true);
        TransportRequest request = mock(TransportRequest.class);
        Authentication authentication = new Authentication(new User("test", "superuser"), new RealmRef("test", "test", "node1"), null);
        doAnswer((i) -> {
            ActionListener callback =
                    (ActionListener) i.getArguments()[1];
            callback.onResponse(authentication.getUser().equals(i.getArguments()[0]) ? ReservedRolesStore.SUPERUSER_ROLE : null);
            return Void.TYPE;
        }).when(authzService).roles(any(User.class), any(ActionListener.class));
        doAnswer((i) -> {
            ActionListener callback =
                    (ActionListener) i.getArguments()[3];
            callback.onResponse(authentication);
            return Void.TYPE;
        }).when(authcService).authenticate(eq(internalAction), eq(request), eq((User)null), any(ActionListener.class));
        doAnswer((i) -> {
            ActionListener callback =
                    (ActionListener) i.getArguments()[3];
            callback.onResponse(authentication);
            return Void.TYPE;
        }).when(authcService).authenticate(eq(nodeOrShardAction), eq(request), eq((User)null), any(ActionListener.class));

        filter.inbound(internalAction, request, channel, new PlainActionFuture<>());
        verify(authcService).authenticate(eq(internalAction), eq(request), eq((User)null), any(ActionListener.class));
        verify(authzService).roles(eq(authentication.getUser()), any(ActionListener.class));
        verify(authzService).authorize(authentication, internalAction, request, ReservedRolesStore.SUPERUSER_ROLE, null);

        filter.inbound(nodeOrShardAction, request, channel, new PlainActionFuture<>());
        verify(authcService).authenticate(eq(nodeOrShardAction), eq(request), eq((User)null), any(ActionListener.class));
        verify(authzService, times(2)).roles(eq(authentication.getUser()), any(ActionListener.class));
        verify(authzService).authorize(authentication, nodeOrShardAction, request, ReservedRolesStore.SUPERUSER_ROLE, null);
        verifyNoMoreInteractions(authcService, authzService);
    }

    private ServerTransportFilter getClientOrNodeFilter() throws IOException {
        return randomBoolean() ? getNodeFilter(true) : getClientFilter(true);
    }

    private ServerTransportFilter.ClientProfile getClientFilter(boolean reservedRealmEnabled) throws IOException {
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        ThreadContext threadContext = new ThreadContext(settings);
        return new ServerTransportFilter.ClientProfile(authcService, authzService, threadContext, false, destructiveOperations,
                reservedRealmEnabled, new SecurityContext(settings, threadContext));
    }

    private ServerTransportFilter.NodeProfile getNodeFilter(boolean reservedRealmEnabled) throws IOException {
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        ThreadContext threadContext = new ThreadContext(settings);
        return new ServerTransportFilter.NodeProfile(authcService, authzService, threadContext, false, destructiveOperations,
                reservedRealmEnabled, new SecurityContext(settings, threadContext));
    }
}
