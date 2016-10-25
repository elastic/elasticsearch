/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.xpack.security.authc.Authentication;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xpack.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.permission.Role;
import org.elasticsearch.xpack.security.authz.permission.SuperuserRole;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.user.XPackUser;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.mock.orig.Mockito.times;
import static org.elasticsearch.xpack.security.support.Exceptions.authenticationError;
import static org.elasticsearch.xpack.security.support.Exceptions.authorizationError;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
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

    @Before
    public void init() throws Exception {
        authcService = mock(AuthenticationService.class);
        authzService = mock(AuthorizationService.class);
        channel = mock(TransportChannel.class);
        when(channel.getProfileName()).thenReturn(TransportSettings.DEFAULT_PROFILE);
    }

    public void testInbound() throws Exception {
        TransportRequest request = mock(TransportRequest.class);
        Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(SystemUser.INSTANCE);
        when(authcService.authenticate("_action", request, null)).thenReturn(authentication);
        ServerTransportFilter filter = getClientOrNodeFilter();
        PlainActionFuture future = new PlainActionFuture();
        filter.inbound("_action", request, channel, future);
        //future.get(); // don't block it's not called really just mocked
        verify(authzService).authorize(authentication, "_action", request, Collections.emptyList(), Collections.emptyList());
    }

    public void testInboundAuthenticationException() throws Exception {
        TransportRequest request = mock(TransportRequest.class);
        doThrow(authenticationError("authc failed")).when(authcService).authenticate("_action", request, null);
        ServerTransportFilter filter = getClientOrNodeFilter();
        try {
            PlainActionFuture future = new PlainActionFuture();
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
        when(authcService.authenticate("_action", request, null)).thenReturn(authentication);
        doAnswer((i) -> {
            ActionListener callback =
                    (ActionListener) i.getArguments()[1];
            callback.onResponse(Collections.emptyList());
            return Void.TYPE;
        }).when(authzService).roles(any(User.class), any(ActionListener.class));
        when(authentication.getUser()).thenReturn(XPackUser.INSTANCE);
        when(authentication.getRunAsUser()).thenReturn(XPackUser.INSTANCE);
        PlainActionFuture future = new PlainActionFuture();
        doThrow(authorizationError("authz failed")).when(authzService).authorize(authentication, "_action", request,
                Collections.emptyList(), Collections.emptyList());
        try {
            filter.inbound("_action", request, channel, future);
            future.actionGet();
            fail("expected filter inbound to throw an authorization exception on authorization error");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.getMessage(), equalTo("authz failed"));
        }
    }

    public void testClientProfileRejectsNodeActions() throws Exception {
        TransportRequest request = mock(TransportRequest.class);
        ServerTransportFilter filter = getClientFilter();
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
        ServerTransportFilter filter = getNodeFilter();
        TransportRequest request = mock(TransportRequest.class);
        Authentication authentication = new Authentication(new User("test", "superuser"), new RealmRef("test", "test", "node1"), null);
        final Collection<Role> userRoles = Collections.singletonList(SuperuserRole.INSTANCE);
        doAnswer((i) -> {
            ActionListener callback =
                    (ActionListener) i.getArguments()[1];
            callback.onResponse(authentication.getUser().equals(i.getArguments()[0]) ? userRoles : Collections.emptyList());
            return Void.TYPE;
        }).when(authzService).roles(any(User.class), any(ActionListener.class));
        when(authcService.authenticate(internalAction, request, null)).thenReturn(authentication);
        when(authcService.authenticate(nodeOrShardAction, request, null)).thenReturn(authentication);

        filter.inbound(internalAction, request, channel, new PlainActionFuture<>());
        verify(authcService).authenticate(internalAction, request, null);
        verify(authzService).roles(eq(authentication.getUser()), any(ActionListener.class));
        verify(authzService).authorize(authentication, internalAction, request, userRoles, Collections.emptyList());

        filter.inbound(nodeOrShardAction, request, channel, new PlainActionFuture<>());
        verify(authcService).authenticate(nodeOrShardAction, request, null);
        verify(authzService, times(2)).roles(eq(authentication.getUser()), any(ActionListener.class));
        verify(authzService).authorize(authentication, nodeOrShardAction, request, userRoles, Collections.emptyList());
        verifyNoMoreInteractions(authcService, authzService);
    }

    private ServerTransportFilter getClientOrNodeFilter() {
        return randomBoolean() ? getNodeFilter() : getClientFilter();
    }

    private ServerTransportFilter.ClientProfile getClientFilter() {
        return new ServerTransportFilter.ClientProfile(authcService, authzService, new ThreadContext(Settings.EMPTY), false);
    }

    private ServerTransportFilter.NodeProfile getNodeFilter() {
        return new ServerTransportFilter.NodeProfile(authcService, authzService, new ThreadContext(Settings.EMPTY), false);
    }
}
