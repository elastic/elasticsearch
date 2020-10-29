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
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.junit.Before;

import java.util.Collections;

import static org.elasticsearch.xpack.core.security.support.Exceptions.authenticationError;
import static org.elasticsearch.xpack.core.security.support.Exceptions.authorizationError;
import static org.hamcrest.Matchers.arrayWithSize;
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
    private ThreadContext threadContext;
    private ServerTransportFilter serverTransportFilter;

    @Before
    public void init() throws Exception {
        authcService = mock(AuthenticationService.class);
        authzService = mock(AuthorizationService.class);
        channel = mock(TransportChannel.class);
        when(channel.getProfileName()).thenReturn(TransportSettings.DEFAULT_PROFILE);
        when(channel.getVersion()).thenReturn(Version.CURRENT);
        failDestructiveOperations = randomBoolean();
        Settings settings = Settings.builder()
                .put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), failDestructiveOperations)
                .put("path.home", createTempDir()).build();
        destructiveOperations = new DestructiveOperations(settings,
                new ClusterSettings(settings, Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING)));
        threadContext = new ThreadContext(settings);
        serverTransportFilter = new ServerTransportFilter(authcService, authzService, threadContext, false, destructiveOperations,
                new SecurityContext(settings, threadContext), new XPackLicenseState(settings, () -> 0));
    }

    public void testInbound() throws Exception {
        TransportRequest request = mock(TransportRequest.class);
        Authentication authentication = mock(Authentication.class);
        when(authentication.getVersion()).thenReturn(Version.CURRENT);
        when(authentication.getUser()).thenReturn(SystemUser.INSTANCE);
        doAnswer(i -> {
            final Object[] args = i.getArguments();
            assertThat(args, arrayWithSize(4));
            AuditUtil.generateRequestId(threadContext);
            ActionListener callback = (ActionListener) args[args.length - 1];
            callback.onResponse(authentication);
            return Void.TYPE;
        }).when(authcService).authenticate(eq("_action"), eq(request), eq(true), any(ActionListener.class));
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        serverTransportFilter.inbound("_action", request, channel, future);
        //future.get(); // don't block it's not called really just mocked
        verify(authzService).authorize(eq(authentication), eq("_action"), eq(request), any(ActionListener.class));
    }

    public void testInboundDestructiveOperations() throws Exception {
        String action = randomFrom(CloseIndexAction.NAME, OpenIndexAction.NAME, DeleteIndexAction.NAME);
        TransportRequest request = new MockIndicesRequest(
                IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()),
                randomFrom("*", "_all", "test*"));
        Authentication authentication = mock(Authentication.class);
        when(authentication.getVersion()).thenReturn(Version.CURRENT);
        when(authentication.getUser()).thenReturn(SystemUser.INSTANCE);
        doAnswer(i -> {
            final Object[] args = i.getArguments();
            assertThat(args, arrayWithSize(4));
            AuditUtil.generateRequestId(threadContext);
            ActionListener callback = (ActionListener) args[args.length - 1];
            callback.onResponse(authentication);
            return Void.TYPE;
        }).when(authcService).authenticate(eq(action), eq(request), eq(true), any(ActionListener.class));
        PlainActionFuture listener = mock(PlainActionFuture.class);
        serverTransportFilter.inbound(action, request, channel, listener);
        if (failDestructiveOperations) {
            verify(listener).onFailure(isA(IllegalArgumentException.class));
            verifyNoMoreInteractions(authzService);
        } else {
            verify(authzService).authorize(eq(authentication), eq(action), eq(request), any(ActionListener.class));
        }
    }

    public void testInboundAuthenticationException() throws Exception {
        TransportRequest request = mock(TransportRequest.class);
        Exception authE = authenticationError("authc failed");
        doAnswer(i -> {
            final Object[] args = i.getArguments();
            AuditUtil.generateRequestId(threadContext);
            assertThat(args, arrayWithSize(4));
            ActionListener callback = (ActionListener) args[args.length - 1];
            callback.onFailure(authE);
            return Void.TYPE;
        }).when(authcService).authenticate(eq("_action"), eq(request), eq(true), any(ActionListener.class));
        try {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            serverTransportFilter.inbound("_action", request, channel, future);
            future.actionGet();
            fail("expected filter inbound to throw an authentication exception on authentication error");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.getMessage(), equalTo("authc failed"));
        }
        verifyZeroInteractions(authzService);
    }

    public void testInboundAuthorizationException() throws Exception {
        TransportRequest request = mock(TransportRequest.class);
        Authentication authentication = mock(Authentication.class);
        doAnswer(i -> {
            final Object[] args = i.getArguments();
            AuditUtil.generateRequestId(threadContext);
            assertThat(args, arrayWithSize(4));
            ActionListener callback = (ActionListener) args[args.length - 1];
            callback.onResponse(authentication);
            return Void.TYPE;
        }).when(authcService).authenticate(eq("_action"), eq(request), eq(true), any(ActionListener.class));
        when(authentication.getVersion()).thenReturn(Version.CURRENT);
        when(authentication.getUser()).thenReturn(XPackUser.INSTANCE);
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        doThrow(authorizationError("authz failed"))
            .when(authzService).authorize(eq(authentication), eq("_action"), eq(request), any(ActionListener.class));
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> {
            serverTransportFilter.inbound("_action", request, channel, future);
            future.actionGet();
        });
        assertThat(e.getMessage(), equalTo("authz failed"));
    }

    public void testAllowsNodeActions() throws Exception {
        final String internalAction = "internal:foo/bar";
        final String nodeOrShardAction = "indices:action" + randomFrom("[s]", "[p]", "[r]", "[n]", "[s][p]", "[s][r]", "[f]");
        TransportRequest request = mock(TransportRequest.class);
        Authentication authentication = new Authentication(new User("test", "superuser"), new RealmRef("test", "test", "node1"), null);
        doAnswer(i -> {
            final Object[] args = i.getArguments();
            AuditUtil.generateRequestId(threadContext);
            assertThat(args, arrayWithSize(4));
            ActionListener callback = (ActionListener) args[args.length - 1];
            callback.onResponse(authentication);
            return Void.TYPE;
        }).when(authcService).authenticate(eq(internalAction), eq(request), eq(true), any(ActionListener.class));
        doAnswer((i) -> {
            final Object[] args = i.getArguments();
            assertThat(args, arrayWithSize(4));
            ActionListener callback = (ActionListener) args[args.length - 1];
            callback.onResponse(authentication);
            return Void.TYPE;
        }).when(authcService).authenticate(eq(nodeOrShardAction), eq(request), eq(true), any(ActionListener.class));

        serverTransportFilter.inbound(internalAction, request, channel, new PlainActionFuture<>());
        verify(authcService).authenticate(eq(internalAction), eq(request), eq(true), any(ActionListener.class));
        verify(authzService).authorize(eq(authentication), eq(internalAction), eq(request), any(ActionListener.class));

        serverTransportFilter.inbound(nodeOrShardAction, request, channel, new PlainActionFuture<>());
        verify(authcService).authenticate(eq(nodeOrShardAction), eq(request), eq(true), any(ActionListener.class));
        verify(authzService).authorize(eq(authentication), eq(nodeOrShardAction), eq(request), any(ActionListener.class));
        verifyNoMoreInteractions(authcService, authzService);
    }
}
