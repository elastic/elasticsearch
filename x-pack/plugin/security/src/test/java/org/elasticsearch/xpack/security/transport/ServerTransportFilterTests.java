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
import org.elasticsearch.action.admin.indices.close.TransportCloseIndexAction;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.junit.Before;

import java.util.Collections;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.core.security.support.Exceptions.authenticationError;
import static org.elasticsearch.xpack.core.security.support.Exceptions.authorizationError;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ServerTransportFilterTests extends AbstractServerTransportFilterTests {

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
        when(channel.getProfileName()).thenReturn(TransportSettings.DEFAULT_PROFILE);
        when(channel.getVersion()).thenReturn(TransportVersion.current());
        failDestructiveOperations = randomBoolean();
        Settings settings = Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), failDestructiveOperations).build();
        destructiveOperations = new DestructiveOperations(
            settings,
            new ClusterSettings(settings, Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))
        );
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

    public void testInboundDestructiveOperations() {
        String action = randomFrom(TransportCloseIndexAction.NAME, OpenIndexAction.NAME, TransportDeleteIndexAction.TYPE.name());
        TransportRequest request = new MockIndicesRequest(
            IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()),
            randomFrom("*", "_all", "test*")
        );
        Authentication authentication = AuthenticationTestHelper.builder().build();
        doAnswer(getAnswer(authentication)).when(authcService).authenticate(eq(action), eq(request), eq(true), anyActionListener());
        ServerTransportFilter filter = getNodeFilter();
        PlainActionFuture<Void> listener = spy(new PlainActionFuture<>());
        filter.inbound(action, request, channel, listener);
        if (failDestructiveOperations) {
            expectThrows(IllegalArgumentException.class, listener::actionGet);
            verifyNoMoreInteractions(authzService);
        } else {
            verify(authzService).authorize(eq(authentication), eq(action), eq(request), anyActionListener());
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

    public void testInboundAuthorizationException() {
        ServerTransportFilter filter = getNodeFilter();
        TransportRequest request = mock(TransportRequest.class);
        Authentication authentication = AuthenticationTestHelper.builder().build();
        String action = TransportSearchAction.TYPE.name();
        doAnswer(getAnswer(authentication)).when(authcService).authenticate(eq(action), eq(request), eq(true), anyActionListener());
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        doThrow(authorizationError("authz failed")).when(authzService)
            .authorize(eq(authentication), eq(action), eq(request), anyActionListener());
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> {
            filter.inbound(action, request, channel, future);
            future.actionGet();
        });
        assertThat(e.getMessage(), equalTo("authz failed"));
        verify(authcService).authenticate(anyString(), any(), anyBoolean(), anyActionListener());
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

}
