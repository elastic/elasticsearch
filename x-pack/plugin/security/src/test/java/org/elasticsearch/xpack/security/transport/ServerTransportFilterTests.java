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
import org.elasticsearch.xpack.core.security.user.BwcXPackUser;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.core.security.support.Exceptions.authenticationError;
import static org.elasticsearch.xpack.core.security.support.Exceptions.authorizationError;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
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
        when(channel.getProfileName()).thenReturn(TransportSettings.DEFAULT_PROFILE);
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
            verify(authzService).authorize(eq(authentication), eq(action), eq(request), any(ActionListener.class));
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
        when(authentication.getVersion()).thenReturn(Version.CURRENT);
        when(authentication.getUser()).thenReturn(XPackUser.INSTANCE);
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        doThrow(authorizationError("authz failed"))
            .when(authzService).authorize(eq(authentication), eq("_action"), eq(request), any(ActionListener.class));
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
        verify(authzService).authorize(eq(authentication), eq(internalAction), eq(request), any(ActionListener.class));

        filter.inbound(nodeOrShardAction, request, channel, new PlainActionFuture<>());
        verify(authcService).authenticate(eq(nodeOrShardAction), eq(request), eq((User)null), any(ActionListener.class));
        verify(authzService).authorize(eq(authentication), eq(nodeOrShardAction), eq(request), any(ActionListener.class));
        verifyNoMoreInteractions(authcService, authzService);
    }

    public void testHandlesKibanaUserCompatibility() throws Exception {
        TransportRequest request = mock(TransportRequest.class);
        User user = new User("kibana", "kibana");
        Authentication authentication = mock(Authentication.class);
        final Version version = Version.fromId(randomIntBetween(Version.V_5_0_0_ID, Version.V_5_2_0_ID - 100));
        when(authentication.getVersion()).thenReturn(version);
        when(authentication.getUser()).thenReturn(user);
        doAnswer((i) -> {
            ActionListener callback =
                    (ActionListener) i.getArguments()[3];
            callback.onResponse(authentication);
            return Void.TYPE;
        }).when(authcService).authenticate(eq("_action"), eq(request), eq((User)null), any(ActionListener.class));
        AtomicReference<String[]> rolesRef = new AtomicReference<>();
        doAnswer((i) -> {
            ActionListener callback =
                    (ActionListener) i.getArguments()[3];
            rolesRef.set(((Authentication) i.getArguments()[0]).getUser().roles());
            callback.onResponse(null);
            return Void.TYPE;
        }).when(authzService).authorize(any(Authentication.class), eq("_action"), eq(request), any(ActionListener.class));
        ServerTransportFilter filter = getClientOrNodeFilter();
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        when(channel.getVersion()).thenReturn(version);
        filter.inbound("_action", request, channel, future);
        assertNotNull(rolesRef.get());
        assertThat(rolesRef.get(), arrayContaining("kibana_system"));

        // test with a version that doesn't need changing
        filter = getClientOrNodeFilter();
        rolesRef.set(null);
        user = new KibanaUser(true);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getVersion()).thenReturn(Version.V_5_2_0);
        future = new PlainActionFuture<>();
        filter.inbound("_action", request, channel, future);
        assertNotNull(rolesRef.get());
        assertThat(rolesRef.get(), arrayContaining("kibana_system"));
    }

    public void testHandlesXPackUserCompatibility() throws Exception {
        TransportRequest request = mock(TransportRequest.class);
        User user = new User("_xpack");
        Authentication authentication = mock(Authentication.class);
        when(authentication.getVersion())
                .thenReturn(Version.fromId(randomIntBetween(Version.V_5_0_0_ID, Version.V_5_6_0_ID)));
        when(authentication.getUser()).thenReturn(user);
        doAnswer((i) -> {
            ActionListener callback =
                    (ActionListener) i.getArguments()[3];
            callback.onResponse(authentication);
            return Void.TYPE;
        }).when(authcService).authenticate(eq("_action"), eq(request), eq((User) null), any(ActionListener.class));
        AtomicReference<User> userRef = new AtomicReference<>();
        doAnswer((i) -> {
            ActionListener callback =
                (ActionListener) i.getArguments()[3];
            userRef.set(((Authentication) i.getArguments()[0]).getUser());
            callback.onResponse(null);
            return Void.TYPE;
        }).when(authzService).authorize(any(Authentication.class), eq("_action"), eq(request), any(ActionListener.class));
        ServerTransportFilter filter = getClientOrNodeFilter();
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        filter.inbound("_action", request, channel, future);
        assertThat(userRef.get(), sameInstance(BwcXPackUser.INSTANCE));
        assertThat(userRef.get().roles(), arrayContaining("superuser"));

        // test with a version that doesn't need changing
        filter = getClientOrNodeFilter();
        userRef.set(null);
        user = XPackUser.INSTANCE;
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getVersion()).thenReturn(Version.V_5_6_1);
        future = new PlainActionFuture<>();
        filter.inbound("_action", request, channel, future);
        assertThat(userRef.get(), sameInstance(XPackUser.INSTANCE));
        assertThat(userRef.get().roles(), arrayContaining(XPackUser.ROLE_NAME));
    }

    private ServerTransportFilter getClientOrNodeFilter() throws IOException {
        return randomBoolean() ? getNodeFilter(true) : getClientFilter(true);
    }

    private ServerTransportFilter.ClientProfile getClientFilter(boolean reservedRealmEnabled) throws IOException {
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        ThreadContext threadContext = new ThreadContext(settings);
        return new ServerTransportFilter.ClientProfile(authcService, authzService, threadContext, false, destructiveOperations,
                reservedRealmEnabled, new SecurityContext(settings, threadContext), new XPackLicenseState(settings));
    }

    private ServerTransportFilter.NodeProfile getNodeFilter(boolean reservedRealmEnabled) throws IOException {
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        ThreadContext threadContext = new ThreadContext(settings);
        return new ServerTransportFilter.NodeProfile(authcService, authzService, threadContext, false, destructiveOperations,
                reservedRealmEnabled, new SecurityContext(settings, threadContext), new XPackLicenseState(settings));
    }
}
