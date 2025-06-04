/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

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
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.CrossClusterAccessAuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.Set;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_HEADER_FILTERS;
import static org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo.CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY;
import static org.elasticsearch.xpack.core.security.support.Exceptions.authenticationError;
import static org.elasticsearch.xpack.core.security.support.Exceptions.authorizationError;
import static org.elasticsearch.xpack.security.authc.CrossClusterAccessHeaders.CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY;
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
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ServerTransportFilterTests extends ESTestCase {

    private AuthenticationService authcService;
    private AuthorizationService authzService;
    private TransportChannel channel;
    private boolean failDestructiveOperations;
    private DestructiveOperations destructiveOperations;
    private CrossClusterAccessAuthenticationService crossClusterAccessAuthcService;
    private MockLicenseState mockLicenseState;

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
        crossClusterAccessAuthcService = mock(CrossClusterAccessAuthenticationService.class);
        when(crossClusterAccessAuthcService.getAuthenticationService()).thenReturn(authcService);
        mockLicenseState = MockLicenseState.createMock();
        Mockito.when(mockLicenseState.isAllowed(Security.ADVANCED_REMOTE_CLUSTER_SECURITY_FEATURE)).thenReturn(true);
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

    public void testCrossClusterAccessInbound() {
        TransportRequest request = mock(TransportRequest.class);
        Authentication authentication = AuthenticationTestHelper.builder().build();
        String action = randomAlphaOfLengthBetween(10, 20);
        doAnswer(getAnswer(authentication)).when(authcService).authenticate(eq(action), eq(request), eq(true), anyActionListener());
        doAnswer(getAnswer(authentication, true)).when(crossClusterAccessAuthcService)
            .authenticate(eq(action), eq(request), anyActionListener());
        ServerTransportFilter filter = getNodeCrossClusterAccessFilter();
        PlainActionFuture<Void> listener = spy(new PlainActionFuture<>());
        filter.inbound(action, request, channel, listener);
        verify(authzService).authorize(eq(authentication), eq(action), eq(request), anyActionListener());
        verify(crossClusterAccessAuthcService).authenticate(anyString(), any(), anyActionListener());
        verify(authcService, never()).authenticate(anyString(), any(), anyBoolean(), anyActionListener());
    }

    public void testCrossClusterAccessInboundInvalidHeadersFail() {
        TransportRequest request = mock(TransportRequest.class);
        Authentication authentication = AuthenticationTestHelper.builder().build();
        String action = randomAlphaOfLengthBetween(10, 20);
        doAnswer(getAnswer(authentication)).when(authcService).authenticate(eq(action), eq(request), eq(true), anyActionListener());
        doAnswer(getAnswer(authentication, true)).when(crossClusterAccessAuthcService)
            .authenticate(eq(action), eq(request), anyActionListener());
        ServerTransportFilter filter = getNodeCrossClusterAccessFilter(Set.copyOf(randomNonEmptySubsetOf(SECURITY_HEADER_FILTERS)));
        PlainActionFuture<Void> listener = new PlainActionFuture<>();
        filter.inbound(action, request, channel, listener);
        var actual = expectThrows(IllegalArgumentException.class, listener::actionGet);
        verifyNoMoreInteractions(authcService);
        verifyNoMoreInteractions(authzService);
        assertThat(
            actual.getMessage(),
            containsString("is not allowed for cross cluster requests through the dedicated remote cluster server port")
        );
        verify(crossClusterAccessAuthcService, never()).authenticate(anyString(), any(), anyActionListener());
    }

    public void testCrossClusterAccessInboundMissingHeadersFail() {
        TransportRequest request = mock(TransportRequest.class);
        Authentication authentication = AuthenticationTestHelper.builder().build();
        String action = randomAlphaOfLengthBetween(10, 20);
        doAnswer(getAnswer(authentication)).when(authcService).authenticate(eq(action), eq(request), eq(true), anyActionListener());
        doAnswer(getAnswer(authentication, true)).when(crossClusterAccessAuthcService)
            .authenticate(eq(action), eq(request), anyActionListener());
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        ThreadContext threadContext = new ThreadContext(settings);
        String firstMissingHeader = CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY;
        if (randomBoolean()) {
            String headerToInclude = randomBoolean()
                ? CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY
                : CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY;
            if (headerToInclude.equals(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY)) {
                firstMissingHeader = CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY;
            }
            threadContext.putHeader(headerToInclude, randomAlphaOfLength(42));
        }
        ServerTransportFilter filter = new CrossClusterAccessServerTransportFilter(
            crossClusterAccessAuthcService,
            authzService,
            threadContext,
            false,
            destructiveOperations,
            new SecurityContext(settings, threadContext),
            mockLicenseState
        );

        PlainActionFuture<Void> listener = new PlainActionFuture<>();
        filter.inbound(action, request, channel, listener);
        var actual = expectThrows(IllegalArgumentException.class, listener::actionGet);

        verifyNoMoreInteractions(authcService);
        verifyNoMoreInteractions(authzService);
        assertThat(
            actual.getMessage(),
            equalTo(
                "Cross cluster requests through the dedicated remote cluster server port require transport header ["
                    + firstMissingHeader
                    + "] but none found. "
                    + "Please ensure you have configured remote cluster credentials on the cluster originating the request."
            )
        );
        verify(crossClusterAccessAuthcService, never()).authenticate(anyString(), any(), anyActionListener());
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

    public void testCrossClusterAccessInboundAuthenticationException() {
        TransportRequest request = mock(TransportRequest.class);
        Exception authE = authenticationError("authc failed");
        String action = randomAlphaOfLengthBetween(10, 20);
        doAnswer(i -> {
            final Object[] args = i.getArguments();
            assertThat(args, arrayWithSize(3));
            @SuppressWarnings("unchecked")
            ActionListener<Authentication> callback = (ActionListener<Authentication>) args[args.length - 1];
            callback.onFailure(authE);
            return Void.TYPE;
        }).when(crossClusterAccessAuthcService).authenticate(eq(action), eq(request), anyActionListener());
        doAnswer(i -> {
            final Object[] args = i.getArguments();
            assertThat(args, arrayWithSize(4));
            @SuppressWarnings("unchecked")
            ActionListener<Authentication> callback = (ActionListener<Authentication>) args[args.length - 1];
            callback.onFailure(authE);
            return Void.TYPE;
        }).when(authcService).authenticate(eq(action), eq(request), eq(true), anyActionListener());
        ServerTransportFilter filter = getNodeCrossClusterAccessFilter();
        try {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            filter.inbound(action, request, channel, future);
            future.actionGet();
            fail("expected filter inbound to throw an authentication exception on authentication error");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.getMessage(), equalTo("authc failed"));
        }
        verifyNoMoreInteractions(authzService);
        verify(crossClusterAccessAuthcService).authenticate(anyString(), any(), anyActionListener());
        verify(authcService, never()).authenticate(anyString(), any(), anyBoolean(), anyActionListener());
    }

    public void testInboundAuthorizationException() {
        boolean crossClusterAccess = randomBoolean();
        ServerTransportFilter filter = crossClusterAccess ? getNodeCrossClusterAccessFilter() : getNodeFilter();
        TransportRequest request = mock(TransportRequest.class);
        Authentication authentication = AuthenticationTestHelper.builder().build();
        String action = TransportSearchAction.TYPE.name();
        doAnswer(getAnswer(authentication)).when(authcService).authenticate(eq(action), eq(request), eq(true), anyActionListener());
        doAnswer(getAnswer(authentication, true)).when(crossClusterAccessAuthcService)
            .authenticate(eq(action), eq(request), anyActionListener());
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        doThrow(authorizationError("authz failed")).when(authzService)
            .authorize(eq(authentication), eq(action), eq(request), anyActionListener());
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> {
            filter.inbound(action, request, channel, future);
            future.actionGet();
        });
        assertThat(e.getMessage(), equalTo("authz failed"));
        if (crossClusterAccess) {
            verify(crossClusterAccessAuthcService).authenticate(anyString(), any(), anyActionListener());
            verify(authcService, never()).authenticate(anyString(), any(), anyBoolean(), anyActionListener());
        } else {
            verify(authcService).authenticate(anyString(), any(), anyBoolean(), anyActionListener());
            verify(crossClusterAccessAuthcService, never()).authenticate(anyString(), any(), anyActionListener());
        }
    }

    public void testCrossClusterAccessInboundFailsWithUnsupportedLicense() {
        final MockLicenseState unsupportedLicenseState = MockLicenseState.createMock();
        Mockito.when(unsupportedLicenseState.isAllowed(Security.ADVANCED_REMOTE_CLUSTER_SECURITY_FEATURE)).thenReturn(false);

        ServerTransportFilter crossClusterAccessFilter = getNodeCrossClusterAccessFilter(unsupportedLicenseState);
        PlainActionFuture<Void> listener = new PlainActionFuture<>();
        String action = randomAlphaOfLengthBetween(10, 20);
        crossClusterAccessFilter.inbound(action, mock(TransportRequest.class), channel, listener);

        ElasticsearchSecurityException actualException = expectThrows(ElasticsearchSecurityException.class, listener::actionGet);
        assertThat(
            actualException.getMessage(),
            equalTo("current license is non-compliant for [" + Security.ADVANCED_REMOTE_CLUSTER_SECURITY_FEATURE.getName() + "]")
        );

        // License check should be executed first, hence we don't expect authc/authz to be even attempted.
        verify(crossClusterAccessAuthcService, never()).authenticate(anyString(), any(), anyActionListener());
        verifyNoInteractions(authzService, authcService);
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

    private static Answer<Class<Void>> getAnswer(Authentication authentication, boolean crossClusterAccess) {
        return i -> {
            final Object[] args = i.getArguments();
            assertThat(args, arrayWithSize(crossClusterAccess ? 3 : 4));
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

    private CrossClusterAccessServerTransportFilter getNodeCrossClusterAccessFilter() {
        return getNodeCrossClusterAccessFilter(Collections.emptySet(), mockLicenseState);
    }

    private CrossClusterAccessServerTransportFilter getNodeCrossClusterAccessFilter(Set<String> additionalHeadersKeys) {
        return getNodeCrossClusterAccessFilter(additionalHeadersKeys, mockLicenseState);
    }

    private CrossClusterAccessServerTransportFilter getNodeCrossClusterAccessFilter(XPackLicenseState licenseState) {
        return getNodeCrossClusterAccessFilter(Collections.emptySet(), licenseState);
    }

    private CrossClusterAccessServerTransportFilter getNodeCrossClusterAccessFilter(
        Set<String> additionalHeadersKeys,
        XPackLicenseState licenseState
    ) {
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        ThreadContext threadContext = new ThreadContext(settings);
        for (var header : additionalHeadersKeys) {
            threadContext.putHeader(header, randomAlphaOfLength(20));
        }
        // Randomly include valid headers
        if (randomBoolean()) {
            for (var validHeader : CrossClusterAccessServerTransportFilter.ALLOWED_TRANSPORT_HEADERS) {
                // don't overwrite additionalHeadersKeys
                if (false == additionalHeadersKeys.contains(validHeader)) {
                    threadContext.putHeader(validHeader, randomAlphaOfLength(20));
                }
            }
        }
        var requiredHeaders = Set.of(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY, CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY);
        for (var header : requiredHeaders) {
            // don't overwrite already present headers
            if (threadContext.getHeader(header) == null) {
                threadContext.putHeader(header, randomAlphaOfLength(20));
            }
        }
        return new CrossClusterAccessServerTransportFilter(
            crossClusterAccessAuthcService,
            authzService,
            threadContext,
            false,
            destructiveOperations,
            new SecurityContext(settings, threadContext),
            licenseState
        );
    }
}
