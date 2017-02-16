/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.Transport.Connection;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor.AsyncSender;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.SecurityContext;
import org.elasticsearch.xpack.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.user.KibanaUser;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.ssl.SSLService;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.arrayContaining;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class SecurityServerTransportInterceptorTests extends ESTestCase {

    private Settings settings;
    private ThreadPool threadPool;
    private ThreadContext threadContext;
    private XPackLicenseState xPackLicenseState;
    private SecurityContext securityContext;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder().put("path.home", createTempDir()).build();
        threadPool = mock(ThreadPool.class);
        threadContext = new ThreadContext(settings);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        securityContext = spy(new SecurityContext(settings, threadPool.getThreadContext()));
        xPackLicenseState = mock(XPackLicenseState.class);
        when(xPackLicenseState.isAuthAllowed()).thenReturn(true);
    }

    public void testSendAsyncUnlicensed() {
        SecurityServerTransportInterceptor interceptor = new SecurityServerTransportInterceptor(settings, threadPool,
                mock(AuthenticationService.class), mock(AuthorizationService.class), xPackLicenseState, mock(SSLService.class),
                securityContext, new DestructiveOperations(Settings.EMPTY, new ClusterSettings(Settings.EMPTY,
                Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))));
        when(xPackLicenseState.isAuthAllowed()).thenReturn(false);
        AtomicBoolean calledWrappedSender = new AtomicBoolean(false);
        AsyncSender sender = interceptor.interceptSender(new AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(Transport.Connection connection, String action, TransportRequest request,
                                                                  TransportRequestOptions options, TransportResponseHandler<T> handler) {
                if (calledWrappedSender.compareAndSet(false, true) == false) {
                    fail("sender called more than once!");
                }
            }
        });
        sender.sendRequest(null, null, null, null, null);
        assertTrue(calledWrappedSender.get());
        verify(xPackLicenseState).isAuthAllowed();
        verifyNoMoreInteractions(xPackLicenseState);
        verifyZeroInteractions(securityContext);
    }

    public void testSendAsync() throws Exception {
        final User user = new User("test");
        final Authentication authentication = new Authentication(user, new RealmRef("ldap", "foo", "node1"), null);
        authentication.writeToContext(threadContext);
        SecurityServerTransportInterceptor interceptor = new SecurityServerTransportInterceptor(settings, threadPool,
                mock(AuthenticationService.class), mock(AuthorizationService.class), xPackLicenseState, mock(SSLService.class),
                securityContext, new DestructiveOperations(Settings.EMPTY, new ClusterSettings(Settings.EMPTY,
                Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))));

        AtomicBoolean calledWrappedSender = new AtomicBoolean(false);
        AtomicReference<User> sendingUser = new AtomicReference<>();
        AsyncSender sender = interceptor.interceptSender(new AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(Transport.Connection connection, String action, TransportRequest request,
                                                                  TransportRequestOptions options, TransportResponseHandler<T> handler) {
                if (calledWrappedSender.compareAndSet(false, true) == false) {
                    fail("sender called more than once!");
                }
                sendingUser.set(securityContext.getUser());
            }
        });
        Transport.Connection connection = mock(Transport.Connection.class);
        when(connection.getVersion()).thenReturn(Version.CURRENT);
        sender.sendRequest(connection, "indices:foo", null, null, null);
        assertTrue(calledWrappedSender.get());
        assertEquals(user, sendingUser.get());
        assertEquals(user, securityContext.getUser());
        verify(xPackLicenseState).isAuthAllowed();
        verify(securityContext, never()).executeAsUser(any(User.class), any(Consumer.class), any(Version.class));
        verifyNoMoreInteractions(xPackLicenseState);
    }

    public void testSendAsyncSwitchToSystem() throws Exception {
        final User user = new User("test");
        final Authentication authentication = new Authentication(user, new RealmRef("ldap", "foo", "node1"), null);
        authentication.writeToContext(threadContext);
        threadContext.putTransient(AuthorizationService.ORIGINATING_ACTION_KEY, "indices:foo");

        SecurityServerTransportInterceptor interceptor = new SecurityServerTransportInterceptor(settings, threadPool,
                mock(AuthenticationService.class), mock(AuthorizationService.class), xPackLicenseState, mock(SSLService.class),
                securityContext, new DestructiveOperations(Settings.EMPTY, new ClusterSettings(Settings.EMPTY,
                Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))));

        AtomicBoolean calledWrappedSender = new AtomicBoolean(false);
        AtomicReference<User> sendingUser = new AtomicReference<>();
        AsyncSender sender = interceptor.interceptSender(new AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(Transport.Connection connection, String action, TransportRequest request,
                                                                  TransportRequestOptions options, TransportResponseHandler<T> handler) {
                if (calledWrappedSender.compareAndSet(false, true) == false) {
                    fail("sender called more than once!");
                }
                sendingUser.set(securityContext.getUser());
            }
        });
        Connection connection = mock(Connection.class);
        when(connection.getVersion()).thenReturn(Version.CURRENT);
        sender.sendRequest(connection, "internal:foo", null, null, null);
        assertTrue(calledWrappedSender.get());
        assertNotEquals(user, sendingUser.get());
        assertEquals(SystemUser.INSTANCE, sendingUser.get());
        assertEquals(user, securityContext.getUser());
        verify(xPackLicenseState).isAuthAllowed();
        verify(securityContext).executeAsUser(any(User.class), any(Consumer.class), eq(Version.CURRENT));
        verifyNoMoreInteractions(xPackLicenseState);
    }

    public void testSendWithoutUser() throws Exception {
        SecurityServerTransportInterceptor interceptor = new SecurityServerTransportInterceptor(settings, threadPool,
                mock(AuthenticationService.class), mock(AuthorizationService.class), xPackLicenseState, mock(SSLService.class),
                securityContext, new DestructiveOperations(Settings.EMPTY, new ClusterSettings(Settings.EMPTY,
                Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))));

        assertNull(securityContext.getUser());
        AsyncSender sender = interceptor.interceptSender(new AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(Transport.Connection connection, String action, TransportRequest request,
                                                                  TransportRequestOptions options, TransportResponseHandler<T> handler) {
                fail("sender should not be called!");
            }
        });
        Transport.Connection connection = mock(Transport.Connection.class);
        when(connection.getVersion()).thenReturn(Version.CURRENT);
        IllegalStateException e =
                expectThrows(IllegalStateException.class, () -> sender.sendRequest(connection, "indices:foo", null, null, null));
        assertEquals("there should always be a user when sending a message", e.getMessage());
        assertNull(securityContext.getUser());
        verify(xPackLicenseState).isAuthAllowed();
        verify(securityContext, never()).executeAsUser(any(User.class), any(Consumer.class), any(Version.class));
        verifyNoMoreInteractions(xPackLicenseState);
    }

    public void testSendWithKibanaUser() throws Exception {
        final User user = new KibanaUser(true);
        final Authentication authentication = new Authentication(user, new RealmRef("reserved", "reserved", "node1"), null);
        authentication.writeToContext(threadContext);
        threadContext.putTransient(AuthorizationService.ORIGINATING_ACTION_KEY, "indices:foo");

        SecurityServerTransportInterceptor interceptor = new SecurityServerTransportInterceptor(settings, threadPool,
                mock(AuthenticationService.class), mock(AuthorizationService.class), xPackLicenseState, mock(SSLService.class),
                securityContext, new DestructiveOperations(Settings.EMPTY, new ClusterSettings(Settings.EMPTY,
                Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))));

        AtomicBoolean calledWrappedSender = new AtomicBoolean(false);
        AtomicReference<User> sendingUser = new AtomicReference<>();
        AsyncSender intercepted = new AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(Transport.Connection connection, String action, TransportRequest request,
                                                                  TransportRequestOptions options, TransportResponseHandler<T> handler) {
                if (calledWrappedSender.compareAndSet(false, true) == false) {
                    fail("sender called more than once!");
                }
                sendingUser.set(securityContext.getUser());
            }
        };
        AsyncSender sender = interceptor.interceptSender(intercepted);
        Transport.Connection connection = mock(Transport.Connection.class);
        final Version version = Version.fromId(randomIntBetween(Version.V_5_0_0_ID, Version.V_5_2_0_ID_UNRELEASED - 100));
        when(connection.getVersion()).thenReturn(version);
        sender.sendRequest(connection, "indices:foo[s]", null, null, null);
        assertTrue(calledWrappedSender.get());
        assertNotEquals(user, sendingUser.get());
        assertEquals(KibanaUser.NAME, sendingUser.get().principal());
        assertThat(sendingUser.get().roles(), arrayContaining("kibana"));
        assertEquals(user, securityContext.getUser());

        // reset and test with version that was changed
        calledWrappedSender.set(false);
        sendingUser.set(null);
        when(connection.getVersion()).thenReturn(Version.V_5_2_0_UNRELEASED);
        sender.sendRequest(connection, "indices:foo[s]", null, null, null);
        assertTrue(calledWrappedSender.get());
        assertEquals(user, sendingUser.get());

        // reset and disable reserved realm
        calledWrappedSender.set(false);
        sendingUser.set(null);
        when(connection.getVersion()).thenReturn(Version.V_5_0_0);
        settings = Settings.builder().put(settings).put(XPackSettings.RESERVED_REALM_ENABLED_SETTING.getKey(), false).build();
        interceptor = new SecurityServerTransportInterceptor(settings, threadPool,
                mock(AuthenticationService.class), mock(AuthorizationService.class), xPackLicenseState, mock(SSLService.class),
                securityContext, new DestructiveOperations(Settings.EMPTY, new ClusterSettings(Settings.EMPTY,
                Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))));
        sender = interceptor.interceptSender(intercepted);
        sender.sendRequest(connection, "indices:foo[s]", null, null, null);
        assertTrue(calledWrappedSender.get());
        assertEquals(user, sendingUser.get());

        verify(xPackLicenseState, times(3)).isAuthAllowed();
        verify(securityContext, times(1)).executeAsUser(any(User.class), any(Consumer.class), eq(version));
        verifyNoMoreInteractions(xPackLicenseState);
    }

    public void testContextRestoreResponseHandler() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        threadContext.putTransient("foo", "bar");
        threadContext.putHeader("key", "value");
        try (ThreadContext.StoredContext storedContext = threadContext.stashContext()) {
            threadContext.putTransient("foo", "different_bar");
            threadContext.putHeader("key", "value2");
            TransportResponseHandler<Empty> handler = new TransportService.ContextRestoreResponseHandler<>(
                    threadContext.wrapRestorable(storedContext), new TransportResponseHandler<Empty>() {

                @Override
                public Empty newInstance() {
                    return Empty.INSTANCE;
                }

                @Override
                public void handleResponse(Empty response) {
                    assertEquals("bar", threadContext.getTransient("foo"));
                    assertEquals("value", threadContext.getHeader("key"));
                }

                @Override
                public void handleException(TransportException exp) {
                    assertEquals("bar", threadContext.getTransient("foo"));
                    assertEquals("value", threadContext.getHeader("key"));
                }

                @Override
                public String executor() {
                    return null;
                }
            });

            handler.handleResponse(null);
            handler.handleException(null);
        }
    }

    public void testContextRestoreResponseHandlerRestoreOriginalContext() throws Exception {
        try (ThreadContext threadContext = new ThreadContext(Settings.EMPTY)) {
            threadContext.putTransient("foo", "bar");
            threadContext.putHeader("key", "value");
            TransportResponseHandler<Empty> handler;
            try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                threadContext.putTransient("foo", "different_bar");
                threadContext.putHeader("key", "value2");
                handler = new TransportService.ContextRestoreResponseHandler<>(threadContext.newRestorableContext(true),
                        new TransportResponseHandler<Empty>() {

                            @Override
                            public Empty newInstance() {
                                return Empty.INSTANCE;
                            }

                            @Override
                            public void handleResponse(Empty response) {
                                assertEquals("different_bar", threadContext.getTransient("foo"));
                                assertEquals("value2", threadContext.getHeader("key"));
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                assertEquals("different_bar", threadContext.getTransient("foo"));
                                assertEquals("value2", threadContext.getHeader("key"));
                            }

                            @Override
                            public String executor() {
                                return null;
                            }
                        });
            }

            assertEquals("bar", threadContext.getTransient("foo"));
            assertEquals("value", threadContext.getHeader("key"));
            handler.handleResponse(null);

            assertEquals("bar", threadContext.getTransient("foo"));
            assertEquals("value", threadContext.getHeader("key"));
            handler.handleException(null);

            assertEquals("bar", threadContext.getTransient("foo"));
            assertEquals("value", threadContext.getHeader("key"));
        }
    }
}
