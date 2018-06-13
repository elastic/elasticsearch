/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheAction;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheRequest;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheResponse;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.kerberos.support.KerberosTestCase;
import org.elasticsearch.xpack.security.authc.kerberos.support.KerberosTicketValidator;
import org.elasticsearch.xpack.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.ietf.jgss.GSSException;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.login.LoginException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class KerberosRealmTests extends ESTestCase {

    private ThreadPool threadPool;
    private Settings globalSettings;
    private ResourceWatcherService resourceWatcherService;
    private Settings settings;
    private RealmConfig config;

    private KerberosTicketValidator mockKerberosTicketValidator;
    private Cache<String, User> mockUsernameUserCache;
    private NativeRoleMappingStore mockNativeRoleMappingStore;

    private static final Set<String> roles;
    static {
        roles = new HashSet<>();
        roles.add("admin");
        roles.add("kibana_user");
    }

    @Before
    public void setup() throws Exception {
        threadPool = new TestThreadPool("kerb realm tests");
        resourceWatcherService = new ResourceWatcherService(Settings.EMPTY, threadPool);
        Path dir = createTempDir();
        globalSettings = Settings.builder().put("path.home", dir).build();
        settings = KerberosTestCase.buildKerberosRealmSettings(KerberosTestCase.writeKeyTab(dir.resolve("key.keytab"), "asa").toString());
    }

    @After
    public void shutdown() throws InterruptedException {
        resourceWatcherService.stop();
        terminate(threadPool);
    }

    public void testSupportAuthentication() {
        final KerberosRealm kerberosRealm = createKerberosRealm("test@REALM");

        final KerberosAuthenticationToken kerberosAuthenticationToken = new KerberosAuthenticationToken(randomByteArrayOfLength(5));
        assertTrue(kerberosRealm.supports(kerberosAuthenticationToken));
        final UsernamePasswordToken usernamePasswordToken =
                new UsernamePasswordToken(randomAlphaOfLength(5), new SecureString(new char[] { 'a', 'b', 'c' }));
        assertFalse(kerberosRealm.supports(usernamePasswordToken));
    }

    public void testAuthenticateWithNonKerberosAuthneticationToken() {
        final KerberosRealm kerberosRealm = createKerberosRealm("test@REALM");

        final UsernamePasswordToken usernamePasswordToken =
                new UsernamePasswordToken(randomAlphaOfLength(5), new SecureString(new char[] { 'a', 'b', 'c' }));
        PlainActionFuture<AuthenticationResult> result = new PlainActionFuture<>();
        kerberosRealm.authenticate(usernamePasswordToken, result);
        assertNotNull(result);
        assertSame(AuthenticationResult.notHandled(), result.actionGet());
    }

    public void testAuthenticateWithValidTicketSucessAuthnWithUserDetails() throws LoginException, GSSException {
        final KerberosRealm kerberosRealm = createKerberosRealm("test@REALM");

        final User expectedUser = new User("test@REALM", roles.toArray(new String[roles.size()]), null, null, null, true);
        final byte[] decodedTicket = "base64encodedticket".getBytes(StandardCharsets.UTF_8);
        final Path keytabPath = config.env().configFile().resolve(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.get(config.settings()));
        final boolean krbDebug = KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE.get(config.settings());
        when(mockKerberosTicketValidator.validateTicket(decodedTicket, keytabPath, krbDebug))
                .thenReturn(new Tuple<>("test@REALM", "out-token"));
        final KerberosAuthenticationToken kerberosAuthenticationToken = new KerberosAuthenticationToken(decodedTicket);

        // authenticate
        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        kerberosRealm.authenticate(kerberosAuthenticationToken, future);
        final AuthenticationResult result = future.actionGet();
        final User user1 = result.getUser();
        assertSuccessAuthnResult(expectedUser, "out-token", result);

        // authenticate with cache
        future = new PlainActionFuture<>();
        kerberosRealm.authenticate(kerberosAuthenticationToken, future);
        final User user2 = result.getUser();
        assertSuccessAuthnResult(expectedUser, "out-token", result);
        assertThat(user1, sameInstance(user2));

        Mockito.verify(mockKerberosTicketValidator, Mockito.times(2)).validateTicket(aryEq(decodedTicket), eq(keytabPath), eq(krbDebug));
    }

    public void testAuthenticateDifferentFailureScenarios() throws LoginException, GSSException {
        final KerberosRealm kerberosRealm = createKerberosRealm("test@REALM");
        final boolean validTicket = rarely();
        final boolean throwExceptionForInvalidTicket = validTicket ? false : randomBoolean();
        final boolean returnOutToken = randomBoolean();
        final boolean throwLoginException = randomBoolean();
        final byte[] decodedTicket = "base64encodedticket".getBytes(StandardCharsets.UTF_8);
        final Path keytabPath = config.env().configFile().resolve(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.get(config.settings()));
        final boolean krbDebug = KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE.get(config.settings());
        if (validTicket) {
            when(mockKerberosTicketValidator.validateTicket(decodedTicket, keytabPath, krbDebug))
                    .thenReturn(new Tuple<>("test@REALM", "out-token"));
        } else {
            if (throwExceptionForInvalidTicket) {
                if (throwLoginException) {
                    when(mockKerberosTicketValidator.validateTicket(decodedTicket, keytabPath, krbDebug))
                            .thenThrow(new LoginException("Login Exception"));
                } else {
                    when(mockKerberosTicketValidator.validateTicket(decodedTicket, keytabPath, krbDebug))
                            .thenThrow(new GSSException(GSSException.FAILURE));
                }
            } else {
                if (returnOutToken) {
                    when(mockKerberosTicketValidator.validateTicket(decodedTicket, keytabPath, krbDebug))
                            .thenReturn(new Tuple<>(null, "out-token"));
                } else {
                    when(mockKerberosTicketValidator.validateTicket(decodedTicket, keytabPath, krbDebug)).thenReturn(null);
                }
            }
        }
        final boolean nullKerberosAuthnToken = rarely();
        final KerberosAuthenticationToken kerberosAuthenticationToken =
                nullKerberosAuthnToken ? null : new KerberosAuthenticationToken(decodedTicket);

        // Verify
        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        kerberosRealm.authenticate(kerberosAuthenticationToken, future);
        AuthenticationResult result = future.actionGet();
        assertNotNull(result);
        if (nullKerberosAuthnToken) {
            assertEquals(AuthenticationResult.Status.CONTINUE, result.getStatus());
            verifyZeroInteractions(mockUsernameUserCache, mockKerberosTicketValidator);
        } else {
            if (validTicket) {
                assertEquals(AuthenticationResult.Status.SUCCESS, result.getStatus());
                final User expectedUser = new User("test@REALM", roles.toArray(new String[roles.size()]), null, null, null, true);
                assertEquals(expectedUser, result.getUser());
                assertNotNull(threadPool.getThreadContext().getHeader(KerberosAuthenticationToken.WWW_AUTHENTICATE));
                assertEquals(KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER + "out-token",
                        threadPool.getThreadContext().getHeader(KerberosAuthenticationToken.WWW_AUTHENTICATE));
                Mockito.verify(mockUsernameUserCache).put(Mockito.eq("test@REALM"), Mockito.eq(expectedUser));
            } else {
                assertEquals(AuthenticationResult.Status.TERMINATE, result.getStatus());
                if (throwExceptionForInvalidTicket == false) {
                    if (returnOutToken) {
                        Map<String, List<String>> responseHeaders = threadPool.getThreadContext().getResponseHeaders();
                        assertNotNull(responseHeaders);
                        assertEquals(KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER + "out-token",
                                responseHeaders.get(KerberosAuthenticationToken.WWW_AUTHENTICATE).get(0));
                        assertEquals("failed to authenticate user, gss netogiation not complete", result.getMessage());
                    } else {
                        assertEquals("failed to authenticate user, invalid kerberos ticket", result.getMessage());
                    }
                } else {
                    if (throwLoginException) {
                        assertEquals("failed to authenticate user, service login failure", result.getMessage());
                    } else {
                        assertEquals("failed to authenticate user, GSS negotiation failure", result.getMessage());
                    }
                    assertTrue(result.getException() instanceof ElasticsearchSecurityException);
                    final List<String> wwwAuthnHeader = ((ElasticsearchSecurityException) result.getException())
                            .getHeader(KerberosAuthenticationToken.WWW_AUTHENTICATE);
                    assertNotNull(wwwAuthnHeader);
                    assertEquals(KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER.trim(), wwwAuthnHeader.get(0));
                }
                verifyZeroInteractions(mockUsernameUserCache);
            }
            Mockito.verify(mockKerberosTicketValidator).validateTicket(aryEq(decodedTicket), eq(keytabPath), eq(krbDebug));
        }
    }

    public void testFailedAuthorization() throws LoginException, GSSException {
        final KerberosRealm kerberosRealm = createKerberosRealm("test@REALM");
        final byte[] decodedTicket = "base64encodedticket".getBytes(StandardCharsets.UTF_8);
        final Path keytabPath = config.env().configFile().resolve(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.get(config.settings()));
        final boolean krbDebug = KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE.get(config.settings());
        when(mockKerberosTicketValidator.validateTicket(decodedTicket, keytabPath, krbDebug))
                .thenReturn(new Tuple<>("does-not-exist@REALM", "out-token"));
        final KerberosAuthenticationToken kerberosAuthenticationToken = new KerberosAuthenticationToken(decodedTicket);

        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        kerberosRealm.authenticate(kerberosAuthenticationToken, future);

        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> future.actionGet());
        assertThat(e.status(), is(RestStatus.FORBIDDEN));
        assertThat(e.getMessage(), Matchers.equalTo("Expected UPN 'test@REALM' but was 'does-not-exist@REALM'"));
    }

    public void testLookupUser() {
        final KerberosRealm kerberosRealm = createKerberosRealm("test@REALM");
        final PlainActionFuture<User> future = new PlainActionFuture<>();
        kerberosRealm.lookupUser("test@REALM", future);
        assertNull(future.actionGet());
    }

    public void testCacheInvalidationScenarios() throws LoginException, GSSException {
        createRealmConfig();
        List<String> userNames = Arrays.asList(randomAlphaOfLength(5) + "@REALM", randomAlphaOfLength(5) + "@REALM");
        final AtomicBoolean invalidateKerbRealmActionState = new AtomicBoolean(false);
        final NativeRoleMappingStore roleMapper = roleMappingStore("test-kerb-realm", userNames, invalidateKerbRealmActionState);

        final KerberosTicketValidator mockKerberosTicketValidator = mock(KerberosTicketValidator.class);
        final KerberosRealm kerberosRealm = new KerberosRealm(config, roleMapper, mockKerberosTicketValidator, threadPool, null);
        String authNUsername = randomFrom(userNames);
        String outToken = randomAlphaOfLength(5);
        final byte[] decodedTicket = "base64encodedticket".getBytes(StandardCharsets.UTF_8);
        final Path keytabPath = config.env().configFile().resolve(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.get(config.settings()));
        final boolean krbDebug = KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE.get(config.settings());
        when(mockKerberosTicketValidator.validateTicket(decodedTicket, keytabPath, krbDebug))
                .thenReturn(new Tuple<>(authNUsername, outToken));
        final User expectedUser = new User(authNUsername, roles.toArray(new String[roles.size()]), null, null, null, true);

        final KerberosAuthenticationToken kerberosAuthenticationToken = new KerberosAuthenticationToken(decodedTicket);
        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        kerberosRealm.authenticate(kerberosAuthenticationToken, future);
        AuthenticationResult result = future.actionGet();
        final User user1 = result.getUser();

        assertSuccessAuthnResult(expectedUser, outToken, result);

        String expireThisUser = randomFrom(userNames);
        boolean expireAll = randomBoolean();
        if (expireAll) {
            // Must call realm.expireAll internally after state change
            roleMapper.onSecurityIndexStateChange(dummyState(ClusterHealthStatus.RED), dummyState(ClusterHealthStatus.GREEN));
            assertTrue(invalidateKerbRealmActionState.get());
            kerberosRealm.expireAll();
        } else {
            kerberosRealm.expire(expireThisUser);
        }

        future = new PlainActionFuture<>();
        kerberosRealm.authenticate(kerberosAuthenticationToken, future);
        result = future.actionGet();
        final User user2 = result.getUser();

        assertSuccessAuthnResult(expectedUser, outToken, result);

        if (expireAll || expireThisUser.equals(authNUsername)) {
            assertFalse(user1 == user2);
        } else {
            assertThat(user1, sameInstance(user2));
        }
    }

    private SecurityIndexManager.State dummyState(ClusterHealthStatus indexStatus) {
        return new SecurityIndexManager.State(true, true, true, true, null, indexStatus);
    }

    private void assertSuccessAuthnResult(final User expectedUser, final String outToken, final AuthenticationResult result) {
        assertNotNull(result);
        assertEquals(AuthenticationResult.Status.SUCCESS, result.getStatus());
        assertEquals(expectedUser, result.getUser());
        Map<String, List<String>> responseHeaders = threadPool.getThreadContext().getResponseHeaders();
        assertNotNull(responseHeaders);
        assertEquals(KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER + outToken,
                responseHeaders.get(KerberosAuthenticationToken.WWW_AUTHENTICATE).get(0));
    }

    @SuppressWarnings("unchecked")
    private KerberosRealm createKerberosRealm(final String userForRoleMapping) {
        createRealmConfig();
        mockNativeRoleMappingStore = mockRoleMappingStore(userForRoleMapping);
        mockUsernameUserCache = mock(Cache.class);
        mockKerberosTicketValidator = mock(KerberosTicketValidator.class);
        final KerberosRealm kerberosRealm =
                new KerberosRealm(config, mockNativeRoleMappingStore, mockKerberosTicketValidator, threadPool, mockUsernameUserCache);
        return kerberosRealm;
    }

    private void createRealmConfig() {
        config = new RealmConfig("test-kerb-realm", settings, globalSettings, TestEnvironment.newEnvironment(globalSettings),
                new ThreadContext(globalSettings));
    }

    private NativeRoleMappingStore mockRoleMappingStore(final String expectedUserName) {
        return roleMappingStore(null, Arrays.asList(expectedUserName), null);
    }

    @SuppressWarnings("unchecked")
    private NativeRoleMappingStore roleMappingStore(final String realmName, final List<String> expectedUserNames,
            final AtomicBoolean expireAll) {
        final Client mockClient = mock(Client.class);
        when(mockClient.threadPool()).thenReturn(threadPool);
        when(mockClient.settings()).thenReturn(settings);

        doAnswer(invocationOnMock -> {
            ActionListener<ClearRealmCacheResponse> listener = (ActionListener<ClearRealmCacheResponse>) invocationOnMock.getArguments()[2];
            listener.onResponse(new ClearRealmCacheResponse(new ClusterName("cluster"), Collections.emptyList(), Collections.emptyList()));
            return null;
        }).when(mockClient).execute(eq(ClearRealmCacheAction.INSTANCE),
                Mockito.argThat(new ClearRealmCacheRequestMatcher(realmName, expireAll)), any(ActionListener.class));

        final NativeRoleMappingStore store = new NativeRoleMappingStore(Settings.EMPTY, mockClient, mock(SecurityIndexManager.class));
        final NativeRoleMappingStore roleMapper = spy(store);

        Mockito.doAnswer(invocation -> {
            final UserRoleMapper.UserData userData = (UserRoleMapper.UserData) invocation.getArguments()[0];
            final ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            if (expectedUserNames.contains(userData.getUsername())) {
                listener.onResponse(roles);
            } else {
                if (expectedUserNames.size() == 1) {
                    listener.onFailure(Exceptions.authorizationError(
                            "Expected UPN '" + expectedUserNames.get(0) + "' but was '" + userData.getUsername() + "'"));
                } else {
                    listener.onFailure(Exceptions
                            .authorizationError("Expected UPN '" + expectedUserNames + "' but was '" + userData.getUsername() + "'"));
                }
            }
            return null;
        }).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), any(ActionListener.class));

        return roleMapper;
    }

    private static class ClearRealmCacheRequestMatcher extends BaseMatcher<ClearRealmCacheRequest> {
        final String expectedRealmNameInRequest;
        final AtomicBoolean invalidateKerbRealmActionState;

        ClearRealmCacheRequestMatcher(final String expectedRealmNameInRequest, final AtomicBoolean invalidateKerbRealmActionState) {
            this.expectedRealmNameInRequest = expectedRealmNameInRequest;
            this.invalidateKerbRealmActionState = invalidateKerbRealmActionState;
        }

        @Override
        public boolean matches(Object item) {
            if (item instanceof ClearRealmCacheRequest) {
                if (Strings.isNullOrEmpty(expectedRealmNameInRequest)) {
                    return true;
                }
                if (Arrays.stream(((ClearRealmCacheRequest) item).realms()).anyMatch((s) -> s.equals(expectedRealmNameInRequest))) {
                    if (invalidateKerbRealmActionState.compareAndSet(false, true)) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public void describeTo(Description description) {
        }
    }

}
