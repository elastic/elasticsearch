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
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.kerberos.support.KerberosTestCase;
import org.elasticsearch.xpack.security.authc.kerberos.support.KerberosTicketValidator;
import org.elasticsearch.xpack.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.security.authc.support.UserRoleMapper.UserData;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.ietf.jgss.GSSException;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.login.LoginException;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class KerberosRealmTests extends ESTestCase {

    private Path dir;
    private ThreadPool threadPool;
    private Settings globalSettings;
    private ResourceWatcherService resourceWatcherService;
    private Settings settings;
    private RealmConfig config;

    private KerberosTicketValidator mockKerberosTicketValidator;
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
        dir = createTempDir();
        globalSettings = Settings.builder().put("path.home", dir).build();
        settings = KerberosTestCase.buildKerberosRealmSettings(KerberosTestCase.writeKeyTab(dir.resolve("key.keytab"), "asa").toString());
    }

    @After
    public void shutdown() throws InterruptedException {
        resourceWatcherService.stop();
        terminate(threadPool);
    }

    public void testSupports() {
        final KerberosRealm kerberosRealm = createKerberosRealm("test@REALM");

        final KerberosAuthenticationToken kerberosAuthenticationToken = new KerberosAuthenticationToken(randomByteArrayOfLength(5));
        assertThat(kerberosRealm.supports(kerberosAuthenticationToken), is(true));
        final UsernamePasswordToken usernamePasswordToken =
                new UsernamePasswordToken(randomAlphaOfLength(5), new SecureString(new char[] { 'a', 'b', 'c' }));
        assertThat(kerberosRealm.supports(usernamePasswordToken), is(false));
    }

    public void testAuthenticateWithNonKerberosAuthenticationToken() {
        final KerberosRealm kerberosRealm = createKerberosRealm("test@REALM");

        final UsernamePasswordToken usernamePasswordToken =
                new UsernamePasswordToken(randomAlphaOfLength(5), new SecureString(new char[] { 'a', 'b', 'c' }));
        PlainActionFuture<AuthenticationResult> result = new PlainActionFuture<>();
        kerberosRealm.authenticate(usernamePasswordToken, result);
        assertThat(result, is(notNullValue()));
        assertThat(result.actionGet(), sameInstance(AuthenticationResult.notHandled()));
    }

    public void testAuthenticateWithValidTicketSucessAuthnWithUserDetails() throws LoginException, GSSException {
        final KerberosRealm kerberosRealm = createKerberosRealm("test@REALM");

        final User expectedUser = new User("test@REALM", roles.toArray(new String[roles.size()]), null, null, null, true);
        final byte[] decodedTicket = "base64encodedticket".getBytes(StandardCharsets.UTF_8);
        final Path keytabPath = config.env().configFile().resolve(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.get(config.settings()));
        final boolean krbDebug = KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE.get(config.settings());
        mockKerberosTicketValidator(decodedTicket, keytabPath, krbDebug, new Tuple<>("test@REALM", "out-token"), null);
        final KerberosAuthenticationToken kerberosAuthenticationToken = new KerberosAuthenticationToken(decodedTicket);

        // authenticate
        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        kerberosRealm.authenticate(kerberosAuthenticationToken, future);
        final AuthenticationResult result = future.actionGet();
        final User user1 = result.getUser();
        assertSuccessAuthenticationResult(expectedUser, "out-token", result);

        // authenticate with cache
        future = new PlainActionFuture<>();
        kerberosRealm.authenticate(kerberosAuthenticationToken, future);
        final User user2 = future.actionGet().getUser();
        assertSuccessAuthenticationResult(expectedUser, "out-token", result);
        assertThat(user1, sameInstance(user2));

        verify(mockKerberosTicketValidator, times(2)).validateTicket(aryEq(decodedTicket), eq(keytabPath), eq(krbDebug),
                any(ActionListener.class));
        verify(mockNativeRoleMappingStore).refreshRealmOnChange(kerberosRealm);
        verify(mockNativeRoleMappingStore).resolveRoles(any(UserData.class), any(ActionListener.class));
        verifyNoMoreInteractions(mockKerberosTicketValidator, mockNativeRoleMappingStore);
    }

    public void testAuthenticateDifferentFailureScenarios() throws LoginException, GSSException {
        final KerberosRealm kerberosRealm = createKerberosRealm("test@REALM");
        final boolean validTicket = rarely();
        final boolean throwExceptionForInvalidTicket = validTicket ? false : randomBoolean();
        final boolean throwLoginException = randomBoolean();
        final byte[] decodedTicket = "base64encodedticket".getBytes(StandardCharsets.UTF_8);
        final Path keytabPath = config.env().configFile().resolve(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.get(config.settings()));
        final boolean krbDebug = KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE.get(config.settings());
        if (validTicket) {
            mockKerberosTicketValidator(decodedTicket, keytabPath, krbDebug, new Tuple<>("test@REALM", "out-token"), null);
        } else {
            if (throwExceptionForInvalidTicket) {
                if (throwLoginException) {
                    mockKerberosTicketValidator(decodedTicket, keytabPath, krbDebug, null, new LoginException("Login Exception"));
                } else {
                    mockKerberosTicketValidator(decodedTicket, keytabPath, krbDebug, null, new GSSException(GSSException.FAILURE));
                }
            } else {
                mockKerberosTicketValidator(decodedTicket, keytabPath, krbDebug, new Tuple<>(null, "out-token"), null);
            }
        }
        final boolean nullKerberosAuthnToken = rarely();
        final KerberosAuthenticationToken kerberosAuthenticationToken =
                nullKerberosAuthnToken ? null : new KerberosAuthenticationToken(decodedTicket);

        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        kerberosRealm.authenticate(kerberosAuthenticationToken, future);
        AuthenticationResult result = future.actionGet();
        assertThat(result, is(notNullValue()));
        if (nullKerberosAuthnToken) {
            assertThat(result.getStatus(), is(equalTo(AuthenticationResult.Status.CONTINUE)));
        } else {
            if (validTicket) {
                assertThat(result.getStatus(), is(equalTo(AuthenticationResult.Status.SUCCESS)));
                final User expectedUser = new User("test@REALM", roles.toArray(new String[roles.size()]), null, null, null, true);
                assertThat(result.getUser(), is(equalTo(expectedUser)));
                assertThat(threadPool.getThreadContext().getResponseHeaders().get(KerberosAuthenticationToken.WWW_AUTHENTICATE),
                        is(notNullValue()));
                assertThat(threadPool.getThreadContext().getResponseHeaders().get(KerberosAuthenticationToken.WWW_AUTHENTICATE),
                        is(contains(KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER_PREFIX + "out-token")));
            } else {
                assertThat(result.getStatus(), is(equalTo(AuthenticationResult.Status.TERMINATE)));
                if (throwExceptionForInvalidTicket == false) {
                    assertThat(result.getException(), is(instanceOf(ElasticsearchSecurityException.class)));
                    final List<String> wwwAuthnHeader = ((ElasticsearchSecurityException) result.getException())
                            .getHeader(KerberosAuthenticationToken.WWW_AUTHENTICATE);
                    assertThat(wwwAuthnHeader, is(notNullValue()));
                    assertThat(wwwAuthnHeader.get(0), is(equalTo(KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER_PREFIX + "out-token")));
                    assertThat(result.getMessage(), is(equalTo("failed to authenticate user, gss context negotiation not complete")));
                } else {
                    if (throwLoginException) {
                        assertThat(result.getMessage(), is(equalTo("failed to authenticate user, service login failure")));
                    } else {
                        assertThat(result.getMessage(), is(equalTo("failed to authenticate user, gss context negotiation failure")));
                    }
                    assertThat(result.getException(), is(instanceOf(ElasticsearchSecurityException.class)));
                    final List<String> wwwAuthnHeader = ((ElasticsearchSecurityException) result.getException())
                            .getHeader(KerberosAuthenticationToken.WWW_AUTHENTICATE);
                    assertThat(wwwAuthnHeader, is(notNullValue()));
                    assertThat(wwwAuthnHeader.get(0), is(equalTo(KerberosAuthenticationToken.NEGOTIATE_SCHEME_NAME)));
                }
            }
            verify(mockKerberosTicketValidator).validateTicket(aryEq(decodedTicket), eq(keytabPath), eq(krbDebug),
                    any(ActionListener.class));
        }
    }

    private void mockKerberosTicketValidator(final byte[] decodedTicket, final Path keytabPath, final boolean krbDebug,
            final Tuple<String, String> value, final Exception e) {
        assert value != null || e != null;
        doAnswer((i) -> {
            ActionListener<Tuple<String, String>> listener = (ActionListener<Tuple<String, String>>) i.getArguments()[3];
            if (e != null) {
                listener.onFailure(e);
            } else {
                listener.onResponse(value);
            }
            return null;
        }).when(mockKerberosTicketValidator).validateTicket(aryEq(decodedTicket), eq(keytabPath), eq(krbDebug), any(ActionListener.class));
    }

    public void testFailedAuthorization() throws LoginException, GSSException {
        final KerberosRealm kerberosRealm = createKerberosRealm("test@REALM");
        final byte[] decodedTicket = "base64encodedticket".getBytes(StandardCharsets.UTF_8);
        final Path keytabPath = config.env().configFile().resolve(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.get(config.settings()));
        final boolean krbDebug = KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE.get(config.settings());
        mockKerberosTicketValidator(decodedTicket, keytabPath, krbDebug, new Tuple<>("does-not-exist@REALM", "out-token"), null);

        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        kerberosRealm.authenticate(new KerberosAuthenticationToken(decodedTicket), future);

        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> future.actionGet());
        assertThat(e.status(), is(RestStatus.FORBIDDEN));
        assertThat(e.getMessage(), equalTo("Expected UPN '" + Arrays.asList("test@REALM") + "' but was 'does-not-exist@REALM'"));
    }

    public void testLookupUser() {
        final KerberosRealm kerberosRealm = createKerberosRealm("test@REALM");
        final PlainActionFuture<User> future = new PlainActionFuture<>();
        kerberosRealm.lookupUser("test@REALM", future);
        assertThat(future.actionGet(), is(nullValue()));
    }

    public void testCacheInvalidationScenarios() throws LoginException, GSSException {
        final List<String> userNames = Arrays.asList(randomAlphaOfLength(5) + "@REALM", randomAlphaOfLength(5) + "@REALM");
        final KerberosRealm kerberosRealm = createKerberosRealm(userNames.toArray(new String[0]));
        verify(mockNativeRoleMappingStore).refreshRealmOnChange(kerberosRealm);

        final String authNUsername = randomFrom(userNames);
        final String outToken = randomAlphaOfLength(5);
        final byte[] decodedTicket = "base64encodedticket".getBytes(StandardCharsets.UTF_8);
        final Path keytabPath = config.env().configFile().resolve(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.get(config.settings()));
        final boolean krbDebug = KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE.get(config.settings());
        mockKerberosTicketValidator(decodedTicket, keytabPath, krbDebug, new Tuple<>(authNUsername, outToken), null);
        final User expectedUser = new User(authNUsername, roles.toArray(new String[roles.size()]), null, null, null, true);

        final KerberosAuthenticationToken kerberosAuthenticationToken = new KerberosAuthenticationToken(decodedTicket);
        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        kerberosRealm.authenticate(kerberosAuthenticationToken, future);
        AuthenticationResult result = future.actionGet();
        final User user1 = result.getUser();

        assertSuccessAuthenticationResult(expectedUser, outToken, result);

        final String expireThisUser = randomFrom(userNames);
        boolean expireAll = randomBoolean();
        if (expireAll) {
            kerberosRealm.expireAll();
        } else {
            kerberosRealm.expire(expireThisUser);
        }

        future = new PlainActionFuture<>();
        kerberosRealm.authenticate(kerberosAuthenticationToken, future);
        result = future.actionGet();
        final User user2 = result.getUser();

        assertSuccessAuthenticationResult(expectedUser, outToken, result);

        if (expireAll || expireThisUser.equals(authNUsername)) {
            assertThat(user1, is(not(sameInstance(user2))));
        } else {
            assertThat(user1, sameInstance(user2));
            verify(mockKerberosTicketValidator, times(2)).validateTicket(aryEq(decodedTicket), eq(keytabPath), eq(krbDebug),
                    any(ActionListener.class));
            verify(mockNativeRoleMappingStore).resolveRoles(any(UserData.class), any(ActionListener.class));
        }
    }

    public void testAuthenticateWithValidTicketSucessAuthnWithUserDetailsWhenCacheDisabled()
            throws LoginException, GSSException, IOException {
        // cache.ttl <= 0 is cache disabled
        settings = KerberosTestCase.buildKerberosRealmSettings(KerberosTestCase.writeKeyTab(dir.resolve("key.keytab"), "asa").toString(),
                100, "0m", true);
        final KerberosRealm kerberosRealm = createKerberosRealm("test@REALM");

        final User expectedUser = new User("test@REALM", roles.toArray(new String[roles.size()]), null, null, null, true);
        final byte[] decodedTicket = "base64encodedticket".getBytes(StandardCharsets.UTF_8);
        final Path keytabPath = config.env().configFile().resolve(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.get(config.settings()));
        final boolean krbDebug = KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE.get(config.settings());
        mockKerberosTicketValidator(decodedTicket, keytabPath, krbDebug, new Tuple<>("test@REALM", "out-token"), null);
        final KerberosAuthenticationToken kerberosAuthenticationToken = new KerberosAuthenticationToken(decodedTicket);

        // authenticate
        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        kerberosRealm.authenticate(kerberosAuthenticationToken, future);
        final AuthenticationResult result = future.actionGet();
        final User user1 = result.getUser();
        assertSuccessAuthenticationResult(expectedUser, "out-token", result);

        // authenticate when cache has been disabled
        future = new PlainActionFuture<>();
        kerberosRealm.authenticate(kerberosAuthenticationToken, future);
        final User user2 = future.actionGet().getUser();
        assertSuccessAuthenticationResult(expectedUser, "out-token", result);
        assertThat(user1, not(sameInstance(user2)));

        verify(mockKerberosTicketValidator, times(2)).validateTicket(aryEq(decodedTicket), eq(keytabPath), eq(krbDebug),
                any(ActionListener.class));
        verify(mockNativeRoleMappingStore).refreshRealmOnChange(kerberosRealm);
        verify(mockNativeRoleMappingStore, times(2)).resolveRoles(any(UserData.class), any(ActionListener.class));
        verifyNoMoreInteractions(mockKerberosTicketValidator, mockNativeRoleMappingStore);
    }

    private void assertSuccessAuthenticationResult(final User expectedUser, final String outToken, final AuthenticationResult result) {
        assertThat(result, is(notNullValue()));
        assertThat(result.getStatus(), is(equalTo(AuthenticationResult.Status.SUCCESS)));
        assertThat(result.getUser(), is(equalTo(expectedUser)));
        final Map<String, List<String>> responseHeaders = threadPool.getThreadContext().getResponseHeaders();
        assertThat(responseHeaders, is(notNullValue()));
        assertThat(responseHeaders.get(KerberosAuthenticationToken.WWW_AUTHENTICATE).get(0),
                is(equalTo(KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER_PREFIX + outToken)));
    }

    private KerberosRealm createKerberosRealm(final String... userForRoleMapping) {
        config = new RealmConfig("test-kerb-realm", settings, globalSettings, TestEnvironment.newEnvironment(globalSettings),
                new ThreadContext(globalSettings));
        mockNativeRoleMappingStore = roleMappingStore(Arrays.asList(userForRoleMapping));
        mockKerberosTicketValidator = mock(KerberosTicketValidator.class);
        final KerberosRealm kerberosRealm =
                new KerberosRealm(config, mockNativeRoleMappingStore, mockKerberosTicketValidator, threadPool, null);
        return kerberosRealm;
    }

    @SuppressWarnings("unchecked")
    private NativeRoleMappingStore roleMappingStore(final List<String> expectedUserNames) {
        final Client mockClient = mock(Client.class);
        when(mockClient.threadPool()).thenReturn(threadPool);
        when(mockClient.settings()).thenReturn(settings);

        final NativeRoleMappingStore store = new NativeRoleMappingStore(Settings.EMPTY, mockClient, mock(SecurityIndexManager.class));
        final NativeRoleMappingStore roleMapper = spy(store);

        doAnswer(invocation -> {
            final UserRoleMapper.UserData userData = (UserRoleMapper.UserData) invocation.getArguments()[0];
            final ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            if (expectedUserNames.contains(userData.getUsername())) {
                listener.onResponse(roles);
            } else {
                listener.onFailure(
                        Exceptions.authorizationError("Expected UPN '" + expectedUserNames + "' but was '" + userData.getUsername() + "'"));
            }
            return null;
        }).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), any(ActionListener.class));

        return roleMapper;
    }

}