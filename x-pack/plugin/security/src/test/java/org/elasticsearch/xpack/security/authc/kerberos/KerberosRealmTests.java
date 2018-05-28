/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.kerberos.support.KerberosTestCase;
import org.elasticsearch.xpack.security.authc.kerberos.support.KerberosTicketValidator;
import org.elasticsearch.xpack.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSName;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.security.auth.login.LoginException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class KerberosRealmTests extends ESTestCase {

    private ThreadPool threadPool;
    private Settings globalSettings;
    private ResourceWatcherService resourceWatcherService;
    private Settings settings;
    @Rule
    public ExpectedException thrown = ExpectedException.none();

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
        settings = KerberosTestCase.buildKerberosRealmSettings(KerberosTestCase.writeKeyTab(dir, "key.keytab", "asa").toString());
    }

    @After
    public void shutdown() throws InterruptedException {
        resourceWatcherService.stop();
        terminate(threadPool);
    }

    @SuppressWarnings("unchecked")
    private NativeRoleMappingStore mockRoleMappingStore(final String expectedUserName) {
        final NativeRoleMappingStore roleMapper = mock(NativeRoleMappingStore.class);

        Mockito.doAnswer(invocation -> {
            final UserRoleMapper.UserData userData = (UserRoleMapper.UserData) invocation.getArguments()[0];
            final ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            if (userData.getUsername().equals(expectedUserName)) {
                listener.onResponse(roles);
            } else {
                listener.onFailure(Exceptions.authorizationError("Expected UPN '" + expectedUserName + "' but was '" + userData + "'"));
            }
            return null;
        }).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), any(ActionListener.class));

        return roleMapper;
    }

    public void testAuthenticateWithValidTicketAndAuthz() throws LoginException, GSSException {
        final RealmConfig config = new RealmConfig("test-kerb-realm", settings, globalSettings,
                TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));
        final NativeRoleMappingStore roleMapper = mockRoleMappingStore("test@REALM");
        @SuppressWarnings("unchecked")
        final Cache<String, User> mockUsernameUserCache = mock(Cache.class);
        final KerberosTicketValidator mockKerberosTicketValidator = mock(KerberosTicketValidator.class);
        final KerberosRealm kerberosRealm = new KerberosRealm(config, roleMapper, mockUsernameUserCache, mockKerberosTicketValidator);
        when(mockKerberosTicketValidator.validateTicket("*", GSSName.NT_HOSTBASED_SERVICE, "base64encodedticket", config))
                .thenReturn(new Tuple<>("test@REALM", "out-token"));

        final KerberosAuthenticationToken kerberosAuthenticationToken =
                new KerberosAuthenticationToken(KerberosAuthenticationToken.UNAUTHENTICATED_PRINCIPAL_NAME, "base64encodedticket");
        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        kerberosRealm.authenticate(kerberosAuthenticationToken, future);
        final AuthenticationResult result = future.actionGet();
        assertNotNull(result);

        assertEquals(AuthenticationResult.Status.SUCCESS, result.getStatus());
        final Map<String, Object> userMetadata = new HashMap<>();
        userMetadata.put("_www-authenticate", "out-token");
        final User expectedUser = new User("test@REALM", roles.toArray(new String[roles.size()]), null, null, userMetadata, true);
        assertEquals(expectedUser, result.getUser());
        Mockito.verify(mockUsernameUserCache).put(Mockito.eq("test@REALM"), Mockito.eq(expectedUser));
        Mockito.verify(mockKerberosTicketValidator).validateTicket(Mockito.eq("*"), Mockito.eq(GSSName.NT_HOSTBASED_SERVICE),
                Mockito.eq("base64encodedticket"), Mockito.eq(config));
    }

    public void testAuthenticateWithCache() throws LoginException, GSSException {
        final RealmConfig config = new RealmConfig("test-kerb-realm", settings, globalSettings,
                TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));
        final NativeRoleMappingStore roleMapper = mockRoleMappingStore("test@REALM");
        @SuppressWarnings("unchecked")
        final Cache<String, User> mockUsernameUserCache = mock(Cache.class);
        final Map<String, Object> userMetadata = new HashMap<>();
        userMetadata.put("_www-authenticate", "out-token");
        final User expectedUser = new User("test@REALM", roles.toArray(new String[roles.size()]), null, null, userMetadata, true);
        when(mockUsernameUserCache.get("test@REALM")).thenReturn(expectedUser);
        final KerberosTicketValidator mockKerberosTicketValidator = mock(KerberosTicketValidator.class);
        final KerberosRealm kerberosRealm = new KerberosRealm(config, roleMapper, mockUsernameUserCache, mockKerberosTicketValidator);
        when(mockKerberosTicketValidator.validateTicket("*", GSSName.NT_HOSTBASED_SERVICE, "base64encodedticket", config))
                .thenReturn(new Tuple<>("test@REALM", "out-token"));

        final KerberosAuthenticationToken kerberosAuthenticationToken =
                new KerberosAuthenticationToken(KerberosAuthenticationToken.UNAUTHENTICATED_PRINCIPAL_NAME, "base64encodedticket");
        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        kerberosRealm.authenticate(kerberosAuthenticationToken, future);
        final AuthenticationResult result = future.actionGet();
        assertNotNull(result);

        assertEquals(AuthenticationResult.Status.SUCCESS, result.getStatus());
        assertEquals(expectedUser, result.getUser());
        Mockito.verify(mockUsernameUserCache).get(Mockito.eq("test@REALM"));
        Mockito.verify(mockUsernameUserCache, Mockito.times(0)).put(Mockito.eq("test@REALM"), Mockito.eq(expectedUser));
        Mockito.verify(mockKerberosTicketValidator).validateTicket(Mockito.eq("*"), Mockito.eq(GSSName.NT_HOSTBASED_SERVICE),
                Mockito.eq("base64encodedticket"), Mockito.eq(config));
    }

    public void testAuthenticateDifferentFailureScenarios() throws LoginException, GSSException {
        final RealmConfig config = new RealmConfig("test-kerb-realm", settings, globalSettings,
                TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));
        final NativeRoleMappingStore roleMapper = mockRoleMappingStore("test@REALM");
        @SuppressWarnings("unchecked")
        final Cache<String, User> mockUsernameUserCache = mock(Cache.class);
        final KerberosTicketValidator mockKerberosTicketValidator = mock(KerberosTicketValidator.class);
        final KerberosRealm kerberosRealm = new KerberosRealm(config, roleMapper, mockUsernameUserCache, mockKerberosTicketValidator);
        final boolean validTicket = rarely();
        final boolean throwExceptionForInvalidTicket = validTicket ? false : randomBoolean();
        final boolean throwLoginException = randomBoolean();
        if (validTicket) {
            when(mockKerberosTicketValidator.validateTicket("*", GSSName.NT_HOSTBASED_SERVICE, "base64encodedticket", config))
                    .thenReturn(new Tuple<>("test@REALM", "out-token"));
        } else {
            if (throwExceptionForInvalidTicket) {
                if (throwLoginException) {
                    when(mockKerberosTicketValidator.validateTicket("*", GSSName.NT_HOSTBASED_SERVICE, "base64encodedticket", config))
                            .thenThrow(new LoginException());
                } else {
                    when(mockKerberosTicketValidator.validateTicket("*", GSSName.NT_HOSTBASED_SERVICE, "base64encodedticket", config))
                            .thenThrow(new GSSException(GSSException.FAILURE));
                }
            } else {
                when(mockKerberosTicketValidator.validateTicket("*", GSSName.NT_HOSTBASED_SERVICE, "base64encodedticket", config))
                        .thenReturn(null);
            }
        }

        final boolean nullKerberosAuthnToken = rarely();
        final KerberosAuthenticationToken kerberosAuthenticationToken = nullKerberosAuthnToken ? null
                : new KerberosAuthenticationToken(KerberosAuthenticationToken.UNAUTHENTICATED_PRINCIPAL_NAME, "base64encodedticket");
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
                final Map<String, Object> userMetadata = new HashMap<>();
                userMetadata.put("_www-authenticate", "out-token");
                final User expectedUser = new User("test@REALM", roles.toArray(new String[roles.size()]), null, null, userMetadata, true);
                assertEquals(expectedUser, result.getUser());
                Mockito.verify(mockUsernameUserCache).put(Mockito.eq("test@REALM"), Mockito.eq(expectedUser));
            } else {
                assertEquals(AuthenticationResult.Status.TERMINATE, result.getStatus());
                if (throwExceptionForInvalidTicket == false) {
                    assertEquals("Could not validate kerberos ticket", result.getMessage());
                } else {
                    if (throwLoginException) {
                        assertEquals("Internal server error: Exception occurred in service login", result.getMessage());
                    } else {
                        assertEquals("Internal server error: Exception occurred in GSS API", result.getMessage());
                    }
                }
                verifyZeroInteractions(mockUsernameUserCache);
            }
            Mockito.verify(mockKerberosTicketValidator).validateTicket(Mockito.eq("*"), Mockito.eq(GSSName.NT_HOSTBASED_SERVICE),
                    Mockito.eq("base64encodedticket"), Mockito.eq(config));
        }
    }

    public void testFailedAuthorization() throws LoginException, GSSException {
        final RealmConfig config = new RealmConfig("test-kerb-realm", settings, globalSettings,
                TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));
        final NativeRoleMappingStore roleMapper = mockRoleMappingStore("test@REALM");
        @SuppressWarnings("unchecked")
        final Cache<String, User> mockUsernameUserCache = mock(Cache.class);
        final KerberosTicketValidator mockKerberosTicketValidator = mock(KerberosTicketValidator.class);
        final KerberosRealm kerberosRealm = new KerberosRealm(config, roleMapper, mockUsernameUserCache, mockKerberosTicketValidator);
        when(mockKerberosTicketValidator.validateTicket("*", GSSName.NT_HOSTBASED_SERVICE, "base64encodedticket", config))
                .thenReturn(new Tuple<>("does-not-exist@REALM", "out-token"));
        final KerberosAuthenticationToken kerberosAuthenticationToken =
                new KerberosAuthenticationToken(KerberosAuthenticationToken.UNAUTHENTICATED_PRINCIPAL_NAME, "base64encodedticket");

        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        kerberosRealm.authenticate(kerberosAuthenticationToken, future);

        thrown.expect(ElasticsearchSecurityException.class);
        thrown.expectMessage("Expected UPN 'test@REALM' but was '");
        future.actionGet();
    }

    public void testLookupUser() {
        final RealmConfig config = new RealmConfig("test-kerb-realm", settings, globalSettings,
                TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));
        final NativeRoleMappingStore roleMapper = mockRoleMappingStore("test@REALM");
        @SuppressWarnings("unchecked")
        final Cache<String, User> mockUsernameUserCache = mock(Cache.class);
        final KerberosTicketValidator mockKerberosTicketValidator = mock(KerberosTicketValidator.class);
        final KerberosRealm kerberosRealm = new KerberosRealm(config, roleMapper, mockUsernameUserCache, mockKerberosTicketValidator);
        final PlainActionFuture<User> future = new PlainActionFuture<>();
        kerberosRealm.lookupUser("test@REALM", future);
        assertNull(future.actionGet());
    }
}
