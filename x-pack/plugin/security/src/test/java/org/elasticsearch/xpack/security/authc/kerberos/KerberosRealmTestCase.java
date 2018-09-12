/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public abstract class KerberosRealmTestCase extends ESTestCase {

    protected Path dir;
    protected ThreadPool threadPool;
    protected Settings globalSettings;
    protected ResourceWatcherService resourceWatcherService;
    protected Settings settings;
    protected RealmConfig config;

    protected KerberosTicketValidator mockKerberosTicketValidator;
    protected NativeRoleMappingStore mockNativeRoleMappingStore;
    protected XPackLicenseState licenseState;

    protected static final Set<String> roles = Sets.newHashSet("admin", "kibana_user");

    @Before
    public void setup() throws Exception {
        threadPool = new TestThreadPool("kerb realm tests");
        resourceWatcherService = new ResourceWatcherService(Settings.EMPTY, threadPool);
        dir = createTempDir();
        globalSettings = Settings.builder().put("path.home", dir).build();
        settings = KerberosTestCase.buildKerberosRealmSettings(KerberosTestCase.writeKeyTab(dir.resolve("key.keytab"), "asa").toString(),
                100, "10m", true, randomBoolean());
        licenseState = mock(XPackLicenseState.class);
        when(licenseState.isAuthorizationRealmAllowed()).thenReturn(true);
    }

    @After
    public void shutdown() throws InterruptedException {
        resourceWatcherService.stop();
        terminate(threadPool);
    }

    protected void mockKerberosTicketValidator(final byte[] decodedTicket, final Path keytabPath, final boolean krbDebug,
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

    protected void assertSuccessAuthenticationResult(final User expectedUser, final String outToken, final AuthenticationResult result) {
        assertThat(result, is(notNullValue()));
        assertThat(result.getStatus(), is(equalTo(AuthenticationResult.Status.SUCCESS)));
        assertThat(result.getUser(), is(equalTo(expectedUser)));
        final Map<String, List<String>> responseHeaders = threadPool.getThreadContext().getResponseHeaders();
        assertThat(responseHeaders, is(notNullValue()));
        assertThat(responseHeaders.get(KerberosAuthenticationToken.WWW_AUTHENTICATE).get(0),
                is(equalTo(KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER_PREFIX + outToken)));
    }

    protected KerberosRealm createKerberosRealm(final String... userForRoleMapping) {
        return createKerberosRealm(Collections.emptyList(), userForRoleMapping);
    }

    protected KerberosRealm createKerberosRealm(final List<Realm> delegatedRealms, final String... userForRoleMapping) {
        config = new RealmConfig("test-kerb-realm", settings, globalSettings, TestEnvironment.newEnvironment(globalSettings),
                new ThreadContext(globalSettings));
        mockNativeRoleMappingStore = roleMappingStore(Arrays.asList(userForRoleMapping));
        mockKerberosTicketValidator = mock(KerberosTicketValidator.class);
        final KerberosRealm kerberosRealm =
                new KerberosRealm(config, mockNativeRoleMappingStore, mockKerberosTicketValidator, threadPool, null);
        Collections.shuffle(delegatedRealms, random());
        kerberosRealm.initialize(delegatedRealms, licenseState);
        return kerberosRealm;
    }

    @SuppressWarnings("unchecked")
    protected NativeRoleMappingStore roleMappingStore(final List<String> userNames) {
        final List<String> expectedUserNames = userNames.stream().map(this::maybeRemoveRealmName).collect(Collectors.toList());
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

    protected String randomPrincipalName() {
        final StringBuilder principalName = new StringBuilder();
        principalName.append(randomAlphaOfLength(5));
        final boolean withInstance = randomBoolean();
        if (withInstance) {
            principalName.append("/").append(randomAlphaOfLength(5));
        }
        principalName.append(randomAlphaOfLength(5).toUpperCase(Locale.ROOT));
        return principalName.toString();
    }

    /**
     * Usually principal names are in the form 'user/instance@REALM'. This method
     * removes '@REALM' part from the principal name if
     * {@link KerberosRealmSettings#SETTING_REMOVE_REALM_NAME} is {@code true} else
     * will return the input string.
     *
     * @param principalName user principal name
     * @return username after removal of realm
     */
    protected String maybeRemoveRealmName(final String principalName) {
        if (KerberosRealmSettings.SETTING_REMOVE_REALM_NAME.get(settings)) {
            int foundAtIndex = principalName.indexOf('@');
            if (foundAtIndex > 0) {
                return principalName.substring(0, foundAtIndex);
            }
        }
        return principalName;
    }
}
