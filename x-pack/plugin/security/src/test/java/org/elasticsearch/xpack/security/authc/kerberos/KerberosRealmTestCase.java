/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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

    protected static final String REALM_NAME = "test-kerb-realm";

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
        settings = buildKerberosRealmSettings(REALM_NAME,
            writeKeyTab(dir.resolve("key.keytab"), "asa").toString(), 100, "10m", true, randomBoolean());
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
        final RealmConfig.RealmIdentifier id = new RealmConfig.RealmIdentifier(KerberosRealmSettings.TYPE, REALM_NAME);
        config = new RealmConfig(id, merge(id, settings, globalSettings),
            TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));
        mockNativeRoleMappingStore = roleMappingStore(Arrays.asList(userForRoleMapping));
        mockKerberosTicketValidator = mock(KerberosTicketValidator.class);
        final KerberosRealm kerberosRealm =
            new KerberosRealm(config, mockNativeRoleMappingStore, mockKerberosTicketValidator, threadPool, null);
        Collections.shuffle(delegatedRealms, random());
        kerberosRealm.initialize(delegatedRealms, licenseState);
        return kerberosRealm;
    }

    private Settings merge(RealmConfig.RealmIdentifier identifier, Settings realmSettings, Settings globalSettings) {
        return Settings.builder().put(realmSettings)
            .normalizePrefix(RealmSettings.realmSettingPrefix(identifier))
            .put(globalSettings)
            .put(RealmSettings.getFullSettingKey(identifier, RealmSettings.ORDER_SETTING), 0)
            .build();
    }

    @SuppressWarnings("unchecked")
    protected NativeRoleMappingStore roleMappingStore(final List<String> userNames) {
        final List<String> expectedUserNames = userNames.stream().map(this::maybeRemoveRealmName).collect(Collectors.toList());
        final Client mockClient = mock(Client.class);
        when(mockClient.threadPool()).thenReturn(threadPool);
        when(mockClient.settings()).thenReturn(settings);

        final NativeRoleMappingStore store = new NativeRoleMappingStore(Settings.EMPTY, mockClient, mock(SecurityIndexManager.class),
            mock(ScriptService.class));
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
        principalName.append("@");
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
        return maybeRemoveRealmName(REALM_NAME, principalName);
    }

    protected String maybeRemoveRealmName(String realmName, final String principalName) {
        if (KerberosRealmSettings.SETTING_REMOVE_REALM_NAME.getConcreteSettingForNamespace(realmName).get(settings)) {
            int foundAtIndex = principalName.indexOf('@');
            if (foundAtIndex > 0) {
                return principalName.substring(0, foundAtIndex);
            }
        }
        return principalName;
    }

    /**
     * Extracts and returns realm part from the principal name.
     * @param principalName user principal name
     * @return realm name if found else returns {@code null}
     */
    protected String realmName(final String principalName) {
        String[] values = principalName.split("@");
        if (values.length > 1) {
            return values[1];
        }
        return null;
    }

    /**
     * Write content to provided keytab file.
     *
     * @param keytabPath {@link Path} to keytab file.
     * @param content Content for keytab
     * @return key tab path
     * @throws IOException if I/O error occurs while writing keytab file
     */
    public static Path writeKeyTab(final Path keytabPath, final String content) throws IOException {
        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(keytabPath, StandardCharsets.US_ASCII)) {
            bufferedWriter.write(Strings.isNullOrEmpty(content) ? "test-content" : content);
        }
        return keytabPath;
    }

    /**
     * Build kerberos realm settings with default config and given keytab
     *
     * @param keytabPath key tab file path
     * @return {@link Settings} for kerberos realm
     */
    public static Settings buildKerberosRealmSettings(final String realmName,final String keytabPath) {
        return buildKerberosRealmSettings(realmName, keytabPath, 100, "10m", true, false);
    }

    public static Settings buildKerberosRealmSettings(String realmName, String keytabPath, int maxUsersInCache, String cacheTTL,
                                                      boolean enableDebugging, boolean removeRealmName) {
        final Settings global = Settings.builder().put("path.home", createTempDir()).build();
        return buildKerberosRealmSettings(realmName, keytabPath, maxUsersInCache, cacheTTL, enableDebugging, removeRealmName, global);
    }

    /**
     * Build kerberos realm settings
     *
     * @param realmName       the name of the realm to configure
     * @param keytabPath      key tab file path
     * @param maxUsersInCache max users to be maintained in cache
     * @param cacheTTL        time to live for cached entries
     * @param enableDebugging for krb5 logs
     * @param removeRealmName {@code true} if we want to remove realm name from the username of form 'user@REALM'
     * @param globalSettings  Any global settings to include
     * @return {@link Settings} for kerberos realm
     */

    public static Settings buildKerberosRealmSettings(String realmName, String keytabPath, int maxUsersInCache, String cacheTTL,
                                                      boolean enableDebugging, boolean removeRealmName, Settings globalSettings) {
        final Settings.Builder builder = Settings.builder()
            .put(RealmSettings.getFullSettingKey(realmName, KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH), keytabPath)
            .put(RealmSettings.getFullSettingKey(realmName, KerberosRealmSettings.CACHE_MAX_USERS_SETTING), maxUsersInCache)
            .put(RealmSettings.getFullSettingKey(realmName, KerberosRealmSettings.CACHE_TTL_SETTING), cacheTTL)
            .put(RealmSettings.getFullSettingKey(realmName, KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE), enableDebugging)
            .put(RealmSettings.getFullSettingKey(realmName, KerberosRealmSettings.SETTING_REMOVE_REALM_NAME), removeRealmName)
            .put(RealmSettings.getFullSettingKey(realmName, RealmSettings.ORDER_SETTING.apply(KerberosRealmSettings.TYPE)), 0)
            .put(globalSettings);
        return builder.build();
    }

}
