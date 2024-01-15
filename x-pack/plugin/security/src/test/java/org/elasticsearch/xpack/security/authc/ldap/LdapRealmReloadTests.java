/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.Modification;
import com.unboundid.ldap.sdk.ModificationType;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslVerificationMode;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapUserSearchSessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.ldap.SearchGroupsResolverSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.core.security.authc.support.CachingUsernamePasswordRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapTestCase;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings.URLS_SETTING;
import static org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings.VERIFICATION_MODE_SETTING_REALM;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LdapRealmReloadTests extends LdapTestCase {

    public static final String BIND_DN = "cn=Thomas Masterman Hardy,ou=people,o=sevenSeas";
    public static final String BIND_PASSWORD = "pass";
    public static final UsernamePasswordToken LDAP_USER_AUTH_TOKEN = new UsernamePasswordToken(
        "jsamuel@royalnavy.mod.uk",
        new SecureString("pass".toCharArray())
    );

    private static final Settings localSettings = Settings.builder()
        .put(getFullSettingKey(REALM_IDENTIFIER.getName(), LdapUserSearchSessionFactorySettings.SEARCH_BASE_DN), "")
        .put(getFullSettingKey(REALM_IDENTIFIER, PoolingSessionFactorySettings.BIND_DN), BIND_DN)
        .setSecureSettings(secureSettings(PoolingSessionFactorySettings.SECURE_BIND_PASSWORD, REALM_IDENTIFIER, BIND_PASSWORD))
        .put(getFullSettingKey(REALM_IDENTIFIER, SearchGroupsResolverSettings.SCOPE), LdapSearchScope.SUB_TREE)
        .put(getFullSettingKey(REALM_IDENTIFIER, VERIFICATION_MODE_SETTING_REALM), SslVerificationMode.CERTIFICATE)
        // explicitly disabling cache to always authenticate against LDAP server
        .put(getFullSettingKey(REALM_IDENTIFIER, CachingUsernamePasswordRealmSettings.CACHE_TTL_SETTING), -1)
        .put(getFullSettingKey(REALM_IDENTIFIER, RealmSettings.ORDER_SETTING), 0)
        .build();

    private ResourceWatcherService resourceWatcherService;
    private Settings defaultGlobalSettings;
    private MockLicenseState licenseState;
    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        licenseState = mock(MockLicenseState.class);
        threadPool = new TestThreadPool("ldap realm reload tests");
        resourceWatcherService = new ResourceWatcherService(Settings.EMPTY, threadPool);
        defaultGlobalSettings = Settings.builder().put("path.home", createTempDir()).build();
        when(licenseState.isAllowed(Security.DELEGATED_AUTHORIZATION_FEATURE)).thenReturn(true);
    }

    @After
    public void shutdown() throws InterruptedException {
        resourceWatcherService.close();
        terminate(threadPool);
    }

    private RealmConfig getRealmConfig(RealmConfig.RealmIdentifier identifier, Settings settings) {
        final Environment env = TestEnvironment.newEnvironment(settings);
        return new RealmConfig(identifier, settings, env, new ThreadContext(settings));
    }

    public void testLdapRealmReloadWithoutConnectionPool() throws Exception {
        final Settings settings = Settings.builder()
            .put(getFullSettingKey(REALM_IDENTIFIER.getName(), LdapUserSearchSessionFactorySettings.POOL_ENABLED), false)
            .putList(getFullSettingKey(REALM_IDENTIFIER, URLS_SETTING), ldapUrls())
            .put(localSettings)
            .put(defaultGlobalSettings)
            .build();
        final RealmConfig config = getRealmConfig(REALM_IDENTIFIER, settings);
        try (SessionFactory sessionFactory = LdapRealm.sessionFactory(config, new SSLService(config.env()), threadPool)) {
            assertThat(sessionFactory, is(instanceOf(LdapUserSearchSessionFactory.class)));

            LdapRealm ldap = new LdapRealm(config, sessionFactory, buildGroupAsRoleMapper(resourceWatcherService), threadPool);
            ldap.initialize(Collections.singleton(ldap), licenseState);

            // Verify authentication is successful before the password change
            authenticateUserAndAssertStatus(ldap, AuthenticationResult.Status.SUCCESS);

            // Generate new password and reload only on ES side
            final String newBindPassword = randomAlphaOfLengthBetween(5, 10);
            final Settings updatedSettings = Settings.builder()
                .setSecureSettings(secureSettings(PoolingSessionFactorySettings.SECURE_BIND_PASSWORD, REALM_IDENTIFIER, newBindPassword))
                .build();
            ldap.reload(updatedSettings);
            authenticateUserAndAssertStatus(ldap, AuthenticationResult.Status.CONTINUE);

            // Change password on LDAP server side and check that authentication works
            changeUserPasswordOnLdapServers(BIND_DN, newBindPassword);
            authenticateUserAndAssertStatus(ldap, AuthenticationResult.Status.SUCCESS);
        }
    }

    private void authenticateUserAndAssertStatus(LdapRealm ldap, AuthenticationResult.Status expectedAuthStatus) {
        final PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        ldap.authenticate(LDAP_USER_AUTH_TOKEN, future);
        final AuthenticationResult<User> result = future.actionGet();
        assertThat(result.getStatus(), is(expectedAuthStatus));
    }

    private void changeUserPasswordOnLdapServers(String userDn, String newPassword) {
        Arrays.stream(ldapServers).forEach(ldapServer -> {
            try {
                ldapServer.modify(userDn, new Modification(ModificationType.REPLACE, "userPassword", newPassword));
            } catch (LDAPException e) {
                fail(e, "failed to change password for user: " + userDn);
            }
        });
    }

    private static SecureSettings secureSettings(
        Function<String, Setting.AffixSetting<SecureString>> settingFactory,
        RealmConfig.RealmIdentifier identifier,
        String value
    ) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(getFullSettingKey(identifier, settingFactory), value);
        return secureSettings;
    }
}
