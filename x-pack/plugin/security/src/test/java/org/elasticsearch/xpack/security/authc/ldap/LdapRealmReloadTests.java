/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPResult;
import com.unboundid.ldap.sdk.Modification;
import com.unboundid.ldap.sdk.ModificationType;
import com.unboundid.ldap.sdk.ResultCode;

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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LdapRealmReloadTests extends LdapTestCase {

    public static final String BIND_DN = "cn=Thomas Masterman Hardy,ou=people,o=sevenSeas";
    public static final String INITIAL_BIND_PASSWORD = "pass";
    public static final UsernamePasswordToken LDAP_USER_AUTH_TOKEN = new UsernamePasswordToken(
        "jsamuel@royalnavy.mod.uk",
        new SecureString("pass".toCharArray())
    );

    private static final Settings defaultRealmSettings = Settings.builder()
        .put(getFullSettingKey(REALM_IDENTIFIER.getName(), LdapUserSearchSessionFactorySettings.SEARCH_BASE_DN), "")
        .put(getFullSettingKey(REALM_IDENTIFIER, PoolingSessionFactorySettings.BIND_DN), BIND_DN)
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

    public void testReloadWithoutConnectionPool() throws Exception {
        final Settings bindPasswordSettings = Settings.builder()
            .setSecureSettings(secureSettings(PoolingSessionFactorySettings.SECURE_BIND_PASSWORD, INITIAL_BIND_PASSWORD))
            .build();
        final Settings settings = Settings.builder()
            .put(getFullSettingKey(REALM_IDENTIFIER.getName(), LdapUserSearchSessionFactorySettings.POOL_ENABLED), false)
            .putList(getFullSettingKey(REALM_IDENTIFIER, URLS_SETTING), ldapUrls())
            .put(defaultRealmSettings)
            .put(defaultGlobalSettings)
            .put(bindPasswordSettings)
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
            final Settings updatedBindPasswordSettings = Settings.builder()
                .setSecureSettings(secureSettings(PoolingSessionFactorySettings.SECURE_BIND_PASSWORD, newBindPassword))
                .build();

            ldap.reload(updatedBindPasswordSettings);
            authenticateUserAndAssertStatus(ldap, AuthenticationResult.Status.CONTINUE);

            // Change password on LDAP server side and check that authentication works
            changeUserPasswordOnLdapServers(BIND_DN, newBindPassword);
            authenticateUserAndAssertStatus(ldap, AuthenticationResult.Status.SUCCESS);
        }
    }

    public void testReloadWithConnectionPool() throws Exception {
        final Settings bindPasswordSettings = Settings.builder()
            .setSecureSettings(secureSettings(PoolingSessionFactorySettings.SECURE_BIND_PASSWORD, INITIAL_BIND_PASSWORD))
            .build();
        final Settings settings = Settings.builder()
            .put(getFullSettingKey(REALM_IDENTIFIER.getName(), LdapUserSearchSessionFactorySettings.POOL_ENABLED), true)
            .putList(getFullSettingKey(REALM_IDENTIFIER, URLS_SETTING), ldapUrls())
            .put(defaultRealmSettings)
            .put(defaultGlobalSettings)
            .put(bindPasswordSettings)
            .build();
        final RealmConfig config = getRealmConfig(REALM_IDENTIFIER, settings);
        try (SessionFactory sessionFactory = LdapRealm.sessionFactory(config, new SSLService(config.env()), threadPool)) {
            assertThat(sessionFactory, is(instanceOf(LdapUserSearchSessionFactory.class)));

            LdapRealm ldap = new LdapRealm(config, sessionFactory, buildGroupAsRoleMapper(resourceWatcherService), threadPool);
            ldap.initialize(Collections.singleton(ldap), licenseState);

            // When a connection is open and already bound, changing the bind password generally
            // does not affect the existing pooled connection. LDAP connections are stateful,
            // and once a connection is established and bound, it remains open until explicitly closed
            // or until a connection timeout occurs. Changing the bind password on the server
            // does not automatically invalidate existing connections. Hence, we are skipping
            // here the check that the authentication works before re-loading bind password,
            // since this check would create and bind a new connection using old password.

            // Generate a new password and reload only on ES side
            final String newBindPassword = randomAlphaOfLengthBetween(5, 10);
            final Settings updatedBindPasswordSettings = Settings.builder()
                .setSecureSettings(secureSettings(PoolingSessionFactorySettings.SECURE_BIND_PASSWORD, newBindPassword))
                .build();

            ldap.reload(updatedBindPasswordSettings);
            // Using new bind password should fail since we did not update it on LDAP server side.
            authenticateUserAndAssertStatus(ldap, AuthenticationResult.Status.CONTINUE);

            // Change password on LDAP server side and check that authentication works now.
            changeUserPasswordOnLdapServers(BIND_DN, newBindPassword);
            authenticateUserAndAssertStatus(ldap, AuthenticationResult.Status.SUCCESS);
        }
    }

    public void testReloadValidation() throws Exception {
        final boolean useLegacyBindSetting = randomBoolean();
        final Settings bindPasswordSettings;
        if (useLegacyBindSetting) {
            bindPasswordSettings = Settings.builder()
                .put(getFullSettingKey(REALM_IDENTIFIER, PoolingSessionFactorySettings.LEGACY_BIND_PASSWORD), INITIAL_BIND_PASSWORD)
                .build();
        } else {
            bindPasswordSettings = Settings.builder()
                .setSecureSettings(secureSettings(PoolingSessionFactorySettings.SECURE_BIND_PASSWORD, INITIAL_BIND_PASSWORD))
                .build();
        }
        final Settings settings = Settings.builder()
            .put(getFullSettingKey(REALM_IDENTIFIER.getName(), LdapUserSearchSessionFactorySettings.POOL_ENABLED), randomBoolean())
            .putList(getFullSettingKey(REALM_IDENTIFIER, URLS_SETTING), ldapUrls())
            .put(defaultRealmSettings)
            .put(defaultGlobalSettings)
            .put(bindPasswordSettings)
            .build();
        final RealmConfig config = getRealmConfig(REALM_IDENTIFIER, settings);
        try (SessionFactory sessionFactory = LdapRealm.sessionFactory(config, new SSLService(config.env()), threadPool)) {
            assertThat(sessionFactory, is(instanceOf(LdapUserSearchSessionFactory.class)));
            LdapRealm ldap = new LdapRealm(config, sessionFactory, buildGroupAsRoleMapper(resourceWatcherService), threadPool);
            ldap.initialize(Collections.singleton(ldap), licenseState);

            var e = expectThrows(Exception.class, () -> ldap.reload(Settings.EMPTY));
            assertThat(
                e.getMessage(),
                equalTo(
                    "["
                        + getFullSettingKey(config, PoolingSessionFactorySettings.BIND_DN)
                        + "] is set but no bind password is specified. Without a corresponding bind password, "
                        + "all "
                        + ldap.type()
                        + " realm authentication will fail. Specify a bind password via ["
                        + getFullSettingKey(config, PoolingSessionFactorySettings.SECURE_BIND_PASSWORD)
                        + "]."
                )
            );

            e = expectThrows(
                Exception.class,
                () -> ldap.reload(
                    Settings.builder()
                        .setSecureSettings(secureSettings(PoolingSessionFactorySettings.SECURE_BIND_PASSWORD, INITIAL_BIND_PASSWORD))
                        .put(getFullSettingKey(REALM_IDENTIFIER, PoolingSessionFactorySettings.LEGACY_BIND_PASSWORD), INITIAL_BIND_PASSWORD)
                        .build()
                )
            );
            assertThat(
                e.getMessage(),
                containsString(
                    "You cannot specify both ["
                        + getFullSettingKey(REALM_IDENTIFIER, PoolingSessionFactorySettings.LEGACY_BIND_PASSWORD)
                        + "] and ["
                        + getFullSettingKey(REALM_IDENTIFIER, PoolingSessionFactorySettings.SECURE_BIND_PASSWORD)
                        + "]"
                )
            );

            if (useLegacyBindSetting) {
                assertSettingDeprecationsAndWarnings(
                    new Setting<?>[] {
                        PoolingSessionFactorySettings.LEGACY_BIND_PASSWORD.apply(REALM_IDENTIFIER.getType())
                            .getConcreteSettingForNamespace(REALM_IDENTIFIER.getName()) }
                );
            }

            // The already configured password should stay unchanged
            // and the authentication should still work.
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
            ldapServer.getPasswordAttributes().forEach(passwordAttribute -> {
                try {
                    LDAPResult result = ldapServer.modify(userDn, new Modification(ModificationType.REPLACE, "userPassword", newPassword));
                    assertThat(result.getResultCode(), equalTo(ResultCode.SUCCESS));
                } catch (LDAPException e) {
                    fail(e, "failed to change " + passwordAttribute + " for user: " + userDn);
                }
            });
        });
    }

    private static SecureSettings secureSettings(Function<String, Setting.AffixSetting<SecureString>> settingFactory, String value) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(getFullSettingKey(REALM_IDENTIFIER, settingFactory), value);
        return secureSettings;
    }
}
