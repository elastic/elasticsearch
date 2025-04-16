/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.InternalRealmsSettings;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.ActiveDirectorySessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapUserSearchSessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SingleSpSamlRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.saml.SamlRealm;
import org.elasticsearch.xpack.security.authc.saml.SamlRealmTests;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.hamcrest.Matchers;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class InternalRealmsTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testNativeRealmRegistersIndexHealthChangeListener() throws Exception {
        SecurityIndexManager securityIndex = mock(SecurityIndexManager.class);
        Map<String, Realm.Factory> factories = InternalRealms.getFactories(
            mock(ThreadPool.class),
            Settings.EMPTY,
            mock(ResourceWatcherService.class),
            mock(SSLService.class),
            mock(NativeUsersStore.class),
            mock(NativeRoleMappingStore.class),
            securityIndex
        );
        assertThat(factories, hasEntry(is(NativeRealmSettings.TYPE), any(Realm.Factory.class)));
        verifyNoMoreInteractions(securityIndex);

        final RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier(NativeRealmSettings.TYPE, "test");
        Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put(RealmSettings.getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0)
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        final ThreadContext threadContext = new ThreadContext(settings);
        factories.get(NativeRealmSettings.TYPE).create(new RealmConfig(realmId, settings, env, threadContext));
        verify(securityIndex).addStateListener(isA(TriConsumer.class));

        factories.get(NativeRealmSettings.TYPE).create(new RealmConfig(realmId, settings, env, threadContext));
        verify(securityIndex, times(2)).addStateListener(isA(TriConsumer.class));
    }

    public void testRealmsRegisterForRefreshAtRoleMapper() throws Exception {
        UserRoleMapper userRoleMapper = mock(UserRoleMapper.class);
        Map<String, Realm.Factory> factories = InternalRealms.getFactories(
            mock(ThreadPool.class),
            Settings.EMPTY,
            mock(ResourceWatcherService.class),
            mock(SSLService.class),
            mock(NativeUsersStore.class),
            userRoleMapper,
            mock(SecurityIndexManager.class)
        );
        {
            RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier(LdapRealmSettings.AD_TYPE, "test");
            Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0)
                .put(RealmSettings.getFullSettingKey(realmId, ActiveDirectorySessionFactorySettings.AD_DOMAIN_NAME_SETTING), "baseDN")
                .build();
            final Environment env = TestEnvironment.newEnvironment(settings);
            final ThreadContext threadContext = new ThreadContext(settings);
            assertThat(factories, hasEntry(is(LdapRealmSettings.AD_TYPE), any(Realm.Factory.class)));
            var realm = factories.get(LdapRealmSettings.AD_TYPE).create(new RealmConfig(realmId, settings, env, threadContext));
            verify(userRoleMapper, times(1)).clearRealmCacheOnChange(same((CachingRealm) realm));
        }
        {
            RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier(LdapRealmSettings.LDAP_TYPE, "test");
            Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0)
                .put(getFullSettingKey(realmId.getName(), LdapUserSearchSessionFactorySettings.SEARCH_BASE_DN), "userSearchBase")
                .put(getFullSettingKey(realmId.getName(), LdapUserSearchSessionFactorySettings.POOL_ENABLED), false)
                .put(RealmSettings.getFullSettingKey(realmId, SessionFactorySettings.URLS_SETTING), "ldap://127.1.1.1")
                .build();
            final Environment env = TestEnvironment.newEnvironment(settings);
            final ThreadContext threadContext = new ThreadContext(settings);
            assertThat(factories, hasEntry(is(LdapRealmSettings.LDAP_TYPE), any(Realm.Factory.class)));
            var realm = factories.get(LdapRealmSettings.LDAP_TYPE).create(new RealmConfig(realmId, settings, env, threadContext));
            verify(userRoleMapper, times(1)).clearRealmCacheOnChange(same((CachingRealm) realm));
        }
        {
            RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier(PkiRealmSettings.TYPE, "test");
            Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0)
                .build();
            final Environment env = TestEnvironment.newEnvironment(settings);
            final ThreadContext threadContext = new ThreadContext(settings);
            assertThat(factories, hasEntry(is(PkiRealmSettings.TYPE), any(Realm.Factory.class)));
            var realm = factories.get(PkiRealmSettings.TYPE).create(new RealmConfig(realmId, settings, env, threadContext));
            verify(userRoleMapper, times(1)).clearRealmCacheOnChange(same((CachingRealm) realm));
        }
        final Path metadata = PathUtils.get(SamlRealm.class.getResource("idp1.xml").toURI());
        {
            RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier(KerberosRealmSettings.TYPE, "test");
            Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0)
                .put(getFullSettingKey(realmId.getName(), KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH), metadata.toString())
                .build();
            final Environment env = TestEnvironment.newEnvironment(settings);
            final ThreadContext threadContext = new ThreadContext(settings);
            assertThat(factories, hasEntry(is(KerberosRealmSettings.TYPE), any(Realm.Factory.class)));
            var realm = factories.get(KerberosRealmSettings.TYPE).create(new RealmConfig(realmId, settings, env, threadContext));
            verify(userRoleMapper, times(1)).clearRealmCacheOnChange(same((CachingRealm) realm));
        }
        {
            RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier(JwtRealmSettings.TYPE, "test");
            Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0)
                .put(getFullSettingKey(realmId.getName(), JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE), "none")
                .put(getFullSettingKey(realmId.getName(), JwtRealmSettings.ALLOWED_ISSUER), "mock")
                .put(getFullSettingKey(realmId.getName(), JwtRealmSettings.ALLOWED_AUDIENCES), "mock")
                .put(getFullSettingKey(realmId.getName(), JwtRealmSettings.CLAIMS_PRINCIPAL.getClaim()), "principal")
                .put(getFullSettingKey(realmId.getName(), JwtRealmSettings.CLAIMS_GROUPS.getClaim()), "roles")
                .put(getFullSettingKey(realmId.getName(), JwtRealmSettings.CLAIMS_DN.getClaim()), "dn")
                .put(getFullSettingKey(realmId.getName(), JwtRealmSettings.CLAIMS_NAME.getClaim()), "name")
                .put(getFullSettingKey(realmId.getName(), JwtRealmSettings.CLAIMS_MAIL.getClaim()), "mail")
                .put(
                    getFullSettingKey(realmId.getName(), JwtRealmSettings.PKC_JWKSET_PATH),
                    getDataPath("/org/elasticsearch/xpack/security/authc/apikey/rsa-public-jwkset.json")
                )
                .build();
            final Environment env = TestEnvironment.newEnvironment(settings);
            final ThreadContext threadContext = new ThreadContext(settings);
            assertThat(factories, hasEntry(is(JwtRealmSettings.TYPE), any(Realm.Factory.class)));
            var realm = factories.get(JwtRealmSettings.TYPE).create(new RealmConfig(realmId, settings, env, threadContext));
            verify(userRoleMapper, times(1)).clearRealmCacheOnChange(same((CachingRealm) realm));
        }
        {
            RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier(SingleSpSamlRealmSettings.TYPE, "test");
            Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0)
                .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
                .put(
                    SingleSpSamlRealmSettings.getFullSettingKey(realmId.getName(), SamlRealmSettings.IDP_METADATA_PATH),
                    metadata.toString()
                )
                .put(
                    SingleSpSamlRealmSettings.getFullSettingKey(realmId.getName(), SamlRealmSettings.IDP_ENTITY_ID),
                    SamlRealmTests.TEST_IDP_ENTITY_ID
                )
                .put(getFullSettingKey(realmId.getName(), SingleSpSamlRealmSettings.SP_ENTITY_ID), "mock")
                .put(getFullSettingKey(realmId.getName(), SingleSpSamlRealmSettings.SP_ACS), "http://mock")
                .put(
                    getFullSettingKey(
                        realmId.getName(),
                        SamlRealmSettings.PRINCIPAL_ATTRIBUTE.apply(SingleSpSamlRealmSettings.TYPE).getAttribute()
                    ),
                    "uid"
                )
                .build();
            final Environment env = TestEnvironment.newEnvironment(settings);
            final ThreadContext threadContext = new ThreadContext(settings);
            assertThat(factories, hasEntry(is(SingleSpSamlRealmSettings.TYPE), any(Realm.Factory.class)));
            try (
                SamlRealm ignored = (SamlRealm) factories.get(SingleSpSamlRealmSettings.TYPE)
                    .create(new RealmConfig(realmId, settings, env, threadContext))
            ) {
                // SAML realm is not caching
                verifyNoMoreInteractions(userRoleMapper);
            }
        }
    }

    public void testLicenseLevels() {
        for (String type : InternalRealms.getConfigurableRealmsTypes()) {
            final LicensedFeature.Persistent feature = InternalRealms.getLicensedFeature(type);
            if (InternalRealms.isBuiltinRealm(type)) {
                assertThat(feature, nullValue());
            } else if (isStandardRealm(type)) {
                assertThat(feature, notNullValue());
                // In theory a "standard" realm could actually be OperationMode.STANDARD, but we don't have any of those at the moment
                assertThat(feature.getMinimumOperationMode(), is(License.OperationMode.GOLD));
            } else {
                assertThat(feature, notNullValue());
                // In theory a (not-builtin & not-standard) realm could actually be OperationMode.ENTERPRISE, but we don't have any
                assertThat(feature.getMinimumOperationMode(), is(License.OperationMode.PLATINUM));
            }
        }
    }

    public void testEachRealmHasRegisteredOrderSetting() {
        final Set<String> registeredOrderKeys = InternalRealmsSettings.getSettings()
            .stream()
            .map(affix -> affix.getConcreteSettingForNamespace("the_name"))
            .map(concrete -> concrete.getKey())
            .filter(key -> key.endsWith(".order"))
            .collect(Collectors.toSet());

        final Set<String> configurableOrderKeys = InternalRealms.getConfigurableRealmsTypes()
            .stream()
            .map(type -> RealmSettings.realmSettingPrefix(type) + "the_name.order")
            .collect(Collectors.toSet());

        assertThat(registeredOrderKeys, Matchers.equalTo(configurableOrderKeys));
    }

    private boolean isStandardRealm(String type) {
        return switch (type) {
            case LdapRealmSettings.LDAP_TYPE, LdapRealmSettings.AD_TYPE, PkiRealmSettings.TYPE -> true;
            default -> false;
        };
    }
}
