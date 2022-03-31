/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.Build;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.InternalRealmsSettings;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.hamcrest.Matchers;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.isA;
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
        verify(securityIndex).addStateListener(isA(BiConsumer.class));

        factories.get(NativeRealmSettings.TYPE).create(new RealmConfig(realmId, settings, env, threadContext));
        verify(securityIndex, times(2)).addStateListener(isA(BiConsumer.class));
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

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/85407")
    public void testJwtRealmDependsOnBuildType() {
        // Whether the JWT realm is registered depends on the build type
        if (Build.CURRENT.isSnapshot()) {
            assertThat(InternalRealms.isInternalRealm(JwtRealmSettings.TYPE), is(true));
        } else {
            assertThat(InternalRealms.isInternalRealm(JwtRealmSettings.TYPE), is(false));
        }
    }

    private boolean isStandardRealm(String type) {
        return switch (type) {
            case LdapRealmSettings.LDAP_TYPE, LdapRealmSettings.AD_TYPE, PkiRealmSettings.TYPE -> true;
            default -> false;
        };
    }
}
